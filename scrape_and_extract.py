# -*- coding: utf-8 -*-
"""
Created on Sun Feb  7 13:39:47 2021

@author: tglaubach
"""
import pandas as pd
import numpy as np

import sqlite3

import requests
import os
import os.path
import argparse

from elasticsearch import Elasticsearch

from file_loader import loadfile, accepted_filetypes, binary_filetypes
from func.text_miner import standard_preproc_txt, standard_preproc_req

from func.file_caching import filetable


import json
import sys

my_path = sys.argv[0]



settings_path = os.path.join(my_path, 'settings.json')
settings = dict(host="127.0.0.1", port=9200, category = "official_folder", doc_type = "pdf_pages", initial_search='')

category = settings['category']
doc_type = settings["doc_type"]

if os.path.exists(settings_path):
    with open(settings_path) as fp:
        settings = {**settings, **json.load(fp)}
        


#%%

def main(path, db_table=":memory:", links_register=None, host='localhost', port=9200):

    #%%
    # assure there is a connection to an elasticsearch server
    res = requests.get(f'http://{host}:{port}')
    print(res.content.decode("utf-8") )

    #%%
    # connect to our cluster

    es = Elasticsearch([{'host': host, 'port': port}])

    #%%

    if os.path.exists(db_table):
        print('LOADING FILETABLE: ' + db_table)
        ft = filetable(db_table)
    else:
        print('CREATING NEW TABLE')
        ft = filetable()

    #%%

    files_avail = set([str(s) for s in ft.df['file_id'].values])

    files = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in [f for f in filenames if f.split('.')[-1] in accepted_filetypes]:
            s = os.path.join(dirpath, filename).replace("\\", "/")
            print('FOUND FILE: ' + s)
            if s in files_avail:
                print('   already exists in filetable --> skipping')
            else:
                print('   not found in filetable --> adding')
                files.append(s)
            
            
    if links_register:
        dms_register_df = pd.read_excel(links_register).dropna()
        names_lnk_dc = {os.path.basename(ff):lnk for ff, lnk in zip(dms_register_df['Dateiname'], dms_register_df['Link'])}
    else:
        names_lnk_dc = {}



    def load(filename):
        tp = filename.split('.')[-1]
        openstr = 'rb' if tp in binary_filetypes else 'r'
        try:
            with open(filename, openstr) as fp:
                pages_dc = loadfile(fp, tp, verbose=1)
                return pages_dc
        except Exception as err:
            print(f'ERROR while loading {err}... --> SKIPPING')
            return None

    def extract_pages(filename, pages_dc, lnk):
        pages_to_upload = []
        if pages_dc is not None:
            try:
                
                tp = filename.split('.')[-1]
                for page_no, page_txt in pages_dc.items():
                    tags = standard_preproc_txt(page_txt)
                    
                    pg = {  'location': os.path.dirname(filename),
                            'file_name': os.path.basename(filename),
                            'page_no': page_no,
                            'n_pages': len(pages_dc),
                            'tags': tags,
                            'link': lnk,
                            'raw_txt': page_txt
                            }
                    
                    pages_to_upload.append(pg)

            except Exception as err:
                print(f"ERROR! {i} had an error while processing error msg: {err}. SKIPPING!")
            
        return pages_to_upload


    pages_to_upload = []
    for i, filename in enumerate(files):
        print(f'{i}/{len(files)}: {filename}')

        pages_dc = load(filename)
        lnk = names_lnk_dc[filename] if filename in names_lnk_dc else ''

        if pages_dc is not None:
            try:
                ft.add(filename, pages_dc, file_type=filename.split('.')[-1])
                pgs_new = extract_pages(filename, pages_dc, lnk)
                pages_to_upload += pgs_new
            except Exception as err:
                print('ERROR while extracting ' + str(err) + ' ... --> SKIPPING')
        else:
            print('ERROR while loading... --> SKIPPING')

        if i % 10 == 0:
            print('INTERMEDIATE SAVING')
            ft.save(db_table)
            df_for_storage = pd.DataFrame.from_records(pages_to_upload)
            df_for_storage.to_sql(name='indextable', con=sqlite3.connect(db_table), if_exists='replace')

    print('FINAL SAVING')
    ft.save(db_table)
    df_for_storage = pd.DataFrame.from_records(pages_to_upload)
    df_for_storage.to_sql(name='indextable', con=sqlite3.connect(db_table), if_exists='replace')

        
    #%%

    backup = json.dumps(pages_to_upload)
    pages_to_upload = json.loads(backup)

    #%%

    res = es.search(index=category, body = {
        'size' : 10000,
        'query': {
            'match_all' : {}
        }
    })

    mkpth = lambda pg: 'PAGE_{}_{}'.format(pg['page_no'], os.path.join(pg['location'], pg['file_name']).replace('\\', '/'))

    id_dc = {mkpth(r['_source']):int(r['_id']) for r in  res['hits']['hits']}
    
    #%%

    for i, pg in enumerate(pages_to_upload):
        print('{}/{}'.format(i, len(pages_to_upload)))
        fname = mkpth(pg)

        if fname in id_dc:
            idnow = id_dc[fname]
        else:
            idnow = np.max(id_dc.values()) + 1
            id_dc[idnow] = fname
        # s = "ctx._source.new_field = '{}'".format(pg['tags'])
        es.index(id=idnow, index=category, doc_type=doc_type, body=pg)


if __name__ == "__main__":

    port = settings['port']
    host = settings['host']

    file_table_path = ":memory:"
    link_register_pth = None

    parser = argparse.ArgumentParser()
    
    parser.add_argument('-p', '--path', help='The folder path to scrape')
    parser.add_argument('--port', type=int, default=port, help='The folder path to scrape')
    parser.add_argument('--host', default=host, help='The folder path to scrape')
    parser.add_argument('--table', default=file_table_path, help='The path to save and load folder path to scrape')
    parser.add_argument('--link', default=None, help='The folder path to scrape')

    args = parser.parse_args()

    if not args.path:
        print("ERROR must give path to scrape!")
    else:
        main(args.path, db_table=args.table, links_register=args.link, host=args.host, port=args.port)