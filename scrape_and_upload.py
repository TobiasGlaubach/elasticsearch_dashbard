# -*- coding: utf-8 -*-
"""
Created on Sun Feb  7 13:39:47 2021

@author: tglaubach
"""
import pandas as pd

import requests
import os
import os.path


from elasticsearch import Elasticsearch

from file_loader import loadfile, accepted_filetypes, binary_filetypes
from func.text_miner import standard_preproc_txt, standard_preproc_req

from func.file_caching import filetable

import json


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

    ft = filetable(db_table)

    #%%

    files = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in [f for f in filenames if f.split('.')[-1] in accepted_filetypes]:
            s = os.path.join(dirpath, filename).replace("\\", "/")
            print('FOUND FILE: ' + s)
            if s in ft:
                print('   already exists in filetable --> skipping')
            else:
                print('   not found in filetable --> adding')
                files.append(s)
            
            
    pages_to_upload = []

    for i, filename in enumerate(files):
        print(f'{i}/{len(files)}: {filename}')
        tp = filename.split('.')[-1]
        openstr = 'rb' if tp in binary_filetypes else 'r'
        with open(filename, openstr) as fp:
            pages_dc = loadfile(fp, tp, verbose=1)
            
        ft.add(filename, pages_dc, file_type=tp)

        for page_no, page_txt in pages_dc.items():
            tags = standard_preproc_txt(page_txt)
            pg = {  'location': os.path.dirname(filename),
                    'file_name': os.path.basename(filename),
                    'page_no': page_no,
                    'n_pages': len(pages_dc),
                    'tags': tags.split(),
                    'raw_txt': page_txt
                    }
            
            pages_to_upload.append(pg)


    #%%

    backup = json.dumps(pages_to_upload)
    pages_to_upload = json.loads(backup)

    #%%
    if links_register:
        dms_register_df = pd.read_excel(links_register).dropna()
        names_lnk_dc = {os.path.basename(ff):lnk for ff, lnk in zip(dms_register_df['Dateiname'], dms_register_df['Link'])}
    else:
        names_lnk_dc = {}

    #%%

    for i, page in enumerate(pages_to_upload):
        lnk = names_lnk_dc[page['file_name']] if page['file_name'] in names_lnk_dc else ''
        raw_txt = pages_to_upload[i]['raw_txt']
        pages_to_upload[i]['link'] = lnk
        if isinstance(pages_to_upload[i]['tags'], list):
            pages_to_upload[i]['tags'] = ' '.join(pages_to_upload[i]['tags'])
        
    #%%

    df_for_storage = pd.DataFrame.from_records(pages_to_upload)
    df_for_storage.to_sql(name='indextable', con=sqlite3.connect(file_table_path), if_exists='replace')

    #%%

    category = "official_folder"
    doc_type = "pdf_pages"

    #%%

    for i, pg in enumerate(pages_to_upload):
        print('{}/{}'.format(i, len(pages_to_upload)))
        # s = "ctx._source.new_field = '{}'".format(pg['tags'])
        es.index(id=i, index=category, doc_type=doc_type, body=pg)


if __name__ == "__main__":
    
    port = 9200
    host = 'localhost'

    file_table_path = ":memory:"
    link_register_pth = None

    parser = argparse.ArgumentParser()
    
    parser.add_argument('-p', '--path', help='The folder path to scrape')
    parser.add_argument('--port', type=int, default=port, help='The folder path to scrape')
    parser.add_argument('--host', default=host, help='The folder path to scrape')
    parser.add_argument('--table', default=file_table_path, help='The path to save and load folder path to scrape')
    parser.add_argument('--link', default=None, help='The folder path to scrape')

    args = parser.parse_args()

    main(args.path, db_table=args.table, links_register=args.link, host=args.host, port=args.port)