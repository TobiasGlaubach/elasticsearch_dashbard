# -*- coding: utf-8 -*-
"""
Created on Sun Feb  7 13:39:47 2021

@author: tglaubach
"""
import pandas as pd
import numpy as np

import sqlite3
import re
import requests
import os
import os.path
import argparse
import json
import sys
import hashlib
import datetime
import pathlib


from elasticsearch import Elasticsearch

from file_loader import loadfile, accepted_filetypes, binary_filetypes
from func.text_miner import standard_preproc_txt, standard_preproc_req

from func.file_caching import filetable



my_path = sys.argv[0]
settings_path = os.path.join(os.path.dirname(my_path), 'settings.json')

settings = {
    'title': "Information Management System",
    'host': "127.0.0.1", 
    'port': 9200, 
    'category': "nextcloud_all",
    'doc_type': "doc_pages",
    'initial_search': '',
    'n_items_max': 10,
    "default_search_field": "raw_txt",
}


print('trying to load settings ' + settings_path)
if os.path.exists(settings_path):
    print('--> success')
    with open(settings_path) as fp:
        settings = {**settings, **json.load(fp)}
else:
    print('--> not found... using default settings')


#%%

def delete_and_upload(db_table, category):

    print('UPLOADING INDEX: ' + category)

    es = Elasticsearch([{'host': settings['host'], 'port': settings['port']}])
    index = 'it_' + category
    if isinstance(db_table, pd.DataFrame):
        df = db_table.copy()
    else:
        df = pd.read_sql('select * from ' + index, sqlite3.connect(db_table))


    es.indices.delete(index=index, ignore=[400, 404])

    pages_to_upload = df.to_dict(orient='records')
    for i, pg in enumerate(pages_to_upload):
        if i % 100 == 0:
            print('{}/{}'.format(i, len(pages_to_upload)), end='\r')

        es.index(id=i, index=category, doc_type=settings['doc_type'], body=pg)
    print('{}/{}'.format(len(pages_to_upload), len(pages_to_upload)), end='\n')

#%%

def main(path, db_table=":memory:", links_register=None, host='localhost', port=9200, reuse_table=False, no_indextable=False):

    #%%
    # assure there is a connection to an elasticsearch server
    res = requests.get(f'http://{host}:{port}')
    print(res.content.decode("utf-8") )

    #%%
    # connect to our cluster

    es = Elasticsearch([{'host': host, 'port': port}])

    #%%
    ft = None
    if os.path.exists(db_table):
        print('LOADING FILETABLE: ' + db_table)
        try:
            ft = filetable(db_table)
        except pd.io.sql.DatabaseError as err:
            print(err)

    if ft is None:
        print('CREATING NEW TABLE')
        ft = filetable()

    #%%

    files_avail = set([str(s) for s in ft.df['file_id'].values])
    fids_avail = set([str(s) for s in ft.get_fids()])
    
    gt_time = lambda f: datetime.datetime.fromtimestamp(pathlib.Path(f).stat().st_mtime).isoformat()

    print(f'FOUND {len(fids_avail)} unique files in ' + db_table)
    print("============================================")

    files = {}
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in [f for f in filenames if f.split('.')[-1] in accepted_filetypes]:
            s = os.path.join(dirpath, filename).replace("\\", "/")

            if '.git' in dirpath or '__pycache__' in dirpath or '.ipynb_checkpoints' in dirpath or filename.endswith('.ipynb'):
                continue

            if not os.path.exists(s):
                print('MISSING FILE --> skipping')
                print(s)
                continue
            
            print('FOUND FILE: ' + s)

            fid = filetable.make_fid({   
                'file_id': s,
                'file_size': os.path.getsize(s),
                't_last_modified': gt_time(s)
            })

            if reuse_table:
                if (s in files_avail):
                    if (fid in fids_avail):
                        print('   already exists in filetable --> and up to date --> loading from table')
                        files[s] = ft.decode_by_fid(fid)[1]
                    else:
                        print('   already exists in filetable --> but out of date --> adding')
                        files[s] = None
                else:
                    print('   not found in filetable --> adding')
                    files[s] = None
            else:
                print('   forced reload of all file --> adding')
                files[s] = None         


    # print('link register:', links_register)
    if links_register:
        
        if links_register.endswith('sqlite'):
            dms_register_df = pd.read_sql('select * from linktable', sqlite3.connect(links_register))

            names_lnk_dc = {os.path.join(settings['path_redmine_dms'], pth, ff).replace('\\', '/'):lnk for pth, ff, lnk in zip(dms_register_df['Category'], dms_register_df['FilePath'], dms_register_df['Link'])}
            # for k,v in names_lnk_dc.items(): print(k,v)

        else:
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

    def extract_pages(filename, pages_dc, lnk, checksum_md5):
        pages_to_upload = []
        if pages_dc is not None:
            try:
                for page_no, page_txt in pages_dc.items():
                    tags = standard_preproc_txt(page_txt)
                    checksum_md5_pg = hashlib.md5(page_txt.encode('utf-8')).hexdigest()

                    # replace something like C:/Users/.../
                    # loc = re.sub(r'[A-Z]:\/Users\/[a-zA-Z0-9]+\/', './', os.path.dirname(filename))
                    loc = os.path.dirname(filename)

                    pg = {  'location': loc,
                            'file_name': os.path.basename(filename),
                            'page_no': page_no,
                            'n_pages': len(pages_dc),
                            'tags': tags,
                            'link': lnk,
                            'checksum_file': checksum_md5,
                            'checksum_page': checksum_md5_pg,
                            'raw_txt': page_txt
                            }
                    
                    pages_to_upload.append(pg)

            except Exception as err:
                print(f"ERROR! {i} had an error while processing error msg: {err}. SKIPPING!")
            
        return pages_to_upload


    pages_to_upload = []
    for i, (filename, pages_dc) in enumerate(files.items()):
        print(f'{i}/{len(files)}: {filename}')

        checksum_file = hashlib.md5(open(filename,'rb').read()).hexdigest() if os.path.exists(filename) else None

        if pages_dc is None:
            pages_dc = load(filename)
            
            if pages_dc is not None:
                dc ={   'file_id': filename,
                    'file_type': filename.split('.')[-1],
                    'checksum_file': checksum_file,
                    't_last_modified': gt_time(filename),
                    'file_size': os.path.getsize(filename),
                }
                ft.add(dc, pages_dc)

        else:
            print('   Already loaded from file table...')

        lnk = names_lnk_dc[filename] if filename in names_lnk_dc else ''

        if pages_dc is not None:
            try:
                pgs_new = extract_pages(filename, pages_dc, lnk, checksum_file)
                pages_to_upload += pgs_new
                print('   Extraction successfull. Added {} new rows (pages)'.format(len(pgs_new)))
            except Exception as err:
                raise
                print('ERROR while extracting ' + str(err) + ' ... --> SKIPPING')
        else:
            print('ERROR while loading... --> SKIPPING')

        if i % 25 == 0 and not no_indextable:
            name = 'it_' + settings['category']
            print('INTERMEDIATE SAVING: ' + name)
            df_for_storage = pd.DataFrame.from_records(pages_to_upload)
            df_for_storage.to_sql(name=name, con=sqlite3.connect(db_table), if_exists='replace')

    
    print('FINAL SAVING')
    ft.save(db_table)
    df_for_storage = pd.DataFrame.from_records(pages_to_upload)

    if not no_indextable:
        df_for_storage.to_sql(name='it_' + settings['category'], con=sqlite3.connect(db_table), if_exists='replace')

    return df_for_storage


if __name__ == "__main__":

    port = settings['port']
    host = settings['host']

    file_table_path = ":memory:"
    link_register_pth = None

    parser = argparse.ArgumentParser()
    
    parser.add_argument('-p', '--path', default='.', help='The folder path to scrape')
    parser.add_argument('--category', default=settings['category'], help='The folder path to scrape')
    parser.add_argument('--port', type=int, default=port, help='The folder path to scrape')
    parser.add_argument('--host', default=host, help='The folder path to scrape')
    parser.add_argument('--table', default=file_table_path, help='The path to save and load folder path to scrape')
    parser.add_argument('--link', default=None, help='The folder path to scrape')
    parser.add_argument('--force_reload', action="store_true", default=False, help='reuse the filetable instead of loading files anew (much faster but potentially outdated data)')
    parser.add_argument('--upload', action="store_true", default=False)
    parser.add_argument('--no_indextable', action="store_true", default=False)

    import sys

    print(sys.argv)
    args = parser.parse_args()

    if not args.path:
        print("ERROR must give path to scrape!")
    else:
        settings['category'] = args.category
        reuse_table = not args.force_reload
        df = main(args.path, db_table=args.table, links_register=args.link, host=args.host, port=args.port, reuse_table=reuse_table, no_indextable=args.no_indextable)


    if args.upload:
        delete_and_upload(df, settings['category'])

# %%
