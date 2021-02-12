import pandas as pd
import numpy as np

import struct
import sqlite3
import json

import hashlib


class filetable():
    columns = ['file_id', 'file_type', 'sub_indices', 'split_indices', 'text', 'checksum_file', 'checksum_txt', 't_last_modified', 'file_size']
    
    def __init__(self, db_pth=None):
        self.df = pd.DataFrame({col:[] for col in self.columns})
        self.__fids_cache = None
        if db_pth:
            self.load(db_pth)
            
    def __contains__(self, idx):
        return idx in self.df.file_id
    
    def __getitem__(self, idx):
        i = self.df.file_id.ne(idx).idxmax() if isinstance(idx, str) else idx
        return self.df.iloc[i]

    def get_fids(self):
        if self.__fids_cache is None:
            self.__fids_cache = np.array([filetable.make_fid(row) for index, row in self.df.iterrows()])
        return self.__fids_cache

    def get_by_fid(self, fid):
        fids = self.get_fids()
        idxs = np.where(fids == fid)[0]
        return self.df.iloc[idxs[0]] if len(idxs) > 0 else None 
            
    def get_by_checksum(self, checksum_md5):
        i = self.df.checksum_file.ne(checksum_md5).idxmax()
        return self.df.iloc[i]

    def load(self, db_pth):
        name = 'filetable'
        self.__fids_cache = None
        self.df = pd.read_sql('select * from ' + name, sqlite3.connect(db_pth))
        self.df.set_index('index', inplace=True)
        
    def save(self, db_pth):
        name = 'filetable'
        self.df.to_sql(name=name, con=sqlite3.connect(db_pth), if_exists='replace')
        
    def add(self, dc, pages_dc):
        self.__fids_cache = None
        self.df = self.df.append(self.encode_file(dc, pages_dc), ignore_index=True)

    def decode_file(self, idx, is_checksum=False):
        if is_checksum:
            i = self.df.checksum_file.ne(idx).idxmax() if isinstance(idx, str) else idx
        else:
            i = self.df.file_id.ne(idx).idxmax() if isinstance(idx, str) else idx

        return self.decode_row(self.df.iloc[i])

    def decode_by_fid(self, fid):
        return self.decode_row(self.get_by_fid(fid))

    def iter_files(self):
        for i in range(len(self.df)):
            yield self.decode_file(i)
            
    def encode_file(self, dc, pages_dc):
        dc['sub_indices'] = json.dumps(list(pages_dc.keys()))
        dc['split_indices'] = json.dumps([len(txt) for txt in pages_dc.values()])
        dc['text'] = '\n\n'.join(pages_dc.values())
        dc['checksum_txt'] = hashlib.md5(dc['text'] .encode('utf-8')).hexdigest()
        return dc

    def decode_row(self, row):
        split_length = json.loads(row.split_indices)
        sub_indices = json.loads(row.sub_indices)
        i, pages_lst = 0, []
        for n in split_length:
            pages_lst.append(row.text[i:i+n])
            i += n
        return row.file_id, dict(zip(sub_indices, pages_lst))

    @staticmethod
    def make_fid(row):
        return '{}_{}_{}'.format(row['file_size'], row['t_last_modified'], row['file_id'])

#%%

def testme():
    import os
    
    pth = "func/"
    
    files = []
    for path, subdirs, fls in os.walk(pth):
        for name in fls:
            files.append(os.path.join(path, name))
    

    n = 500
    
    files = [f for f in files if f.endswith('py')]
    
    ft = filetable()
    for f in files:
        with open(f, "r") as fp:
            txt = fp.read()
        pages_dc = {cnt:txt[i:i+n] for cnt, i in enumerate(range(0, len(txt), n))}
        
        ft.add(f, pages_dc, 'txt')
        

    a = ft.decode_file(0)
    ft.save('test_db.sqlite')
    ft2 = filetable('test_db.sqlite')
    
