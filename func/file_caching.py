import pandas as pd
import numpy as np

import struct
import sqlite3
import json


class filetable():
    columns = ['file_id', 'file_type', 'sub_indices', 'split_indices', 'text']
    def __init__(self, db_pth=None):
        self.df = pd.DataFrame({col:[] for col in self.columns})
        if db_pth:
            self.load(db_pth)
            
    def __contains__(self, idx):
        return idx in self.df.file_id
    
    def load(self, db_pth):
        self.df = pd.read_sql('select * from filetable', sqlite3.connect(db_pth))
        self.df.set_index('index', inplace=True)
        
    def save(self, db_pth):
        self.df.to_sql(name='filetable', con=sqlite3.connect(db_pth), if_exists='replace')
        
    def add(self, file_id, pages_dc, file_type=''):
        pgs = {0:pages_dc} if isinstance(pages_dc, str) else pages_dc
        self.df.loc[len(self.df)] = self.encode_file(file_id, file_type, pgs)
        
    def decode_file(self, idx):
        i = self.df.file_id.ne(idx).idxmax() if isinstance(idx, str) else idx
        return self.decode_row(self.df.iloc[i])

    def iter_files(self):
        for i in range(len(self.df)):
            yield self.decode_file(i)
            
    def encode_file(self, file_id, file_type, pages_dc):
        text = ''.join(pages_dc.values())
        sub_indices = json.dumps(list(pages_dc.keys()))
        split_indices = json.dumps([len(txt) for txt in pages_dc.values()])
        return file_id, file_type, sub_indices, split_indices, text

    def decode_row(self, row):
        split_length = json.loads(row.split_indices)
        sub_indices = json.loads(row.sub_indices)
        i, pages_lst = 0, []
        for n in split_length:
            pages_lst.append(row.text[i:i+n])
            i += n
        return row.file_id, dict(zip(sub_indices, pages_lst))


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
    
