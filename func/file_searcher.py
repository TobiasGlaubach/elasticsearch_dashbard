import numpy as np
import os
import re
from difflib import SequenceMatcher


def clean(a):
    return re.sub(r"['_‐\-\[\]!,*)@#%(&$?.^]", ' ', a)
    
def similar(a, b):
#    replace stupid second dash
    a = a.replace("‐", "-")
    b = b.replace("‐", "-")
    
    if a == b:
        return np.inf

    if a in b:
        return 1.0
    b_clean = clean(b)
    a_clean = clean(a)
    if a_clean in b_clean or a_clean == b_clean:
        return 0.99
    
    
    return SequenceMatcher(None, a, b).ratio()


def similar_vec(s, repo):
    return np.array([similar(s, r) for r in repo])


def get_file_sys(folder = r"D:/Nextcloud/Shared/MeerKAT Extension"):
    
    # traverse root directory, and list directories as dirs and files as files

    all_files = []
    all_folders = []
    for root, dirs, files in os.walk(folder):
        
        r = root.replace(os.sep, '/')
        for file in files:    
            if file.endswith('.pdf'):
                all_files.append(file) 
                all_folders.append(r)

    return all_files, all_folders

# all_files

