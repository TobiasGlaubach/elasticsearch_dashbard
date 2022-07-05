from getpass import getpass
import requests

import pandas as pd
import sqlite3
import json


settings = {
    'user': 'tglaubach',
    'redmine_project_lnk': "https://project.mpifr-bonn.mpg.de/projects/meerkat-ausschreibung/dmsf.json",
    'redmine_lnk': "https://project.mpifr-bonn.mpg.de",
    'redmine_project_name': "MKp_DS_Evaluation_and_Design_Adoption"
}

#%%

if 'user' not in settings or not settings['user']:
    print('input user name: ')
    user = input()
else:
    user = settings['user']

print('input password: ')
password = getpass()


#%%

r = requests.get(settings['redmine_project_lnk'], auth=(user, password))
r.headers['content-type']

dc = r.json()['dmsf']


print(dc)

#%%

def get_folder_by_id(id_to_get=None):
    lnk = settings['redmine_project_lnk']
    if id_to_get is not None: lnk += '?folder_id=' + str(id_to_get)
    # print(lnk)
    r = requests.get(lnk, auth=(user, password))
    r_dc = r.json()
    return r_dc['dmsf']
    
    


def get_folder(folder_id=None, my_path=''): 
    
    dc_id = {}
    dc = get_folder_by_id(folder_id)
    # print(dc)
    
    for dc_sub in dc['dmsf_files']:
        sub_path = my_path + dc_sub['name'] if my_path else dc_sub['name']
        dc_id[dc_sub['id']] = sub_path
        print(dc_sub['id'], sub_path)

    # print(dc['dmsf_folders'])
    for dc_sub in dc['dmsf_folders']:
        ttl = dc_sub['title'] + '/'
        # get path of the new subfolder
        sub_path = my_path + ttl if my_path else ttl
        # add sub folder to dc
        dc_id[dc_sub['id']] = sub_path
        print(dc_sub['id'], sub_path)
        
        dc_id_sub = get_folder(dc_sub['id'], sub_path)
                                
        dc_id = {**dc_id, **dc_id_sub}
        
        for k, v in dc_id_sub.items(): 
            print(k, v)
        
        
    return dc_id
    
    
    # do the same thing for all sub folders
    

#%%
dc_id = get_folder()

#%%


dc_folders = {k:v for k,v in dc_id.items() if v.endswith('/')}
dc_files = {k:v for k,v in dc_id.items() if not v.endswith('/')}


#%%


mklink = lambda file_id: settings['redmine_lnk'] + f"/dmsf/files/{file_id}/view"
records = [(settings['redmine_project_name'], v, mklink(k)) for k, v in dc_files.items()]
df = pd.DataFrame(records, columns=['Category', 'FilePath', 'Link'])
df.to_sql('linktable', con=sqlite3.connect('local_file_cache.sqlite'), if_exists='replace')



