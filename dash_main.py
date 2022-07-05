import argparse
# import base64
# import datetime
# import io
import os
import sys
import re
import json
import pathlib
# import urllib.parse
# import subprocess

import pandas as pd
import numpy as np

import dash
from dash.dependencies import Input, Output, State

from dash import dash_table, dcc, html

# import dash_bootstrap_components as dbc

from elasticsearch import Elasticsearch
# from sklearn.utils import resample

from func.text_miner import standard_preproc_req, standard_preproc_txt, get_nbest, exclude_words_high_freq
# from func.text_miner import get_word_matches_nlp, get_word_matches_cnt
from func.file_searcher import get_file_sys, similar_vec
# from file_loader import loadfile
# from func.file_caching import filetable


app = dash.Dash(__name__)




my_path = sys.argv[0]
settings_path = os.path.join(os.path.dirname(my_path), 'settings.json')

settings = {
    'title': "Information Management System",
    'host': "127.0.0.1", 
    'port': 9200, 
    'category': "official_folder",
    'doc_type': "pdf_pages",
    'initial_search': '',
    'n_items_max': 10,
    "default_search_field": "raw_txt"
}


print('trying to load settings ' + settings_path)
if os.path.exists(settings_path):
    print('--> success')
    with open(settings_path) as fp:
        settings = {**settings, **json.load(fp)}
else:
    print('--> not found... using default settings')


es = Elasticsearch('http://' + settings['host'] + ':' + str(settings['port']))

initial_search = settings["initial_search"]

def get_all_files_in_category():

    res = es.search(index=settings['category'], size=10000, query=dict(match_all={}))
    return res['hits']['hits']

res = get_all_files_in_category()


dc_index = es.indices.get(index='*')

indices = list(dc_index.keys())
fields = {k:list(v['mappings']['properties'].keys()) for k, v in dc_index.items()}


assert len(indices) > 0, "ERROR: no indices found in elasticsearch"

if settings['category'] not in indices:
    print("WARNING: " + settings["category"] + " not contained in available indices using first indice available")
    




external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

def get_files_opts(res):
    files = list(set(f['_source']['file_name'] for f in res))
    return [{'label': f, 'value': f} for f in files]


def get_search_field_opts(indice):
    return [{'label': f, 'value': f} for f in fields[indice]]


def get_opts(res):
    opts = []
    for doc in res:
        dc = doc['_source']
        opts.append({'label': f"PAGE {dc['page_no']}/{dc['n_pages']} DOC: " + dc['file_name'], 'value': doc['_id']})
    return opts

app.layout = html.Div([
    html.Center(html.H1(settings['title'])),
    dcc.Tabs([        
        dcc.Tab(label='Text Search Engine', children=
        [
            html.Center(
            [
                html.H6('Search Text'),
                dcc.Textarea(
                    id='input-1-submit',
                    value='',
                    style={'width': 450, 'height': 100},
                ),
                dcc.Dropdown(id='dd-words', options=[],
                            value=[],multi=True,
                    style={'width': 450}),
                html.H6("Filename"),
                dcc.Dropdown(
                    id='input-2-dropdown',
                    options = get_files_opts(res),
                    value=None,
                    style={'width': 450, 'justify-content': 'left'}
                )
            ]),
            html.Div([
                html.Br(),
                html.Div(id='output-status'),
                html.Hr(),
                html.Div(id='output-keypress')
            ])
        ]),
        dcc.Tab(label='File Search Engine', children=
        [
            html.Center(
            [
                html.H6('Search Filename(s)'),
                dcc.Textarea(
                    id='input-submit-files',
                    value='',
                    style={'width': 450, 'height': 100},
                ),

                html.H6("Filename"),
                dcc.Dropdown(
                    id='input-dropdown-files',
                    options = [
                        {'label': 'Local File Sys D:', 'value': 'local_d'},
                        {'label': 'Local File Sys C:', 'value': 'local_c'},
                        {'label': 'Elasticsearch', 'value': 'elastic'},
                    ],
                    value='local_d',
                    style={'width': 450, 'justify-content': 'left'}
                )
            ]),
            html.Div([
                html.Br(),
                html.Div(id='output-status-files'),
                html.Hr(),
                html.Div(id='output-keypress-files')
            ])
        ]),

        dcc.Tab(label='Settings', children=[
            html.H6("Settings"),
            dcc.Checklist(
                id='settings-raw-search-cl',
                options=[
                    {'label': 'Pre Process Input Text', 'value': 'pp_inp'},
                    {'label': 'Pre Process Search Text', 'value': 'use_tags'},
                    {'label': 'Show Raw Text in Output', 'value': 'out_raw'},
                    {'label': 'Show Page Preview in Output', 'value': 'out_prev'}
                ],
                value=['out_raw']
            ),
            html.Div('n items to show max'),
            dcc.Input(
                id="set-n_items_max", type="number",
                debounce=True, value=settings['n_items_max']),
            html.Div(id='n_items_max-output-container', children='n items to show max: {}'.format(settings['n_items_max'])),
            dcc.Dropdown(
                id='set-category',
                options=[{'label': l, 'value': l} for l in indices],
                value=settings['category']
            ),
            html.Div('Searchfield'),
            dcc.Dropdown(
                id='set-searchfield',
                options=get_search_field_opts(settings['category']),
                value=settings['default_search_field']
            ),
            html.Div(id='set-searchfield-container', children='searching in field: {}'.format(settings['default_search_field']))
        ]),
        dcc.Tab(label='Document Analyser', children=[
            html.H6('CATEGORY'),
            html.Div(id='dd-category-container'),
            html.H4('FILE TO ANALYSE'),
            dcc.Dropdown(
                    id='files-disp-dropdown',
                    options=get_opts(res),
                    value=None
                ),
            html.H4('FILE CONTENT'),
            html.Div(id='dd-output-container')
        ])  
    ])
])

def intersperse(lst, item):
    result = [item] * (len(lst) * 2 - 1)
    result[0::2] = lst
    return result

def decode_text(res, keywords = None):
    txt = res['_source']['raw_txt']
    if keywords:
        parts = [txt]
        for kw in keywords:
            i = 0
            while i < len(parts):
                t = parts[i]                
                if isinstance(t, str):
                    els = re.split(kw, t, flags=re.IGNORECASE)

                    new_list = intersperse(els, html.Mark(kw))
                    parts = parts[:i] + new_list + parts[i+1:]

                i += 1
        el = [html.Code(parts)]
    else:
        el = html.Code(txt)
    return html.Pre(el, style={"border":"3px", "border-style":"solid", "padding": "1em"})

def es_search(search_str, file_folder_str=None, field="raw_txt"):
    if file_folder_str is not None:
        body={
            'size' : settings['n_items_max'],
            "query": {
                "bool": {
                "must": {
                    "match": {
                    "raw_txt": search_str
                    }
                },
                "filter": {
                    "match": {
                    "file_name": file_folder_str
                    }
                }
                }
            }
        }

        if settings['debug']: print(body)

    else:
        body = {
                'size' : settings['n_items_max'],
                "query": {'match':{field: search_str}}
            }

    if settings['debug']:
        print(body)

    res = es.search(index=settings['category'], body=body)
    return res

def file_search(line, env_key):
    if env_key == 'local_d':
        pth = "D:/"  
    elif env_key == 'local_c':
        pth = "C:/"
    else:
        return file_search_es(line)

    all_files, all_folders = get_file_sys(pth)

    similarity = similar_vec(line, all_files)
    idxs = np.argsort(similarity)[::-1]
    
    rets = []
    matches = []
    cnt = 0
    for idx in idxs:
        if all_files[idx].lower() in matches:
            continue
            
        matches.append(all_files[idx].lower())
        cnt += 1
        if cnt >= 4:
            break
        
        file_name = all_files[idx]
        dir_name = all_folders[idx]
        
        r = dict(score=np.round(min(similarity[idx], 1.0),2), file_name=file_name, id=idx, dir=dir_name)
        rets.append(r)

    df = pd.DataFrame(rets)    

    # table = file2item(df)
    table = file2table(df)
    table = [table]

    return table

def file2item(df):
    fun = lambda v, l: html.A(v, href=l, target="_blank",  rel="noopener noreferrer")
    # fun = lambda v, l: html.Div('NAME: {} | LINK: {}'.format(v, l))

    res = []
    for i in df.index:
        print(i)
        f, d = df.loc[i, 'file_name'], df.loc[i, 'dir']
        p = df.loc[i, 'dir'] + '/' + df.loc[i, 'file_name']
        
        res += [html.Div('ITEM ID: {} | Match Score: {}'.format(df.loc[i, 'id'], df.loc[i, 'score']))]
        res += [fun(f, pathlib.Path(p).as_uri())]
        res += [fun(d, pathlib.Path(d))]

    return res
    
def file2table(df):
    table = dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records'),
        
        style_data_conditional=[
            {
            'if': {
                    'column_id': 'score',
                    'filter_query': '{score} eq "1.00"'
                },
            'backgroundColor': 'green'
            },
            {
            'if':{
                    'column_id': 'score',
                    'filter_query': '{score} eq "0.99"'
                },
            'backgroundColor': 'green'
            }],
    )
    return table

def file_search_es(line):
    # body = {
    #             'size' : 100,
    #             "query": {'match':{'file_name': line}}
    #         }

    # res = es.search(index=settings['category'], body=body)
    res = es.search(index=settings['category'], size=100, query={'match':{'file_name': line}})
    data = []
    keys = []
    cnt = 0
    for doc in res['hits']['hits']:
        d = dict(score=np.round(doc['_score'],2), file_name=doc['_source']['file_name'], id=doc['_id'], dir=doc['_source']['location'])
        k = d['dir'] + '/' + d['file_name'] + str(d['score'])
        if k in keys:
            continue
        keys.append(k)
        data.append(d)
        if len(data) >= settings['n_items_max']:
            break
        
    df = pd.DataFrame(data)

    table = file2table(df)

    return [table]


def doc2item(doc, keywords=None, embed=[]):

    if settings['debug']:
        print(doc['_source'].keys())

    comps = [html.H6(f"filename: {doc['_source']['file_name']}"),
            html.Div('ITEM ID: {} | Match Score: {}'.format(doc['_id'], doc['_score'])),
            html.Div(f"page: {doc['_source']['page_no']}/{doc['_source']['n_pages']}")]

    if doc['_source']['link']:
        lnk = doc['_source']['link'] + '#page={}'.format(doc['_source']['page_no'])
        comps.append(html.Div(html.A(lnk, href=lnk, target="_blank",  rel="noopener noreferrer")))
        # comps.append(html.Link(title=doc['_source'], href=lnk, target='_blank',  rel="noopener noreferrer"))
    
    fpath = os.path.join(doc['_source']['location'], doc['_source']['file_name']).replace("\\", "/")
    if 'local_path' in settings and settings['local_path']:
        if not fpath.startswith(settings['local_path']) and not os.path.exists(fpath):
            fpath = os.path.join(settings['local_path'], fpath).replace("\\", "/")
        
    lnk_local = pathlib.Path(fpath).as_uri() + '#page={}'.format(doc['_source']['page_no']) 
    lnk_name = fpath.replace(settings['local_path'], '')
    lnk_name = lnk_name.replace(doc['_source']['file_name'], '')

    comps += [ html.Div(html.A(lnk_name, href=lnk_local, target="_blank",  rel="noopener noreferrer")) ]

    if "code" in embed:

        if settings['debug']: print('doc2item.tempstore contains n={} items'.format(len(doc2item.tempstore)))

        if doc['_source']['raw_txt'] not in doc2item.tempstore:
            doc2item.tempstore += [doc['_source']['raw_txt']]
            comps += [html.Br(), 
                html.Div(decode_text(doc, keywords), style={
                    "width": "80%",
                    "height": "300px",
                    # "padding": "20px",
                    "overflow": "auto"}), 
                html.Hr()]
        else:
            comps += [html.Div(f"content already shown in search results")]

                    
    elif "iframe" in embed and doc['_source']['link']:
        comps += [html.Br(),
                    html.Div(html.Iframe(src=lnk), style={"border":"2px black solid"}),
                    html.Hr()]
        

    return html.Div(comps)

doc2item.tempstore = []
  

@app.callback(
    Output('dd-output-container', 'children'),
    Input('files-disp-dropdown', 'value')
)
def update_output_files(value):
    if value:
        res = es.get(index=settings['category'], id=value)        
        
        return html.Div([html.H6('DOCUMENT ID:{}'.format(value)),
                html.Div(f"filename: {res['_source']['file_name']}"),
                html.Div(f"location: {res['_source']['location']}"),
                dcc.Link(title=res['_source']['link'], href=res['_source']['link']),
                html.Br(),
                html.Pre(decode_text(res)),
                html.Br(),
                html.Div([html.Code(res['_source']['tags'])], style={'font-family': 'Courier New'}),
                html.Hr()])

@app.callback(
    Output("output-keypress", 'children'),
    Output("output-status", 'children'),
    Input("input-2-dropdown", 'value'),
    Input('dd-words', 'value'),
    State('settings-raw-search-cl', 'value')
)
def update_output_qry(fname_str, words, settings_local):

    search_term = ' '.join(words)
    if not search_term:
        return [], html.Div('')

    if "use_tags" in settings_local and settings['default_search_field'] == 'raw_txt':
        res = es_search(search_term, file_folder_str=fname_str, field="tags")
    else:
        res = es_search(search_term, file_folder_str=fname_str, field=settings["default_search_field"])

    status = html.Div('timeout: {} | {} Docs in {}ms'.format(res['timed_out'], len(res['hits']['hits']), res['took']))            
    embed = []
    if "out_raw" in settings_local: embed.append("code")
    if "out_prev" in settings_local: embed.append("iframe")

    doc2item.tempstore = []
    return [doc2item(doc, embed=embed, keywords=search_term.split()) for doc in res['hits']['hits']], status

@app.callback(
    Output("output-keypress-files", 'children'),
    Output("output-status-files", 'children'),
    Input('input-submit-files', 'value'),
    Input("input-dropdown-files", 'value'),
    State('settings-raw-search-cl', 'value')
)
def update_output_qry_files(fname_str, env_key, settings_local):

    if settings['debug']:
        print(fname_str) 
        print(env_key)
        print(settings_local)

    if not fname_str:
        return [], html.Div('')

    all_res = []
    lines = fname_str.split('\n')
    for line in lines:

        res = es_search(line, field='file_name')

        status = html.Div('timeout: {} | {} Docs in {}ms'.format(res['timed_out'], len(res['hits']['hits']), res['took']))            

        comps = file_search(line, env_key)

        # filename2item.tempstore = []
        all_res += [html.H6("LINE: " + line)] + comps
        # all_res += [filename2item(line, doc, embed=embed, keywords=[line]) for doc in res['hits']['hits']]


    return all_res, status


@app.callback(
    Output("dd-words", 'options'),
    Output("dd-words", 'value'),
    Input('input-1-submit', 'value'),
    State('settings-raw-search-cl', 'value')
)
def update_output_wrds(input1, settings_loc):

    if settings['debug']:
        print(input1) 
        print(settings_loc)

    if "pp_inp" in settings_loc:
        search_term = ' '.join(standard_preproc_req(input1))
        opts_loc = [{'label': w, "value": w} for w in search_term.split()]
        vals_loc = search_term.split()                    
    else:
        search_term = input1
        # opts_loc = [{'label': ["RAW TEXT SEARCH"], "value": [search_term]}]
        opts_loc = []
        vals_loc = [search_term]

    return opts_loc, vals_loc


@app.callback(
    Output("n_items_max-output-container", "children"),
    Input("set-n_items_max", 'value'))
def set_n_items_max(value):
    settings['n_items_max'] = value
    return 'n items to show max: {}'.format(settings['n_items_max'])
    
@app.callback(
    Output("files-disp-dropdown", 'options'),    
    Output("dd-category-container", 'children'),
    Input('set-category', "value")
)
def set_category(value):
    settings['category'] = value
    return get_opts(get_all_files_in_category()), settings['category']

@app.callback(
    Output("set-searchfield-container", 'children'),    
    Input('set-searchfield', "value")
)
def set_searchfield(value):
    settings['default_search_field'] = value
    return 'searching field' + value


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action="store_true", default=False, help='print debug messages to stderr')


    args = parser.parse_args()
    
    # args.debug=True
    

    settings['debug'] = args.debug
    app.run_server(debug=args.debug)