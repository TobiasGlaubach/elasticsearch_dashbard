import argparse
import base64
import datetime
import io
import os
import sys
import re
import json
import pathlib

import pandas as pd

import dash
import dash_table
from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc

from elasticsearch import Elasticsearch

from func.text_miner import standard_preproc_req, standard_preproc_txt, get_nbest, exclude_words_high_freq
from func.text_miner import get_word_matches_nlp, get_word_matches_cnt

from file_loader import loadfile
from func.file_caching import filetable


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


es = Elasticsearch(settings['host'], port=settings['port'])

initial_search = settings["initial_search"]

def get_all_files_in_category():

    res = es.search(index=settings['category'], body = {
        'size' : 10000,
        'query': {
            'match_all' : {}
        }
    })
    return res['hits']['hits']

res = get_all_files_in_category()


indices = list(es.indices.get('*').keys())

assert len(indices) > 0, "ERROR: no indices found in elasticsearch"

if settings['category'] not in indices:
    print("WARNING: " + settings["category"] + " not contained in available indices using first indice available")
    

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

def get_files_opts(res):
    files = list(set(f['_source']['file_name'] for f in res))
    return [{'label': f, 'value': f} for f in files]


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
                value=['pp_inp', 'out_raw']
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
    if file_folder_str:
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
        body = {"query": {'match':{field: search_str}}}

    if settings['debug']:
        print(body)

    res = es.search(index=settings['category'], body=body)
    return res

def doc2item(doc, keywords=None, embed=[]):

    if settings['debug']:
        print(doc['_source'].keys())

    comps = [html.H6('ITEM ID: {} | Match Score: {}'.format(doc['_id'], doc['_score'])),
            html.Div(f"filename: {doc['_source']['file_name']}"),
            html.Div(f"page: {doc['_source']['page_no']}/{doc['_source']['n_pages']}")]

    if doc['_source']['link']:
        lnk = doc['_source']['link'] + '#page={}'.format(doc['_source']['page_no'])
        comps.append(html.Div(html.A(lnk, href=lnk, target="_blank",  rel="noopener noreferrer")))
        # comps.append(html.Link(title=doc['_source'], href=lnk, target='_blank',  rel="noopener noreferrer"))
    
    fpath = os.path.join(doc['_source']['location'], doc['_source']['file_name']).replace("\\", "/")
    if 'local_path' in settings and settings['local_path']:
        if not fpath.startswith(settings['local_path']) and not os.path.exists(fpath):
            fpath = os.path.join(settings['local_path'], fpath).replace("\\", "/")
        
    lnk_local = pathlib.Path(fpath).as_uri()

    comps += [ html.Div(html.A("Local link", href=lnk_local, target="_blank",  rel="noopener noreferrer")) ]

    if "code" in embed:
        comps += [html.Br(), 
            html.Div(decode_text(doc, keywords),style={
                "width": "80%",
                "height": "300px",
                # "padding": "20px",
                "overflow": "auto"}), 
            html.Hr()]
                    
                    
    elif "iframe" in embed and doc['_source']['link']:
        comps += [html.Br(),
                    html.Div(html.Iframe(src=lnk), style={"border":"2px black solid"}),
                    html.Hr()]
        

    return html.Div(comps)

  

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

    if "use_tags" in settings_local:
        res = es_search(search_term, file_folder_str=fname_str, field="tags")
    else:
        res = es_search(search_term, file_folder_str=fname_str, field=settings["default_search_field"])

    status = html.Div('timeout: {} | {} Docs in {}ms'.format(res['timed_out'], len(res['hits']['hits']), res['took']))            
    embed = []
    if "out_raw" in settings_local: embed.append("code")
    if "out_prev" in settings_local: embed.append("iframe")

    return [doc2item(doc, embed=embed, keywords=search_term.split()) for doc in res['hits']['hits']], status


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

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action="store_true", default=False, help='print debug messages to stderr')

    
    args = parser.parse_args()
    
    # args.debug=True

    settings['debug'] = args.debug
    app.run_server(debug=args.debug)