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

from dash import dash_table, dcc, html, no_update

# import dash_bootstrap_components as dbc

from elasticsearch import Elasticsearch
# from sklearn.utils import resample

import w_search

app = dash.Dash(__name__)







external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)



def get_opts(res):
    opts = []
    for doc in res:
        dc = doc['_source']
        opts.append({'label': f"PAGE {dc['page']}/{dc['n_pages']} DOC: " + dc['source'], 'value': doc['_id']})
    return opts

app.layout = html.Div([
    html.Center(html.H1("Search Engine")),
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
            # dcc.Checklist(
            #     id='settings-raw-search-cl',
            #     options=[
            #         {'label': 'Pre Process Input Text', 'value': 'pp_inp'},
            #         {'label': 'Pre Process Search Text', 'value': 'use_tags'},
            #         {'label': 'Show Raw Text in Output', 'value': 'out_raw'},
            #         {'label': 'Show Page Preview in Output', 'value': 'out_prev'}
            #     ],
            #     value=['out_raw']
            # ),
            # html.Div('n items to show max'),
            # dcc.Input(
            #     id="set-n_items_max", type="number",
            #     debounce=True, value=settings['n_items_max']),
            # html.Div(id='n_items_max-output-container', children='n items to show max: {}'.format(settings['n_items_max'])),
            # dcc.Dropdown(
            #     id='set-category',
            #     options=[{'label': l, 'value': l} for l in indices],
            #     value=settings['category']
            # ),
            # html.Div('Searchfield'),
            # dcc.Dropdown(
            #     id='set-searchfield',
            #     options=get_search_field_opts(settings['category']),
            #     value=settings['default_search_field']
            # ),
            # html.Div(id='set-searchfield-container', children='searching in field: {}'.format(settings['default_search_field']))
        ])
    ])
])



@app.callback(
    Output("output-keypress", 'children'),
    Output("output-status", 'children'),
    Input('input-1-submit', 'value'),
)
def update_output_qry(search_text):
    try:
        if not search_text:
            return [], html.Div('')

        tempstore = set()
        hits, runtime = w_search.wsearch(search_text)

        res = [w_search.doc2item(hit, tempstore) for hit in hits]
        status = html.Div(f'Searchtext: "{search_text}" | -> {len(hits)} Docs in {runtime*1000:.1f}ms') 
    except Exception as err:
        import traceback
        print(err)
        traceback.print_exc()

    return res, status



if __name__ == '__main__':
    app.run_server(debug=True)