#!/usr/bin/env python3
##########################################################################
# scripts/json2profile.py
#
#
# Part of Project Thrill - http://project-thrill.org
#
# Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
#
# All rights reserved. Published under the BSD-2 license in the LICENSE file.
##########################################################################

import sys
import json
import collections

from flask import Flask, render_template
app = Flask(__name__)

import pandas as pd
import numpy as np

# parse Json in all input files
stats = []

# flatten nested dictionary by replacing them with d1_d2_key entries
def flatten_dicts(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_dicts(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

for input_file in sys.argv[1:]:
    with open(input_file, 'r') as json_file:
        for line in json_file:
            try:
                r = json.loads(line)
                if 'class' in r:
                    stats.append(flatten_dicts(r))
            except ValueError:
                print("JSON line invalid: " + line)
                pass

print("Read " + str(len(stats)) + " json events from " + str(len(sys.argv) - 1) + " files")

# transform Json into a pandas DataFrame
df_stats = pd.DataFrame(stats).sort_values(by='ts')

# find start of program, subtract from all timestamps
min_ts = df_stats['ts'].min()
df_stats['ts'] = (df_stats['ts'] - min_ts) / 1e3

# extract a plain profile series from sclass with key
def Series(sclass,key):
    df = df_stats
    if not key in df:
        return pd.DataFrame(columns=['ts',key]).set_index('ts')
    df = df[(df['class'] == sclass) & (df['event'] == 'profile')][['ts',key]].set_index('ts')
    return df

# extract sum of two profile series from sclass with key
def SeriesSum(sclass,key1,key2):
    df = df_stats
    if not key1 in df or not key2 in df:
        return pd.DataFrame(columns=['ts',key1]).set_index('ts')
    df = df[(df['class'] == sclass) & (df['event'] == 'profile')][['ts',key1,key2]]
    df[key1] = df[key1] + df[key2]
    del df[key2]
    df = df.set_index('ts')
    return df

def ProcSeries(key):
    return Series('LinuxProcStats', key)

def ProcSeriesSum(key1,key2):
    return SeriesSum('LinuxProcStats', key1, key2)

def NetManagerSeries(key):
    return Series('NetManager', key)

def NetManagerSeriesSum(key1,key2):
    return SeriesSum('NetManager', key1, key2)

def MemProfileSeries(key):
    return Series('MemProfile', key)

def BlockPoolSeries(key):
    return Series('BlockPool', key)

def BlockPoolSeriesSum(key1,key2):
    return SeriesSum('BlockPool', key1, key2)

def series_to_xy(df):
    return [list(a) for a in zip(df.index.tolist(), df.iloc[:,0].values.tolist())]

def xy_aggregated(df):
    # set index to ts and group all entries in a second.
    df = df.dropna()
    df = df.groupby(lambda r : int(r / 1e3 * 10) * 1e3 / 10).aggregate(np.average)
    return series_to_xy(df)

# extract program name and cmdline
def progname():
    df = df_stats
    try:
        df = df[(df['class'] == 'Cmdline') & (df['event'] == 'start') & (df['host_rank'] == 0)]
        if len(df) == 0:
            return "unknown"
        return df['program'].values.tolist()[0]
    except KeyError:
        return "unknown"

# generate table of DIAs
def dia_table():
    df = df_stats
    df = df[(df['class'] == 'DIABase') & (df['event'] == 'create') & (df['worker_rank'] == 0)]
    if len(df) == 0:
        return pd.DataFrame(columns=['id','label','label_id']).set_index('id')
    df = df[['id','label','type','parents']]
    df['id'] = df['id'].astype(int)
    df['label_id'] = df['label'] + "." + df['id'].map(str)
    df = df.set_index('id')
    return df

# generate table of Stream statistics
def stream_table():
    df = df_stats
    cols = ['host_rank','worker_rank','rx_net_items','tx_net_items','rx_net_bytes','tx_net_bytes']
    cols = cols + ['rx_int_items','tx_int_items','rx_int_bytes','tx_int_bytes']
    df = df[(df['class'] == 'Stream') & (df['event'] == 'close')]
    if len(df) == 0:
        return pd.DataFrame(columns=['ts']).set_index('ts')
    df = df[['ts','id','dia_id'] + cols].set_index('ts')
    df['id'] = df['id'].astype(int)
    df['dia_id'] = df['dia_id'].astype(int)
    df['host_rank'] = df['host_rank'].astype(int)
    df['worker_rank'] = df['worker_rank'].astype(int)
    df['rx_net_items'] = df['rx_net_items'].astype(int)
    df['tx_net_items'] = df['tx_net_items'].astype(int)
    df['rx_net_bytes'] = df['rx_net_bytes'].astype(int)
    df['tx_net_bytes'] = df['tx_net_bytes'].astype(int)
    df['rx_int_items'] = df['rx_int_items'].astype(int)
    df['tx_int_items'] = df['tx_int_items'].astype(int)
    df['rx_int_bytes'] = df['rx_int_bytes'].astype(int)
    df['tx_int_bytes'] = df['tx_int_bytes'].astype(int)
    df = df.sort_values(by=['id','dia_id','host_rank','worker_rank'])
    df = df.merge(right=dia_table(), how='left', left_on='dia_id', right_index=True)
    df = df[['id','label_id'] + cols]
    return df

def stream_html_table():
    return stream_table().to_html()

# generate table of Stream summary statistics
def stream_summary_table():
    df = df_stats
    cols = ['rx_net_items','tx_net_items','rx_net_bytes','tx_net_bytes']
    cols = cols + ['rx_int_items','tx_int_items','rx_int_bytes','tx_int_bytes']
    df = df[(df['class'] == 'Stream') & (df['event'] == 'close')]
    if len(df) == 0:
        return pd.DataFrame(columns=['ts']).set_index('ts')
    df = df[['ts','id','dia_id'] + cols].set_index('ts')
    df['id'] = df['id'].astype(int)
    df['dia_id'] = df['dia_id'].astype(int)
    df['rx_net_items'] = df['rx_net_items'].astype(int)
    df['tx_net_items'] = df['tx_net_items'].astype(int)
    df['rx_net_bytes'] = df['rx_net_bytes'].astype(int)
    df['tx_net_bytes'] = df['tx_net_bytes'].astype(int)
    df['rx_int_items'] = df['rx_int_items'].astype(int)
    df['tx_int_items'] = df['tx_int_items'].astype(int)
    df['rx_int_bytes'] = df['rx_int_bytes'].astype(int)
    df['tx_int_bytes'] = df['tx_int_bytes'].astype(int)
    df['rx_items'] = df['rx_net_items'] + df['rx_int_items']
    df['rx_bytes'] = df['rx_net_bytes'] + df['rx_int_bytes']
    df['tx_items'] = df['tx_net_items'] + df['tx_int_items']
    df['tx_bytes'] = df['tx_net_bytes'] + df['tx_int_bytes']
    df = df.sort_values(by=['id','dia_id'])
    df = df.merge(right=dia_table(), how='left', left_on='dia_id', right_index=True)
    df = df[['id','label_id','rx_items','tx_items','rx_bytes','tx_bytes'] + cols]
    df = df.groupby(['id','label_id']).aggregate(np.sum)
    return df

def stream_summary_html_table():
    return stream_summary_table().to_html()

# generate table of File statistics
def file_table():
    df = df_stats
    df = df[(df['class'] == 'File') & (df['event'] == 'close')]
    if len(df) == 0:
        return pd.DataFrame(columns=['ts']).set_index('ts')
    df = df.set_index('ts')
    # Filter out all File which never contained items
    df = df[df['items'] > 0]
    df['id'] = df['id'].astype(int)
    df['dia_id'] = df['dia_id'].astype(int)
    df['host_rank'] = df['host_rank'].astype(int)
    df['items'] = df['items'].astype(int)
    df['bytes'] = df['bytes'].astype(int)
    df = df.sort_values(by=['dia_id','id','host_rank'])
    df = df.merge(right=dia_table(), how='left', left_on='dia_id', right_index=True)
    df = df[['label_id','id','host_rank','items','bytes']]
    return df

def file_html_table():
    return file_table().to_html()

# generate list of StageBuilder lines
def stage_lines():
    df = df_stats
    try:
        df = df[(df['class'] == 'StageBuilder') & (df['worker_rank'] == 0)]
        df = df[(df['event'] == 'execute-start') | (df['event'] == 'pushdata-start')]
        df = df[['ts','event','id','label']].set_index('ts')
        df['id'] = df['id'].astype(int)
        df['label'] = df['label'] + '.' + df['id'].map(str) + ' ' + df['event']
        df = [{'color':'#888888', 'width':1, 'value': a[0], 'label': { 'text': a[1] }} for a in zip(df.index.tolist(), df['label'].values.tolist())]
        return df
    except KeyError:
        return []

#print( stage_lines() )
#sys.exit(0)

##########################################################################

@app.route('/')
@app.route('/index')
def index(chartID = 'chart_ID'):
    title = {'text': progname()}
    chart = {'renderTo': chartID, 'zoomType': 'x', 'panning': 'true', 'panKey': 'shift'}
    xAxis = {'type': 'datetime', 'title': {'text': 'Execution Time'}, 'plotLines': stage_lines()}
    yAxis = [
        { 'title': { 'text': 'CPU Load (%)' } },
        { 'title': { 'text': 'Network/Disk (B/s)' }, 'opposite': 'true' },
        { 'title': { 'text': 'Data System (B)' }, 'opposite': 'true' }
    ]
    legend = {
        'layout': 'vertical',
        'align': 'right',
        'verticalAlign': 'middle',
        'borderWidth': 0
    }
    plotOptions = {
        'series': { 'animation': 0, 'marker': { 'radius': 2.5 } }
    }
    series = [
        { 'data': xy_aggregated(ProcSeriesSum('cpu_user','cpu_sys')), 'name': 'CPU', 'tooltip': { 'valueSuffix': ' %' } },
        { 'data': xy_aggregated(ProcSeries('cpu_user')), 'name': 'CPU User', 'visible': False, 'tooltip': { 'valueSuffix': ' %' } },
        { 'data': xy_aggregated(ProcSeries('cpu_sys')), 'name': 'CPU Sys', 'visible': False, 'tooltip': { 'valueSuffix': ' %' } },
        { 'data': xy_aggregated(ProcSeries('pr_rss')), 'name': 'Mem RSS', 'yAxis': 2, 'visible': False, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(NetManagerSeriesSum('tx_speed', 'rx_speed')), 'name': 'TX+RX net', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(NetManagerSeries('tx_speed')), 'name': 'TX net', 'yAxis': 1, 'visible': False, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(NetManagerSeries('rx_speed')), 'name': 'RX net', 'yAxis': 1, 'visible': False, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeriesSum('net_tx_bytes', 'net_rx_bytes')), 'name': 'TX+RX sys net', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeries('net_tx_bytes')), 'name': 'TX sys net', 'yAxis': 1, 'visible': False, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeries('net_rx_bytes')), 'name': 'RX sys net', 'yAxis': 1, 'visible': False, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeriesSum('diskstats_rd_bytes', 'diskstats_wr_bytes')), 'name': 'I/O sys', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeries('diskstats_rd_bytes')), 'name': 'I/O sys read', 'yAxis': 1, 'visible': False, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeries('diskstats_wr_bytes')), 'name': 'I/O sys write', 'yAxis': 1, 'visible': False, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(BlockPoolSeries('total_bytes')), 'name': 'Data bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('ram_bytes')), 'name': 'RAM bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('reading_bytes')), 'name': 'Reading bytes', 'yAxis': 2, 'visible': False, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('writing_bytes')), 'name': 'Writing bytes', 'yAxis': 2, 'visible': False, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('pinned_bytes')), 'name': 'Pinned bytes', 'yAxis': 2, 'visible': False, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('unpinned_bytes')), 'name': 'Unpinned bytes', 'yAxis': 2, 'visible': False, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('swapped_bytes')), 'name': 'Swapped bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('rd_speed')), 'name': 'I/O read', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(BlockPoolSeries('wr_speed')), 'name': 'I/O write', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(MemProfileSeries('total')), 'name': 'Mem Total', 'yAxis': 2, 'visible': False, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(MemProfileSeries('float')), 'name': 'Mem Float', 'yAxis': 2, 'visible': False, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(MemProfileSeries('base')), 'name': 'Mem Base', 'yAxis': 2, 'visible': False, 'tooltip': { 'valueSuffix': ' B' } },
    ]
    return render_template(
        'index.html', page_title=progname(),
        chartID=chartID, chart=chart,
        title=title, xAxis=xAxis, yAxis=yAxis,
        legend=legend, plotOptions=plotOptions,
        series=json.dumps(series),
        stream_summary_table=stream_summary_html_table(),
        stream_table=stream_html_table(),
        file_table=file_html_table()
    )

if __name__ == '__main__':
        app.run(debug = True, host='0.0.0.0', port=8080, passthrough_errors=True)

##########################################################################
