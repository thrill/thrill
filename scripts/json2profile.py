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
    df = df[(df['class'] == sclass) & (df['event'] == 'profile')][['ts',key]].set_index('ts')
    return df

# extract sum of two profile series from sclass with key
def SeriesSum(sclass,key1,key2):
    df = df_stats
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

def BlockPoolSeries(key):
    return Series('BlockPool', key)

def BlockPoolSeriesSum(key1,key2):
    return SeriesSum('BlockPool', key1, key2)

def series_to_xy(df):
    return [list(a) for a in zip(df.index.tolist(), df.iloc[:,0].values.tolist())]

def xy_aggregated(df):
    # set index to ts and group all entries in a second.
    df = df.dropna()
    df = df.groupby(lambda r : int(r / 1e3) * 1e3).aggregate(np.average)
    return series_to_xy(df)

# generate table of DIAs
def dia_table():
    df = df_stats
    df = df[(df['class'] == 'DIABase') & (df['event'] == 'create') & (df['worker_rank'] == 0)]
    df = df[['id','label','type','parents']]
    df['id'] = df['id'].astype(int)
    df['label_id'] = df['label'] + "." + df['id'].map(str)
    df = df.set_index('id')
    return df

# generate table of Stream statistics
def stream_table():
    df = df_stats
    df = df[(df['class'] == 'Stream') & (df['event'] == 'close')]
    df = df[['ts','id','dia_id','host_rank','worker_rank','rx_bytes','tx_bytes']].set_index('ts')
    df['id'] = df['id'].astype(int)
    df['dia_id'] = df['dia_id'].astype(int)
    df['host_rank'] = df['host_rank'].astype(int)
    df['worker_rank'] = df['worker_rank'].astype(int)
    df['rx_bytes'] = df['rx_bytes'].astype(int)
    df['tx_bytes'] = df['tx_bytes'].astype(int)
    df = df.sort_values(by=['id','dia_id','host_rank','worker_rank'])
    df = df.merge(right=dia_table(), how='left', left_on='dia_id', right_index=True)
    df = df[['id','label_id','host_rank','worker_rank','rx_bytes','tx_bytes']]
    return df

def stream_html_table():
    return stream_table().to_html()

# generate table of Stream summary statistics
def stream_summary_table():
    df = df_stats
    df = df[(df['class'] == 'Stream') & (df['event'] == 'close')]
    df = df[['ts','id','dia_id','host_rank','worker_rank','rx_bytes','tx_bytes']].set_index('ts')
    df['id'] = df['id'].astype(int)
    df['dia_id'] = df['dia_id'].astype(int)
    df['host_rank'] = df['host_rank'].astype(int)
    df['worker_rank'] = df['worker_rank'].astype(int)
    df['rx_bytes'] = df['rx_bytes'].astype(int)
    df['tx_bytes'] = df['tx_bytes'].astype(int)
    df = df.sort_values(by=['id','dia_id','host_rank','worker_rank'])
    df = df.merge(right=dia_table(), how='left', left_on='dia_id', right_index=True)
    df = df[['id','label_id','rx_bytes','tx_bytes']]
    df = df.groupby(['id','label_id']).aggregate(np.sum)
    return df

def stream_summary_html_table():
    return stream_summary_table().to_html()

# generate table of File statistics
def file_table():
    df = df_stats
    df = df[(df['class'] == 'File') & (df['event'] == 'close')]
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

#print( file_table() )
#sys.exit(0)

##########################################################################

@app.route('/')
@app.route('/index')
def index(chartID = 'chart_ID'):
    title = {'text': 'My Title'}
    chart = {'renderTo': chartID, 'zoomType': 'x', 'panning': 'true', 'panKey': 'shift'}
    xAxis = {'type': 'datetime', 'title': {'text': 'Execution Time'}}
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
        { 'data': xy_aggregated(ProcSeries('cpu_user')), 'name': 'CPU User', 'tooltip': { 'valueSuffix': ' %' } },
        { 'data': xy_aggregated(ProcSeries('cpu_sys')), 'name': 'CPU Sys', 'tooltip': { 'valueSuffix': ' %' } },
        { 'data': xy_aggregated(ProcSeriesSum('cpu_user','cpu_sys')), 'name': 'CPU', 'tooltip': { 'valueSuffix': ' %' } },
        #{ 'data': xy_aggregated(ProcSeries('pr_rss')), 'name': 'Mem RSS', 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(NetManagerSeries('tx_speed')), 'name': 'TX net', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(NetManagerSeries('rx_speed')), 'name': 'RX net', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(NetManagerSeriesSum('tx_speed', 'rx_speed')), 'name': 'TX+RX net', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeries('net_tx_bytes')), 'name': 'TX', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeries('net_rx_bytes')), 'name': 'RX', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeriesSum('net_tx_bytes', 'net_rx_bytes')), 'name': 'TX+RX', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeries('diskstats_rd_bytes')), 'name': 'I/O read', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeries('diskstats_wr_bytes')), 'name': 'I/O write', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(ProcSeriesSum('diskstats_rd_bytes', 'diskstats_wr_bytes')), 'name': 'I/O', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(BlockPoolSeries('total_bytes')), 'name': 'Data bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('ram_bytes')), 'name': 'RAM bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('reading_bytes')), 'name': 'Reading bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('writing_bytes')), 'name': 'Writing bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('pinned_bytes')), 'name': 'Pinned bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('unpinned_bytes')), 'name': 'Unpinned bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('swapped_bytes')), 'name': 'Swapped bytes', 'yAxis': 2, 'tooltip': { 'valueSuffix': ' B' } },
        { 'data': xy_aggregated(BlockPoolSeries('rd_speed')), 'name': 'I/O read', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
        { 'data': xy_aggregated(BlockPoolSeries('wr_speed')), 'name': 'I/O write', 'yAxis': 1, 'tooltip': { 'valueSuffix': ' B/s' } },
    ]
    return render_template(
        'index.html', chartID=chartID, chart=chart,
        title=title, xAxis=xAxis, yAxis=yAxis,
        legend=legend, plotOptions=plotOptions,
        series=series,
        stream_summary_table=stream_summary_html_table(),
        stream_table=stream_html_table(),
        file_table=file_html_table()
    )

if __name__ == '__main__':
        app.run(debug = True, host='0.0.0.0', port=8080, passthrough_errors=True)

##########################################################################
