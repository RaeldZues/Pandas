"""
Author: mattyb
Date: 7 Apr 2018
Description: This script reads in multiple bro log files from a path from input, and pushes to a hard coded
             Elasticsearch instance. It uses multiprocessing for some performance gain.

"""
import pandas as pd
from elasticsearch import helpers
import elasticsearch
import json
import glob2
import concurrent.futures
import itertools


def df_to_elk(df, tag, index):
    df['tag'] = tag
    df['timestamp'] = pd.to_datetime(df['ts'], unit='s')
    tmp = df.to_json(orient='records', date_format='iso')
    df_json = json.loads(tmp)
    df_list = []
    for doc in df_json:
        try:
            action = {"_index": index, "_source": doc}
            df_list.append(action)
        except Exception as ex:
            print(ex)
            pass
    try:
        helpers.bulk(es, df)
        print('going to elk')
    except Exception as ex:
        print(ex)
        pass


def read_bro(file):
    with open(file) as f:
        separator = f.readline().strip().split()[-1].encode().decode('unicode_escape')
        set_separator = f.readline().strip().split(separator)[-1]
        empty_field = f.readline().strip().split(separator)[-1]
        unset_field = f.readline().strip().split(separator)[-1]
        path = f.readline().strip().split(separator)[-1]
        open_time = f.readline().strip().split(separator)[-1]
        fields = f.readline().strip().split(separator)[1:]
        types = f.readline().strip().split(separator)[1:]
        default_kwargs = {"sep": separator,
                          "header": None,
                          "names": fields,
                          "comment": "#"}
        index = "bro-" + path
        df_to_elk(pd.read_csv(f, **default_kwargs), tag=path, index=index)


if __name__ == '__main__':
    print(r"Input the path to your folder containing all of your bro logs you'd like to load into elk")
    print(r"Example:  C:\users\<user>\pathtobro\**")
    print(r'This will show all files and sub-files in your directory')
    path = glob2.glob(input())
    es = elasticsearch.Elasticsearch(<enter your ip here>, timeout=100000)
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as proc:
        try:
            proc.map(read_bro, str(path))
        except Exception as e:
            print(e)
            pass
