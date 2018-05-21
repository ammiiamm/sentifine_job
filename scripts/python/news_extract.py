#!/usr/bin/env python3
# -*- coding: utf-8 -*-

##########
# Program: 02_news_extract.py
# Github: @ammiiamm
# Collections: news_raw
# Log pattern: [Program name] [I=Information, S=Status, E=Error, W=Warning] [Description]
# Descriptions:
# 1. Clean text by removing unnecessary characters
# 2. Thai Text tokenization by the power of deepcut library
##########

import sys
import os
import re
import string
import pandas as pd
import pymongo
import deepcut
import datetime
import traceback
from rq import Queue
from redis import Redis
from news_transform import func_news_transform

def func_news_extract(*args, **kwarg):
    #init console log
    print("[02_news_extract] S Started job at " + str(datetime.datetime.utcnow()))

    # Connect to Mongo
    client = pymongo.MongoClient()
    collection_raw = client.sentifine.news_raw
    #collection_extract = client.sentifine.news_extract

    cursor = collection_raw.find( {"status": "Retrieved"} )
    df = pd.DataFrame(list(cursor))

    status_default = "Extracted"

    for index, row in df.iterrows():

        deepcut_title = row["title"]
        #deepcut_title_detail = row["title_detail"]
        #deepcut_summary = row["summary"]

        # Start removing unused characters and spaces
        #&#39;
        deepcut_title = re.sub(r"&#39;", "", deepcut_title)
        #deepcut_title_detail = re.sub(r"&#39;", "", deepcut_title_detail)
        #deepcut_summary = re.sub(r"&#39;", "", deepcut_summary)
        #&quot;
        deepcut_title = re.sub(r"&quot;", "", deepcut_title)
        #deepcut_title_detail = re.sub(r"&quot;", "", deepcut_title_detail)
        #deepcut_summary = re.sub(r"&quot;", "", deepcut_summary)
        #\xa0
        deepcut_title = re.sub(r"\xa0", "", deepcut_title)
        #deepcut_title_detail = re.sub(r"\xa0", "", deepcut_title_detail)
        #deepcut_summary = re.sub(r"\xa0", "", deepcut_summary)
        #(?<!ฯล)ฯ(?!ลฯ)
        deepcut_title = re.sub(r'(?<!ฯล)ฯ(?!ลฯ)', "", deepcut_title)
        #deepcut_title_detail = re.sub(r'(?<!ฯล)ฯ(?!ลฯ)', "", deepcut_title_detail)
        #deepcut_summary = re.sub(r'(?<!ฯล)ฯ(?!ลฯ)', "", deepcut_summary)
        #replace(r"(-.)", 'ลบ')
        deepcut_title = deepcut_title.replace(r"(-.)", 'ลบ')
        #deepcut_title_detail = deepcut_title_detail.replace(r"(-.)", 'ลบ')
        #deepcut_summary = deepcut_summary.replace(r"(-.)", 'ลบ')
        #replace(r"?", '')
        deepcut_title = deepcut_title.replace(r"?", '')
        #deepcut_title_detail = deepcut_title_detail.replace(r"?", '')
        #deepcut_summary = deepcut_summary.replace(r"?", '')
        #replace(r":", '')
        deepcut_title = deepcut_title.replace(r":", '')
        #deepcut_title_detail = deepcut_title_detail.replace(r":", '')
        #deepcut_summary = deepcut_summary.replace(r":", '')
        #replace(r". จุด", '')
        deepcut_title = deepcut_title.replace(r". จุด", '')
        #deepcut_title_detail = deepcut_title_detail.replace(r". จุด", '')
        #deepcut_summary = deepcut_summary.replace(r". จุด", '')
        #replace(r".จุด", '')
        deepcut_title = deepcut_title.replace(r".จุด", '')
        #deepcut_title_detail = deepcut_title_detail.replace(r".จุด", '')
        #deepcut_summary = deepcut_summary.replace(r".จุด", '')
        #replace(r"-จุด", '-')
        deepcut_title = deepcut_title.replace(r"-จุด", '-')
        #deepcut_title_detail = deepcut_title_detail.replace(r"-จุด", '-')
        #deepcut_summary = deepcut_summary.replace(r"-จุด", '-')
        #replace(r" จุด ", ' ')
        deepcut_title = deepcut_title.replace(r" จุด ", ' ')
        #deepcut_title_detail = deepcut_title_detail.replace(r" จุด ", ' ')
        #deepcut_summary = deepcut_summary.replace(r" จุด ", ' ')
        #replace(r"บวกจุด", 'บวก')
        deepcut_title = deepcut_title.replace(r"บวกจุด", 'บวก')
        #deepcut_title_detail = deepcut_title_detail.replace(r"บวกจุด", 'บวก')
        #deepcut_summary = deepcut_summary.replace(r"บวกจุด", 'บวก')
        #replace(r"ลบจุด", 'ลบ')
        deepcut_title = deepcut_title.replace(r"ลบจุด", 'ลบ')
        #deepcut_title_detail = deepcut_title_detail.replace(r"ลบจุด", 'ลบ')
        #deepcut_summary = deepcut_summary.replace(r"ลบจุด", 'ลบ')
        #+.
        deepcut_title = deepcut_title.replace('+.', '+')
        #deepcut_title_detail = deepcut_title_detail.replace('+.', '+')
        #deepcut_summary = deepcut_summary.replace('+.', '+')
        #-.
        deepcut_title = deepcut_title.replace('-.', '-')
        #deepcut_title_detail = deepcut_title_detail.replace('-.', '-')
        #deepcut_summary = deepcut_summary.replace('-.', '-')
        #«
        deepcut_title = deepcut_title.replace('«', '')
        #deepcut_title_detail = deepcut_title_detail.replace('«', '')
        #deepcut_summary = deepcut_summary.replace('«', '')
        #->
        deepcut_title = deepcut_title.replace('->', '')
        #deepcut_title_detail = deepcut_title_detail.replace('->', '')
        #deepcut_summary = deepcut_summary.replace('->', '')
        #…
        deepcut_title = deepcut_title.replace('…', '')
        #deepcut_title_detail = deepcut_title_detail.replace('…', '')
        #deepcut_summary = deepcut_summary.replace('…', '')
        #1
        deepcut_title = deepcut_title.replace('1', '')
        #deepcut_title_detail = deepcut_title_detail.replace('1', '')
        #deepcut_summary = deepcut_summary.replace('1', '')
        #2
        deepcut_title = deepcut_title.replace('2', '')
        #deepcut_title_detail = deepcut_title_detail.replace('2', '')
        #deepcut_summary = deepcut_summary.replace('2', '')
        #3
        deepcut_title = deepcut_title.replace('3', '')
        #deepcut_title_detail = deepcut_title_detail.replace('3', '')
        #deepcut_summary = deepcut_summary.replace('3', '')
        #4
        deepcut_title = deepcut_title.replace('4', '')
        #deepcut_title_detail = deepcut_title_detail.replace('4', '')
        #deepcut_summary = deepcut_summary.replace('4', '')
        #5
        deepcut_title = deepcut_title.replace('5', '')
        #deepcut_title_detail = deepcut_title_detail.replace('5', '')
        #deepcut_summary = deepcut_summary.replace('5', '')
        #6
        deepcut_title = deepcut_title.replace('6', '')
        #deepcut_title_detail = deepcut_title_detail.replace('6', '')
        #deepcut_summary = deepcut_summary.replace('6', '')
        #7
        deepcut_title = deepcut_title.replace('7', '')
        #deepcut_title_detail = deepcut_title_detail.replace('7', '')
        #deepcut_summary = deepcut_summary.replace('7', '')
        #8
        deepcut_title = deepcut_title.replace('8', '')
        #deepcut_title_detail = deepcut_title_detail.replace('8', '')
        #deepcut_summary = deepcut_summary.replace('8', '')
        #9
        deepcut_title = deepcut_title.replace('9', '')
        #deepcut_title_detail = deepcut_title_detail.replace('9', '')
        #deepcut_summary = deepcut_summary.replace('9', '')
        #0
        deepcut_title = deepcut_title.replace('0', '')
        #deepcut_title_detail = deepcut_title_detail.replace('0', '')
        #deepcut_summary = deepcut_summary.replace('0', '')
        #dot dot dot
        deepcut_title = deepcut_title.replace('...', '')
        #deepcut_title_detail = deepcut_title_detail.replace('...', '')
        #deepcut_summary = deepcut_summary.replace('...', '')
        #dot dot
        deepcut_title = deepcut_title.replace('..', '')
        #deepcut_title_detail = deepcut_title_detail.replace('..', '')
        #deepcut_summary = deepcut_summary.replace('..', '')
        #dot
        deepcut_title = deepcut_title.replace(' . ', '')
        #deepcut_title_detail = deepcut_title_detail.replace(' . ', '')
        #deepcut_summary = deepcut_summary.replace(' . ', '')
        #slash
        deepcut_title = deepcut_title.replace('/', '')
        #deepcut_title_detail = deepcut_title_detail.replace('/', '')
        #deepcut_summary = deepcut_summary.replace('/', '')

        #backslash

        #percent %
        deepcut_title = deepcut_title.replace('%', '')
        #deepcut_title_detail = deepcut_title_detail.replace('%', '')
        #deepcut_summary = deepcut_summary.replace('%', '')
        #dollar $
        deepcut_title = deepcut_title.replace('$', '')
        #deepcut_title_detail = deepcut_title_detail.replace('$', '')
        #deepcut_summary = deepcut_summary.replace('$', '')
        #comma
        deepcut_title = deepcut_title.replace(',', '')
        #deepcut_title_detail = deepcut_title_detail.replace(',', '')
        #deepcut_summary = deepcut_summary.replace(',', '')
        #exclaimation mark !
        deepcut_title = deepcut_title.replace('!', '')
        #deepcut_title_detail = deepcut_title_detail.replace('!', '')
        #deepcut_summary = deepcut_summary.replace('!', '')
        #open rectangle blanket
        deepcut_title = deepcut_title.replace('[', '')
        #deepcut_title_detail = deepcut_title_detail.replace('[', '')
        #deepcut_summary = deepcut_summary.replace('[', '')
        #close rectangle blanket
        deepcut_title = deepcut_title.replace(']', '')
        #deepcut_title_detail = deepcut_title_detail.replace(']', '')
        #deepcut_summary = deepcut_summary.replace(']', '')
        #open blanket
        deepcut_title = deepcut_title.replace('(', '')
        #deepcut_title_detail = deepcut_title_detail.replace('(', '')
        #deepcut_summary = deepcut_summary.replace('(', '')
        #close blanket
        deepcut_title = deepcut_title.replace(')', '')
        #deepcut_title_detail = deepcut_title_detail.replace(')', '')
        #deepcut_summary = deepcut_summary.replace(')', '')
        #open single quote
        deepcut_title = deepcut_title.replace('‘', '')
        #deepcut_title_detail = deepcut_title_detail.replace('‘', '')
        #deepcut_summary = deepcut_summary.replace('‘', '')
        #close single quote
        deepcut_title = deepcut_title.replace('’', '')
        #deepcut_title_detail = deepcut_title_detail.replace('’', '')
        #deepcut_summary = deepcut_summary.replace('’', '')
        #open double quote
        deepcut_title = deepcut_title.replace('“', '')
        #deepcut_title_detail = deepcut_title_detail.replace('“', '')
        #deepcut_summary = deepcut_summary.replace('“', '')
        #close double quote
        deepcut_title = deepcut_title.replace('”', '')
        #deepcut_title_detail = deepcut_title_detail.replace('”', '')
        #deepcut_summary = deepcut_summary.replace('”', '')
        #apostrophe
        deepcut_title = deepcut_title.replace('''''', '')
        #deepcut_title_detail = deepcut_title_detail.replace('''''', '')
        #deepcut_summary = deepcut_summary.replace('''''', '')
        #5space
        deepcut_title = deepcut_title.replace('     ', ' ')
        #deepcut_title_detail = deepcut_title_detail.replace('     ', ' ')
        #deepcut_summary = deepcut_summary.replace('     ', ' ')
        #4space
        deepcut_title = deepcut_title.replace('    ', ' ')
        #deepcut_title_detail = deepcut_title_detail.replace('    ', ' ')
        #deepcut_summary = deepcut_summary.replace('    ', ' ')
        #3space
        deepcut_title = deepcut_title.replace('   ', ' ')
        #deepcut_title_detail = deepcut_title_detail.replace('   ', ' ')
        #deepcut_summary = deepcut_summary.replace('   ', ' ')
        #2space
        deepcut_title = deepcut_title.replace('  ', ' ')
        #deepcut_title_detail = deepcut_title_detail.replace('  ', ' ')
        #deepcut_summary = deepcut_summary.replace('  ', ' ')

        ### Then left trim and deepcut
        deepcut_title =  ' '.join(deepcut.tokenize(deepcut_title.lstrip()))
        #deepcut_title_detail =  ' '.join(deepcut.tokenize(deepcut_title_detail.lstrip()))
        #deepcut_summary =  ' '.join(deepcut.tokenize(deepcut_summary.lstrip()))

        #insert item into news_extract
        #e = {
                #'source':row["source"],
                #'source_url':row["source_url"],
                #'title':row['title'],
                #'published':row['published'],
                #'title_detail':row['title_detail'],
                #'summary':row['summary'],
                #'url_link':row['url_link'],
                #'retrieved':row['retrieved'],
                #'category':row['category'],
                #'raw_id':row['_id'],
                #'dc_title':deepcut_title,
                #'dc_title_detail': deepcut_title_detail,
                #'dc_summary':deepcut_summary,
                #'ex_dt':datetime.datetime.utcnow()
                #'status':status_default
        #}    
        #try:    
        #    collection_extract.insert_one(e)
        #except Exception as ex:
        #    print ("[02_news_extract] E Unexpected error while inserting collection news_extract.")
        #    print ("[02_news_extract] E " + str(ex))
            #raise

        #update status of item in news_raw 
        r_query = { "_id": row["_id"]}
        r_update = {"$set":{ "status": status_default, "dc_title": deepcut_title, "ex_dt": str(datetime.datetime.utcnow())}}

        try:
            collection_raw.update_one(r_query, r_update)
        except Exception as ex:
            print ("[02_news_extract] E Unexpected error while updating collection news_raw.")
            print ("[02_news_extract] E " + str(ex))
            #raise

    #final log
    print("[02_news_extract] S Finished job at " + str(datetime.datetime.utcnow()))
    job_status = "news_extract complete"
    # Tell RQ what Redis connection to use
    redis_conn = Redis()
    q = Queue('newsfeed', connection=redis_conn)  # no args implies the default queue
    q.enqueue(func_news_transform)
    return(job_status)