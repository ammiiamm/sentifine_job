#!/usr/bin/env python3
# -*- coding: utf-8 -*-

##########
# Program: 04_news_load.py
# Github: @ammiiamm
# Collections: news_raw, finnew
# Log pattern: [Program name] [I=Information, S=Status, E=Error, W=Warning] [Description]
# Descriptions:
# 1. Pass the word vectors through our Unsupervised ML Model
# 2. Save the sentiment output in the final Collection
##########

from keras.preprocessing.sequence import pad_sequences
import keras.models
import tensorflow
import sys
import os
import re
import string
import pandas as pd
import pymongo
import datetime 
import traceback
from keras.models import load_model

def func_news_load(*args, **kwarg):

    #init console log
    print("[04_news_load] S Started job at " + str(datetime.datetime.utcnow()))

    f_model_json = "/Users/ammii/sentifine_job/pretrain_thai2vec_no_dropout_model_json.json"
    f_model_weights = "/Users/ammii/sentifine_job/pretrain_thai2vec_no_dropout_model_weights.h5"
    f_model = "/Users/ammii/sentifine_job/pretrain_thai2vec_no_dropout_model.h5"
    status_default = "Loaded"

    # Connect to MongoDB
    client = pymongo.MongoClient()
    collection_raw = client.sentifine.news_raw
    #collection_transform = client.sentifine.news_transform
    #collection_sentifine = client.sentifine.news_sentifine
    collection_sentifine = client.sentifine.finnews
    cursor = collection_raw.find( {"status": "Transformed"} )

    print("[04_news_load] I Loading news and model...")
    #load news
    df = pd.DataFrame(list(cursor))
    print("[04_news_load] I No. of news to be transformed: " + str(len(df)))

    if len(df) > 0:
        #load json for our model's architecture
        with open(f_model_json) as ff:
            model_json=ff.read()
            model=keras.models.model_from_json(model_json)
        #load weights
        model.load_weights(f_model_weights)

        #model = load_model(f_model)

        print("[04_news_load] I Setting up parameters...")
    
        model.compile(loss='categorical_crossentropy',
                    #optimizer='adam',
                    optimizer='adamax',
                    metrics=['accuracy'])
        title_int = pad_sequences(df['tf_title_int'], maxlen = 300) #pad sequence of tf_title_int

        print("[04_news_load] I Inferencing...")
        #news_fit = model.predict(title_int, batch_size=10, verbose=1)
        news_class = model.predict_classes(title_int)

        print("[04_news_load] I Updating sentiments...")

        for index, row in df.iterrows():
            
            i_sentiment = ''
            if news_class[index] == 0:
                i_sentiment = "Negative"
            elif news_class[index] == 1:
                i_sentiment = "Neutral"
            elif news_class[index] == 2:
                i_sentiment = "Positive"
            else:
                i_sentiment = "NA"

            #insert item into finnew
            s = {
                    'source':row["source"],
                    'source_url':row["source_url"],
                    'title':row['title'],
                    'published':row['published'],
                    'title_detail':row['title_detail'],
                    'summary':row['summary'],
                    'url_link':row['url_link'],
                    'retrieved':row['retrieved'],
                    'category':row['category'],
                    'sentiment':i_sentiment,
                    'filter_BOT':row['filter_BOT'],
                    'fetch_dt':str(datetime.datetime.utcnow())
            }    
            try:    
                collection_sentifine.insert_one(s)
            except Exception as ex:
                print ("[04_news_load] E Unexpected error while inserting collection finnew.")
                print ("[04_news_load] E " + str(ex))

            #update status of item in news_raw 
            r_query = { "_id": row["_id"]}
            r_update = {"$set":{ "status": status_default, "ld_dt": str(datetime.datetime.utcnow())}}

            try:
                collection_raw.update_one(r_query, r_update)
            except Exception as ex:
                print ("[04_news_load] E Unexpected error while updating collection news_raw.")
                print ("[04_news_load] E " + str(ex))
    else:
        print ("[04_news_load] I The rest processes were exempted due to 0 row of news")

    #final log
    print("[04_news_load] S Finished job at " + str(datetime.datetime.utcnow()))
