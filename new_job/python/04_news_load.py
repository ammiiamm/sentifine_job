##########
# Program: 04_news_load.py
# Github: @ammiiamm
# Collections: news_raw, news_sentifine
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

#init console log
print("[04_news_load] S Started job at " + str(datetime.datetime.utcnow()))

f_model_h5 = "/home/st118957_ait/sentifine/model/thai2vec-3_model.h5"
f_model_json = "/home/st118957_ait/sentifine/model/thai2vec-3_model_json.json"
f_model_weights = "/home/st118957_ait/sentifine/model/thai2vec-3_model_weights.h5"
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
#load json for our model's architecture
with open(f_model_json) as ff:
    model_json=ff.read()
    model=keras.models.model_from_json(model_json)
#load weights
model.load_weights(f_model_weights)

print("[04_news_load] I Setting up parameters...")
model.compile(loss='categorical_crossentropy',
              optimizer='adam',
              metrics=['accuracy'])
title_int = pad_sequences(df['tf_title_int'], maxlen = 300) #pad sequence of tf_title_int

print("[04_news_load] I Inferencing...")
news_fit = model.predict(title_int, batch_size=10, verbose=1)
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

    #insert item into news_transformation
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
            'fetch_dt':str(datetime.datetime.utcnow())
    }    
    try:    
        collection_sentifine.insert_one(s)
    except Exception as ex:
        print ("[04_news_load] E Unexpected error while inserting collection news_sentifine.")
        print ("[04_news_load] E " + str(ex))

    #update status of item in news_extract 
    r_query = { "_id": row["_id"]}
    r_update = {"$set":{ "status": status_default, "ld_dt": str(datetime.datetime.utcnow())}}

    try:
        collection_raw.update_one(r_query, r_update)
    except Exception as ex:
        print ("[04_news_load] E Unexpected error while updating collection news_transform.")
        print ("[04_news_load] E " + str(ex))

#final log
print("[04_news_load] S Finished job at " + str(datetime.datetime.utcnow()))
