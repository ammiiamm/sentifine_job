##########
# Program: 03_news_transform.py
# Github: @ammiiamm
# Collections: news_raw
# Log pattern: [Program name] [I=Information, S=Status, E=Error, W=Warning] [Description]
# Descriptions:
# 1. Load Thai2vec Pre-trained Word Embedding
# 2. Transform Thai Headline news to word vectors format
##########

from gensim.models import KeyedVectors
from pythainlp.tokenize import word_tokenize

import tensorflow
import sys
import os
import re
import string
import pandas as pd
import numpy as np
import pymongo
import datetime 
import traceback

#init console log
print("[03_news_transform] S Started job at " + str(datetime.datetime.utcnow()))

# Connect to the finnews
client = pymongo.MongoClient()
collection_raw = client.sentifine.news_raw
#collection_extract = client.sentifine.news_extract
#collection_transform = client.sentifine.news_transform

#cursor = collection_extract.find( {"status": "Extracted"} )
cursor = collection_raw.find( {"status": "Extracted"} )
df = pd.DataFrame(list(cursor))
status_default = "Transformed"
#thai2vec_vector = "/home/st118957_ait/sentifine/wordembedding/thai2vec.vec"
thai2vec_vocab = "/home/st118957_ait/sentifine/wordembedding/thai2vec.vocab"


print("[03_news_transform] I Setting up vocab...")
#load vocab into dataframe
thai2vec_vocab = pd.read_csv(thai2vec_vocab, header=None, sep= " ", encoding="UTF-8", names = ["Text", "Key"])
#convert dataframe to dict
my_dict_to_int = thai2vec_vocab.set_index('Text')['Key'].to_dict() 

#assign each word in title of news to int and build a new list of titles with int
# data_ints = []
# i_count = 0
# for index, row in df.iterrows():
#     temp_ints = []
#     for word in row['Text'].split():
#         #print(vocab_to_int[word])
#         if my_dict_to_int.get(word, 0) != 0:
#             temp_ints.append(my_dict_to_int[word])
#     data_ints.append(temp_ints)
#     i_count = i_count + 1

print("[03_news_transform] I Transforming words to numbers...")
for index, row in df.iterrows():
    
    #converting word to int 
    temp_ints = []
    for word in row['dc_title'].split():
        #print(vocab_to_int[word])
        if my_dict_to_int.get(word, 0) != 0:
            temp_ints.append(my_dict_to_int[word])

    # #insert item into news_transformation
    # t = {
    #         #'source':row["source"],
    #         #'source_url':row["source_url"],
    #         #'title':row['title'],
    #         #'published':row['published'],
    #         #'title_detail':row['title_detail'],
    #         #'summary':row['summary'],
    #         #'url_link':row['url_link'],
    #         #'retrieved':row['retrieved'],
    #         #'category':row['category'],
    #         #'dc_title':row['dc_title'],
    #         #'dc_title_detail': row['dc_title_detail'],
    #         #'dc_summary':row['dc_summary'],
    #         'raw_id':row['_id'],
    #         'tf_title_int': temp_ints,
    #         'tf_dt':datetime.datetime.utcnow()
    #         #'status':status_default
    # }    
    # try:    
    #     collection_transform.insert_one(t)
    # except Exception as ex:
    #     print ("[03_news_transform] E Unexpected error while inserting collection news_transform.")
    #     print ("[03_news_transform] E " + str(ex))

    #update status of item in news_raw 
    r_query = { "_id": row["_id"]}
    r_update = {"$set":{ "status": status_default, "tf_title_int": temp_ints, "tf_dt": str(datetime.datetime.utcnow())}}

    try:
        collection_raw.update_one(r_query, r_update)
    except Exception as ex:
        print ("[03_news_transform] E Unexpected error while updating collection news_raw.")
        print ("[03_news_transform] E " + str(ex))
        #raise

#final log
print("[03_news_transform] S Finished job at " + str(datetime.datetime.utcnow()))