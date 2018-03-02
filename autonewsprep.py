#!/usr/bin/env python
import sys
import os
import re
import string
import pandas as pd
import pymongo

# Connect to finnews 
client = pymongo.MongoClient()
#collection = client.sentidb.collect_news
collection_news = client.sentifine.finnews
collection_prep = client.sentifine.finnews_prep

cursor = collection_news.find( {"sentiment": "N/A"} )
df = pd.DataFrame(list(cursor))

print(df)
