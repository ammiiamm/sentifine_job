#!/usr/bin/env python
#%reload_ext autoreload
#%autoreload 2
#%matplotlib inline

import tensorflow

from pythainlp.tokenize import word_tokenize
from gensim.models import KeyedVectors
from sklearn.manifold import TSNE
import matplotlib.font_manager as fm
import dill as pickle

#import gensim
import sys
import os
import re
import string
import pandas as pd
import pymongo
import deepcut

PATH='/home/st118957_ait/sentifine/job/'

# Connect to the finnews
client = pymongo.MongoClient()
collection_news = client.sentifine.finnews_raw
collection_prep = client.sentifine.finnews_prep

cursor = collection_news.find( {"sentiment": "N/A"} )
df = pd.DataFrame(list(cursor))

sentiment_default = "Pending"

#load Thai2vec model into gensim
#model = KeyedVectors.load_word2vec_format('/home/st118957_ait/sentifine/job/thai2vec.vec',binary=False)
model = KeyedVectors.load_word2vec_format('/home/st118957_ait/sentifine/job/wiki.th.vec',binary=False)
#create dataframe
thai2dict = {}
for word in model.index2word:
    thai2dict[word] = model[word]
thai2vec = pd.DataFrame.from_dict(thai2dict,orient='index')
print(thai2vec.head())

for index, row in df.iterrows():

    ##############
    ### Start removing unused characters and spaces
    ##############
    #&#39;
    row["title"] = re.sub(r"&#39;", "", row["title"])
    row["title_detail"] = re.sub(r"&#39;", "", row["title_detail"])
    row["summary"] = re.sub(r"&#39;", "", row["summary"])
    #&quot;
    row["title"] = re.sub(r"&quot;", "", row["title"])
    row["title_detail"] = re.sub(r"&quot;", "", row["title_detail"])
    row["summary"] = re.sub(r"&quot;", "", row["summary"])
    #\xa0
    row["title"] = re.sub(r"\xa0", "", row["title"])
    row["title_detail"] = re.sub(r"\xa0", "", row["title_detail"])
    row["summary"] = re.sub(r"\xa0", "", row["summary"])
    #(?<!ฯล)ฯ(?!ลฯ)
    row["title"] = re.sub(r'(?<!ฯล)ฯ(?!ลฯ)', "", row["title"])
    row["title_detail"] = re.sub(r'(?<!ฯล)ฯ(?!ลฯ)', "", row["title_detail"])
    row["summary"] = re.sub(r'(?<!ฯล)ฯ(?!ลฯ)', "", row["summary"])
    #replace(r"(-.)", 'ลบ')
    row["title"] = row["title"].replace(r"(-.)", 'ลบ')
    row["title_detail"] = row["title_detail"].replace(r"(-.)", 'ลบ')
    row["summary"] = row["summary"].replace(r"(-.)", 'ลบ')
    #replace(r"?", '')
    row["title"] = row["title"].replace(r"?", '')
    row["title_detail"] = row["title_detail"].replace(r"?", '')
    row["summary"] = row["summary"].replace(r"?", '')
    #replace(r":", '')
    row["title"] = row["title"].replace(r":", '')
    row["title_detail"] = row["title_detail"].replace(r":", '')
    row["summary"] = row["summary"].replace(r":", '')
    #replace(r". จุด", '')
    row["title"] = row["title"].replace(r". จุด", '')
    row["title_detail"] = row["title_detail"].replace(r". จุด", '')
    row["summary"] = row["summary"].replace(r". จุด", '')
    #replace(r".จุด", '')
    row["title"] = row["title"].replace(r".จุด", '')
    row["title_detail"] = row["title_detail"].replace(r".จุด", '')
    row["summary"] = row["summary"].replace(r".จุด", '')
    #replace(r"-จุด", '-')
    row["title"] = row["title"].replace(r"-จุด", '-')
    row["title_detail"] = row["title_detail"].replace(r"-จุด", '-')
    row["summary"] = row["summary"].replace(r"-จุด", '-')
    #replace(r" จุด ", ' ')
    row["title"] = row["title"].replace(r" จุด ", ' ')
    row["title_detail"] = row["title_detail"].replace(r" จุด ", ' ')
    row["summary"] = row["summary"].replace(r" จุด ", ' ')
    #replace(r"บวกจุด", 'บวก')
    row["title"] = row["title"].replace(r"บวกจุด", 'บวก')
    row["title_detail"] = row["title_detail"].replace(r"บวกจุด", 'บวก')
    row["summary"] = row["summary"].replace(r"บวกจุด", 'บวก')
    #replace(r"ลบจุด", 'ลบ')
    row["title"] = row["title"].replace(r"ลบจุด", 'ลบ')
    row["title_detail"] = row["title_detail"].replace(r"ลบจุด", 'ลบ')
    row["summary"] = row["summary"].replace(r"ลบจุด", 'ลบ')
    #+.
    row["title"] = row["title"].replace('+.', '+')
    row["title_detail"] = row["title_detail"].replace('+.', '+')
    row["summary"] = row["summary"].replace('+.', '+')
    #-.
    row["title"] = row["title"].replace('-.', '-')
    row["title_detail"] = row["title_detail"].replace('-.', '-')
    row["summary"] = row["summary"].replace('-.', '-')
    #«
    row["title"] = row["title"].replace('«', '')
    row["title_detail"] = row["title_detail"].replace('«', '')
    row["summary"] = row["summary"].replace('«', '')
    #->
    row["title"] = row["title"].replace('->', '')
    row["title_detail"] = row["title_detail"].replace('->', '')
    row["summary"] = row["summary"].replace('->', '')
    #…
    row["title"] = row["title"].replace('…', '')
    row["title_detail"] = row["title_detail"].replace('…', '')
    row["summary"] = row["summary"].replace('…', '')
    #1
    row["title"] = row["title"].replace('1', '')
    row["title_detail"] = row["title_detail"].replace('1', '')
    row["summary"] = row["summary"].replace('1', '')
    #2
    row["title"] = row["title"].replace('2', '')
    row["title_detail"] = row["title_detail"].replace('2', '')
    row["summary"] = row["summary"].replace('2', '')
    #3
    row["title"] = row["title"].replace('3', '')
    row["title_detail"] = row["title_detail"].replace('3', '')
    row["summary"] = row["summary"].replace('3', '')
    #4
    row["title"] = row["title"].replace('4', '')
    row["title_detail"] = row["title_detail"].replace('4', '')
    row["summary"] = row["summary"].replace('4', '')
    #5
    row["title"] = row["title"].replace('5', '')
    row["title_detail"] = row["title_detail"].replace('5', '')
    row["summary"] = row["summary"].replace('5', '')
    #6
    row["title"] = row["title"].replace('6', '')
    row["title_detail"] = row["title_detail"].replace('6', '')
    row["summary"] = row["summary"].replace('6', '')
    #7
    row["title"] = row["title"].replace('7', '')
    row["title_detail"] = row["title_detail"].replace('7', '')
    row["summary"] = row["summary"].replace('7', '')
    #8
    row["title"] = row["title"].replace('8', '')
    row["title_detail"] = row["title_detail"].replace('8', '')
    row["summary"] = row["summary"].replace('8', '')
    #9
    row["title"] = row["title"].replace('9', '')
    row["title_detail"] = row["title_detail"].replace('9', '')
    row["summary"] = row["summary"].replace('9', '')
    #0
    row["title"] = row["title"].replace('0', '')
    row["title_detail"] = row["title_detail"].replace('0', '')
    row["summary"] = row["summary"].replace('0', '')
    #dot dot dot
    row["title"] = row["title"].replace('...', '')
    row["title_detail"] = row["title_detail"].replace('...', '')
    row["summary"] = row["summary"].replace('...', '')
    #dot dot
    row["title"] = row["title"].replace('..', '')
    row["title_detail"] = row["title_detail"].replace('..', '')
    row["summary"] = row["summary"].replace('..', '')
    #dot
    row["title"] = row["title"].replace(' . ', '')
    row["title_detail"] = row["title_detail"].replace(' . ', '')
    row["summary"] = row["summary"].replace(' . ', '')
    #slash
    row["title"] = row["title"].replace('/', '')
    row["title_detail"] = row["title_detail"].replace('/', '')
    row["summary"] = row["summary"].replace('/', '')
    #backslash
    #row["title"] = row["title"].replace('\', '')
    #row["title_detail"] = row["title_detail"].replace('\', '')
    #row["summary"] = row["summary"].replace('\', '')
    #percent %
    row["title"] = row["title"].replace('%', '')
    row["title_detail"] = row["title_detail"].replace('%', '')
    row["summary"] = row["summary"].replace('%', '')
    #dollar $
    row["title"] = row["title"].replace('$', '')
    row["title_detail"] = row["title_detail"].replace('$', '')
    row["summary"] = row["summary"].replace('$', '')
    #comma
    row["title"] = row["title"].replace(',', '')
    row["title_detail"] = row["title_detail"].replace(',', '')
    row["summary"] = row["summary"].replace(',', '')
    #exclaimation mark !
    row["title"] = row["title"].replace('!', '')
    row["title_detail"] = row["title_detail"].replace('!', '')
    row["summary"] = row["summary"].replace('!', '')
    #open rectangle blanket
    row["title"] = row["title"].replace('[', '')
    row["title_detail"] = row["title_detail"].replace('[', '')
    row["summary"] = row["summary"].replace('[', '')
    #close rectangle blanket
    row["title"] = row["title"].replace(']', '')
    row["title_detail"] = row["title_detail"].replace(']', '')
    row["summary"] = row["summary"].replace(']', '')
    #open blanket
    row["title"] = row["title"].replace('(', '')
    row["title_detail"] = row["title_detail"].replace('(', '')
    row["summary"] = row["summary"].replace('(', '')
    #close blanket
    row["title"] = row["title"].replace(')', '')
    row["title_detail"] = row["title_detail"].replace(')', '')
    row["summary"] = row["summary"].replace(')', '')
    #open single quote
    row["title"] = row["title"].replace('‘', '')
    row["title_detail"] = row["title_detail"].replace('‘', '')
    row["summary"] = row["summary"].replace('‘', '')
    #close single quote
    row["title"] = row["title"].replace('’', '')
    row["title_detail"] = row["title_detail"].replace('’', '')
    row["summary"] = row["summary"].replace('’', '')
    #open double quote
    row["title"] = row["title"].replace('“', '')
    row["title_detail"] = row["title_detail"].replace('“', '')
    row["summary"] = row["summary"].replace('“', '')
    #close double quote
    row["title"] = row["title"].replace('”', '')
    row["title_detail"] = row["title_detail"].replace('”', '')
    row["summary"] = row["summary"].replace('”', '')
    #apostrophe
    row["title"] = row["title"].replace('''''', '')
    row["title_detail"] = row["title_detail"].replace('''''', '')
    row["summary"] = row["summary"].replace('''''', '')
    #5space
    row["title"] = row["title"].replace('     ', ' ')
    row["title_detail"] = row["title_detail"].replace('     ', ' ')
    row["summary"] = row["summary"].replace('     ', ' ')
    #4space
    row["title"] = row["title"].replace('    ', ' ')
    row["title_detail"] = row["title_detail"].replace('    ', ' ')
    row["summary"] = row["summary"].replace('    ', ' ')
    #3space
    row["title"] = row["title"].replace('   ', ' ')
    row["title_detail"] = row["title_detail"].replace('   ', ' ')
    row["summary"] = row["summary"].replace('   ', ' ')
    #2space
    row["title"] = row["title"].replace('  ', ' ')
    row["title_detail"] = row["title_detail"].replace('  ', ' ')
    row["summary"] = row["summary"].replace('  ', ' ')

    ##############
    ### Then left trim and deepcut
    ##############
    row["title"] =  ' '.join(deepcut.tokenize(row["title"].lstrip()))
    row["title_detail"] =  ' '.join(deepcut.tokenize(row["title_detail"].lstrip()))
    row["summary"] =  ' '.join(deepcut.tokenize(row["summary"].lstrip()))

    #print(row["title"])

    p = {
            'source':row["source"],
            'title':row['title'],
            'published':row['published'],
            'title_detail':row['title_detail'],
            'summary':row['summary'],
            'url_link':row['url_link'],
            'retrieved':row['retrieved'],
            'sentiment':sentiment_default
        }
    try:
        collection_prep.insert_one(p)
    except:
        print ("Unexpected error while updating collection finnews_prep :", sys.exc_info()[0])
        raise

