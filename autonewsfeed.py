#Scraping headlines with crontab, Python, and MongoDB
#Tutorial from https://www.adamerispaha.com/2017/07/09/scraping-headlines-with-cronjobs-python-and-mongodb/

#!/usr/bin/env python
import sys
import os
import feedparser
import datetime
import pymongo
#from langdetect import detect
from dateutil import parser

#create a dictionary of rss feeds
feeds = dict(
    thaipr_fin = r'http://www.thaipr.net/finance/feed',
    thaipr_property = r'http://www.thaipr.net/estate/feed',
    posttoday_econ = r'https://www.posttoday.com/rss/src/economy.xml',
    posttoday_fin = r'https://www.posttoday.com/rss/src/money.xml',
    posttoday_market = r'https://www.posttoday.com/rss/src/market.xml',
    posttoday_property = r'https://www.posttoday.com/rss/src/property.xml',
    bbkbiznews_buz = r'http://www.bangkokbiznews.com/rss/feed/business.xml',
    bkkbiznews_econ = r'http://www.bangkokbiznews.com/rss/feed/economic.xml',
    bkkbiznews_fin = r'http://www.bangkokbiznews.com/rss/feed/finance.xml',
    bkkbiznews_property = r'http://www.bangkokbiznews.com/rss/feed/property.xml',
    thaipbs_econ = r'http://news.thaipbs.or.th/rss/news/economy',
    matichon_econ = r'https://www.matichon.co.th/category/economy/feed',
    manager_stock = r'http://www.manager.co.th/RSS/StockMarket/StockMarket.xml',
    manager_mutualfund = r'http://www.manager.co.th/RSS/MutualFund/MutualFund.xml',
    manager_biz = r'http://www.manager.co.th/RSS/iBizChannel/iBizChannel.xml',
)

#grab the current time
dt = datetime.datetime.utcnow()

data = []
count_insert = 0
count_duplicate = 0

# Access the 'headlines' collection in the 'news' database
client = pymongo.MongoClient()
#collection = client.sentidb.collect_news
collection = client.sentifine.finnews_raw
collection_fin = client.sentifine.finnews

for feed, url in feeds.items():

    rss_parsed = feedparser.parse(url)

    for art in rss_parsed['items']:
        #Filter only Thai language from title
        #lang = detect(art['title'])
        #print(art)
        #if lang == 'th':
            published = parser.parse(art['published'])
            sentiment_default = "N/A"
            d = {
                '_id':art['link'],
                'source':feed,
                'title':art['title'],
                'published':published,
                'title_detail':art['title_detail']['value'],
                'summary':art['summary'],
                #'summary_detail':art['summary_detail'],
                'url_link':art['link'],
                'retrieved':dt
            }

            f = {
                'source':feed,
                'title':art['title'],
                'published':published,
                'title_detail':art['title_detail']['value'],
                'summary':art['summary'],
                #'summary_detail':art['summary_detail'],
                'url_link':art['link'],
                'retrieved':dt,
                'sentiment':sentiment_default
            }

            #insert item by item because of the duplicate of some source's links
            try:
                count_insert = count_insert + 1
                collection.insert_one(d)
                collection_fin.insert_one(f)
            except pymongo.errors.DuplicateKeyError:
                count_insert = count_insert - 1
                count_duplicate = count_duplicate + 1
                #pass #allow only this exception
            except:
                print ("Unexpected error:", sys.exc_info()[0])
                raise

print("Number of Duplicated Records :" + str(count_duplicate))
print("Number of New Records :" + str(count_insert))
