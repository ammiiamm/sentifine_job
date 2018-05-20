#Scraping headlines with crontab, Python, and MongoDB
#Tutorial from https://www.adamerispaha.com/2017/07/09/scraping-headlines-with-cronjobs-python-and-mongodb/

#!/usr/bin/env python
import sys
import os
import feedparser
import datetime
import pymongo
from langdetect import detect
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

news_cat = dict(
        thaipr_fin = 'Finance',
        thaipr_property = 'Property',
        posttoday_econ = 'Economy',
        posttoday_fin = 'Finance',
        posttoday_market = 'Business',
        posttoday_property = 'Property',
        bbkbiznews_buz = 'Business',
        bkkbiznews_econ = 'Economy',
        bkkbiznews_fin = 'Finance',
        bkkbiznews_property = 'Property',
        thaipbs_econ = 'Economy',
        matichon_econ = 'Economy',
        manager_stock = 'Finance',
        manager_mutualfund = 'Finance',
        manager_biz = 'Business'
    )

news_source = dict(
        thaipr_fin = 'ThaiPR',
        thaipr_property = 'ThaiPR',
        posttoday_econ = 'PostToday',
        posttoday_fin = 'PostToday',
        posttoday_market = 'PostToday',
        posttoday_property = 'PostToday',
        bkkbiznews_buz = 'BangkokBizNews',
        bkkbiznews_econ = 'BangkokBizNews',
        bkkbiznews_fin = 'BangkokBizNews',
        bkkbiznews_property = 'BangkokBizNews',
        thaipbs_econ = 'ThaiPBS',
        matichon_econ = 'Matichon',
        manager_stock = 'Manager',
        manager_mutualfund = 'Manager',
        manager_biz = 'Manager'
    )

#grab the current time
dt = datetime.datetime.utcnow()

data = []
count_insert = 0
count_duplicate = 0

# Access the 'headlines' collection in the 'news' database
client = pymongo.MongoClient()
#collection = client.sentidb.collect_news
collection = client.sentifine.finnews_map
collection_fin = client.sentifine.finnews_raw

for feed, url in feeds.items():

    rss_parsed = feedparser.parse(url)

    for art in rss_parsed['items']:
        #Filter only Thai language from title
        lang = detect(art['title'])
        #print(art)
        if lang == 'th':
            
            published = parser.parse(art['published'])
            sentiment_default = "N/A"
            d = {
                '_id':art['link'],
                #'source':feed,
                'title':art['title'],
                'published':published,
                #'title_detail':art['title_detail']['value'],
                #'summary':art['summary'],
                'url_link':art['link'],
                'retrieved':dt
            }

            f = {
                'source':news_source.get(feed),
                'source_url':feed,
                'title':art['title'],
                'published':published,
                'title_detail':art['title_detail']['value'],
                'summary':art['summary'],
                #'summary_detail':art['summary_detail'],
                'category':news_cat.get(feed),
                'url_link':art['link'],
                'retrieved':dt,
                'sentiment':sentiment_default,
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
        else:
            print("Non-Thai Content")
            #print(art)

print("Number of Duplicated Records :" + str(count_duplicate))
print("Number of New Records :" + str(count_insert))
