#!/usr/bin/env python3
# -*- coding: utf-8 -*-

##########
# Program: 01_news_retrieve.py
# Github: @ammiiamm
# Collections: news_map, news_raw
# Log pattern: [Program name] [I=Information, S=Status, E=Error, W=Warning] [Description]
# Descriptions:
# 1. Retrieve news by RSS Scraping from the specific sources
# 2. Check News Duplicate from URL and Update MongoDB (news_map, news_raw)
##########

from langdetect import detect
from dateutil import parser
import sys
import os
import feedparser
import datetime
import pymongo
import traceback
import re

def func_news_retrieve(*args, **kwarg):
    #init console log
    print("[01_news_retrieve] S Started job at " + str(datetime.datetime.utcnow()))

    #grab the current time
    dt = datetime.datetime.utcnow()

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
        #matichon_econ = r'https://www.matichon.co.th/category/economy/feed',
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

    #data = []
    count_insert = 0
    count_duplicate = 0
    filterBOTKeyword = ['ธปท','ธนาคารแห่งประเทศไทย','ธนาคารชาติ','ธนาคารกลาง','แบงค์ชาติ',
                        'แบงก์ขาติ','Bank of Thailand','กนง','คณะกรรมการนโยบายการเงิน',
                        'ศคง','ศูนย์คุ้มครองผู้ใช้บริการทางการเงิน','สถาบันวิจัยเศรษฐกิจป๋วย อึ๊งภากรณ์',
                        'กองทุนเพื่อการฟื้นฟู','FIDF','วิรไท สันติประภพ','ไพบูลย์ กิตติศรีกังวาน',
                        'เมธี สุภาพงษ์','วชิรา อารมย์ดี','จาตุรงค์ จันทรังษ์','ฤชุกร สิริโยธิน',
                        'รณดล นุ่มนนท์','สิริธิดา พนมวัน ณ อยุธยา','ณัฐวุฒิ พงศ์สิริ','เพิ่มสุข สุทธินุ่น',
                        'วรพร ตั้งสง่าศักดิ์ศรี','นวพร มหารักขกะ','พฤทธิพงศ์ ศรีมาจันทร์','สุภาวดี ปุณศรี',
                        'จันทวรรณ สุจริตกุล','ปิติ ดิษยทัต','สักกะภพ พันธ์ยานุกูล','ดอน นาครทรรพ','สุรัช แทนบุญ',
                        'ยรรยง ไทยเจริญ','รุ่ง มัลลิกะมาส']

    # Access the 'headlines' collection in the 'news' database
    client = pymongo.MongoClient()
    collection = client.sentifine.news_map
    collection_fin = client.sentifine.news_raw

    for feed, url in feeds.items():

        rss_parsed = feedparser.parse(url)

        for art in rss_parsed['items']:
            #Filter only Thai language from title
            lang = detect(art['title'])
            #print(art)
            if lang == 'th':
                
                #Checking if each news related with BOT
                filter_bot = 'N'
                if any(k in str(art['title']) for k in filterBOTKeyword) or (any(k in str(art['title_detail']) for k in filterBOTKeyword)) or (any(k in str(art['summary']) for k in filterBOTKeyword)):
                    filter_bot = 'Y'

                published = parser.parse(art['published'])
                sentiment_default = "Retrieved"
                #remove blank space
                new_summary = art['summary'].replace('<p>&nbsp;</p>', '')
                #remove last line
                #new_smmary = art['summary'].replace(' <p>The post <a rel="nofollow"', '')

                m = {
                    '_id':art['link'],
                    'title':art['title'],
                    'published':published,
                    'url_link':art['link'],
                    'retrieved':dt
                }

                r = {
                    'source':news_source.get(feed, 'BangkokBizNews'),
                    'source_url':feed,
                    'title':art['title'],
                    'published':published,
                    'title_detail':art['title_detail']['value'],
                    'summary':new_summary,
                    'category':news_cat.get(feed),
                    'url_link':art['link'],
                    'retrieved':dt,
                    'filter_BOT':filter_bot,
                    'status':sentiment_default
                }

                #insert item by item because of the duplicate of some source's links
                try:
                    count_insert = count_insert + 1
                    collection.insert_one(m) #news_map
                    collection_fin.insert_one(r) #news_raw
                except pymongo.errors.DuplicateKeyError:
                    count_insert = count_insert - 1
                    count_duplicate = count_duplicate + 1
                    #pass #allow only this exception
                except Exception as ex:
                    print ("[01_news_retrieve] E Unexpected error while inserting collection news_map & news_raw.")
                    print (str(ex))
                    #raise
            else:
                print("[01_news_retrieve] W Non-Thai Content from: " + art['link'])


    #final log
    print("[01_news_retrieve] I Number of Duplicated Records :" + str(count_duplicate))
    print("[01_news_retrieve] I Number of New Records :" + str(count_insert))
    print("[01_news_retrieve] S Finished job at " + str(datetime.datetime.utcnow()))
