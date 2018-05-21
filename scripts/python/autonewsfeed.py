#!/usr/bin/env python3
import sys
import time

from news_retrieve import func_news_retrieve
from news_extract import func_news_extract
from news_transform import func_news_transform
from news_load import func_news_load


# call function in the sequential
while True:
    func_news_retrieve()
    time.sleep(5)
    func_news_extract()
    time.sleep(5)
    func_news_transform()
    time.sleep(5)
    func_news_load()
    time.sleep(5)