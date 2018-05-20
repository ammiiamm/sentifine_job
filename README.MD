# Log pattern: [Program name] [I=Information, S=Status, E=Error, W=Warning] [Description]

## Program: 01_news_retrieve.py
# 1. Retrieve news by RSS Scraping from the specific sources
# 2. Check News Duplicate from URL and Update MongoDB (news_map, news_raw)
# (Collections: news_map, news_raw)

## Program: 02_news_extract.py
# 1. Clean text by removing unnecessary characters
# 2. Thai Text tokenization by the power of deepcut library
# (Collections: news_raw)

## Program: 03_news_transform.py
# 1. Load Thai2vec Pre-trained Word Embedding
# 2. Transform Thai Headline news to word vectors format
# (Collections: news_raw)

## Program: 04_news_load.py
# 1. Pass the word vectors through our Unsupervised ML Model
# 2. Save the sentiment output in the final Collection
# (Collections: news_raw, finnew)