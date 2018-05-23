SentiFine Back-end Job
=============

The purpose of this repository is to perform ETL process for SentiFine Web Application. This git consolidated all back-end scripts of SentiFine Web Application which has the overview steps as follows:

**Step 01: news_retrieve.py** (MongoDB Collections: news_map, news_raw)
1. Retrieve news by RSS Scraping from the specific sources
2. Check News Duplicate from URL and Update MongoDB (news_map, news_raw)

**Step 02: news_extract.py** (MongoDB Collection: news_raw)
1. Clean text by removing unnecessary characters
2. Thai Text tokenization by the power of deepcut library

**Step 03: news_transform.py** (MongoDB Collection: news_raw)
1. Load Thai2vec Pre-trained Word Embedding
2. Transform Thai Headline news to word vectors format

**Step 04: news_load.py** (MongoDB Collections: news_raw, finnew)
1. Pass the word vectors through our ML Model
2. Save the sentiment output in the final Collection

All scripts were written and tested on Ubuntu 16.04 server with the following requirements:

## Requirement 
1. Python3
2. MongoDB 

## Install & Run it!
1. Clone this repository:
    `git clone https://github.com/ammiiamm/sentifine_job.git`

2. Go to the main script:
    `cd sentifine_job/scripts/python`

3. Run it with command:
    `python3 autonewsfeed.py`

## Note
Log pattern: `[Script name] [I=Information, S=Status, E=Error, W=Warning] [Description]`