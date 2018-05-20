#!/bin/bash
# To retrieve news in every 5 minutes by running autonewsfeed.py

printf "Job 01_news_retrieve Start: %s\n" "$(date)"

python3 /home/st118957_ait/sentifine/job/scripts/python/01_news_retrieve.py

printf "Job 01_news_retrieve Finish: %s\n" "$(date)"
printf "Job 02_news_extract Start: %s\n" "$(date)"

python3 /home/st118957_ait/sentifine/job/scripts/python/02_news_extract.py

printf "Job 02_news_extract Finish: %s\n" "$(date)"
printf "Job 03_news_transform Start: %s\n" "$(date)"

python3 /home/st118957_ait/sentifine/job/scripts/python/03_news_transform.py

printf "Job 03_news_transform Finish: %s\n" "$(date)"
printf "Job 04_news_load Start: %s\n" "$(date)"

python3 /home/st118957_ait/sentifine/job/scripts/python/04_news_load.py

printf "Job 04_news_load Finish: %s\n" "$(date)"

sleep 5m