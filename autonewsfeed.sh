#!/bin/bash
# To retrieve news in every 5 minutes by running autonewsfeed.py

printf "Job Start: %s\n" "$(date)"

python3 /home/st118957_ait/sentifine/job/autonewsfeed.py

printf "Job Finish: %s\n" "$(date)"
