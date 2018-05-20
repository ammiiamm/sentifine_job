import sys
import time
from rq import Queue
from redis import Redis
from news_retrieve import func_news_retrieve
from news_extract import func_news_extract
from news_transform import func_news_transform
from news_load import func_news_load

# Tell RQ what Redis connection to use
redis_conn = Redis()
q = Queue('newsfeed', connection=redis_conn)  # no args implies the default queue

retrieve_job = q.enqueue(func_news_retrieve)
extract_job = q.enqueue(func_news_extract, depends_on=retrieve_job)
transform_job = q.enqueue(func_news_transform, depends_on=extract_job)
load_job = q.enqueue(func_news_load, depends_on=transform_job)

time.sleep(2)
print(retrieve_job.result)
print(extract_job.result)
print(transform_job.result)
print(load_job.result)

