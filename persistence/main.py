# https://github.com/sureshdsk/redis-stream-python-example/blob/main/consumergroup.py
# https://sureshdsk.dev/redis-streams-in-python-example
from walrus import Database
from typer import Typer
from enum import Enum
from io import BytesIO
from PIL import Image
import traceback
import os
from pymongo import MongoClient
from multiprocessing import Process

import util

# for demo
import time
import random

# read timeout
BLOCK_TIME = 5000
STREAM_KEY = "SQL:requests"
QUEUE_KEY = "SQL:updates"
OUT_STREAM_KEY = "VQG:jobs"
GROUP_ID = "SQL:workers"
CONSUMER_ID = os.environ.get("HOSTNAME", "SQL:worker:1")

MONGO_DB = os.environ['MONGO_INITDB_DATABASE']
MONGO_USER = os.environ['MONGO_INITDB_ROOT_USERNAME']
MONGO_PASS = os.environ['MONGO_INITDB_ROOT_PASSWORD']
MONGO_COLLECTION = os.environ['MONGO_INITDB_COLLECTION']

app = Typer()

class StartFrom(str, Enum):
    beginning = "0"
    latest = "$"

def listen_updates():
    # need to create a seperate connection as this runs on a seperate thread
    rdb = Database(host="ict3102-redis-1")
    update_queue = rdb.ZSet(QUEUE_KEY)
    conn = MongoClient(
        f'mongodb://ict3102-database-1:27017/{MONGO_DB}',
        username=MONGO_USER,
        password=MONGO_PASS
    )
    collection = conn[MONGO_DB][MONGO_COLLECTION]

    while True:
        update = update_queue.bpopmin(5)
        if update is not None:
            im_hash, timestamp = update
            qna_lock = rdb.lock(im_hash, ttl=5000)
            with qna_lock:
                job_entry = rdb.Hash(im_hash)
                # save to mongo db if timestamp is newer
                # OR if image hasn't been saved before
                util.insert_if_not_exist(
                    im_hash,
                    {
                        'qna_list': job_entry[b'qnas'],
                        'img_data': job_entry[b'img'],
                        'timestamp': timestamp
                    }
                )
                util.update_if_older(
                    im_hash, job_entry[b'qnas'],
                    timestamp, collection
                )

@app.command()
def start(start_from: StartFrom = StartFrom.latest):
    # launch db update process
    updater_proc = Process(target=listen_updates)
    updater_proc.start()
    rdb = Database(host="ict3102-redis-1")
    consumer_group = rdb.consumer_group(GROUP_ID, [STREAM_KEY], consumer=CONSUMER_ID)
    consumer_group.create()
    if start_from == StartFrom.beginning:
        consumer_group.set_id(start_from)
    
    out_stream = rdb.Stream(OUT_STREAM_KEY)

    # start mongo connection
    conn = MongoClient(
        f'mongodb://ict3102-database-1:27017/{MONGO_DB}',
        username=MONGO_USER,
        password=MONGO_PASS
    )
    collection = conn[MONGO_DB][MONGO_COLLECTION]

    while True:
        print("Reading stream...")
        streams = consumer_group.read(1, block=BLOCK_TIME)

        if len(streams) == 0:
            # when there are no immediate jobs to do
            # pick up requests that were dropped for more than one minute
            abandoned_jobs = consumer_group.main_jobs.autoclaim(
                CONSUMER_ID, 60000, count=1
            )
            if len(abandoned_jobs[1]) > 0:
                streams = [[
                    bytes(STREAM_KEY, encoding="utf-8"),
                    abandoned_jobs[1]
                ]]

        for stream_id, jobs in streams:
            for job_id, job in jobs:
                try:
                    print(f"processing {stream_id}::{job_id}::{job}")
                    im_hash = job[b"job_id"]
                    print(f"Checking for image {im_hash}")

                    # check SQL DB for existing data
                    persist_job = util.get_im(im_hash, collection)

                    # if exists, populate the redis DB entry and push pubsub event
                    job_entry = rdb.Hash(im_hash)
                    if persist_job is not None:
                        job_entry.update(
                            qnas=persist_job["qnas"]
                        )
                        # keep this database entry for only 5 minutes unless updated
                        job_entry.expire(300)
                        # rdb.publish()

                    # else, forward new job to VQG
                    else:
                        msg_id = out_stream.add(
                            {
                                "job_id": im_hash
                            },
                            id="*"
                        )
                        print(f"finished processing {job_id}")
                        print(f"job event {msg_id} forwarded to VQG")

                    consumer_group.main_jobs.ack(job_id)
                    
                    
                    # if float(job[b"temp"]) > 0.7:
                        # these jobs will in pending state.
                        # https://redis.io/commands/XPENDING
                        # other consumers in the same group can claim with XCLAIM
                        # raise ValueError("High temperature")
                    print(f"{consumer_group.main_jobs.key} {consumer_group.main_jobs.group}")
                except:
                    print(f"Error occured in processing {job_id}")
                    traceback.print_exc()
                    

if __name__ == "__main__":
    app()
