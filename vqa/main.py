# https://github.com/sureshdsk/redis-stream-python-example/blob/main/consumergroup.py
# https://sureshdsk.dev/redis-streams-in-python-example
from walrus import Database
from typer import Typer
from enum import Enum
from io import BytesIO
from PIL import Image
import traceback
import json
import os

# for demo
import time
import random

# read timeout
BLOCK_TIME = 5000
STREAM_KEY = "VQA:jobs"
GROUP_ID = "VQA:workers"
CONSUMER_ID = os.environ.get("HOSTNAME", "VQA:worker:1")

app = Typer()

class StartFrom(str, Enum):
    beginning = "0"
    latest = "$"

@app.command()
def start(start_from: StartFrom = StartFrom.latest):
    rdb = Database(host="ict3102-redis-1")
    consumer_group = rdb.consumer_group(GROUP_ID, [STREAM_KEY], consumer=CONSUMER_ID)
    consumer_group.create()
    if start_from == StartFrom.beginning:
        consumer_group.set_id(start_from)

    while True:
        print("Reading stream...")
        streams = consumer_group.read(1, block=BLOCK_TIME)

        if len(streams) == 0:
            # when there are no immediate jobs to do
            # pick up jobs that were dropped for more than one minute
            abandoned_jobs = consumer_group.vqa_jobs.autoclaim(
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
                    question = job[b"question"]
                    print(f"Processing image {im_hash}")

                    if rdb.hash_exists(im_hash):
                        job_entry = rdb.Hash(im_hash)
                        # https://stackoverflow.com/questions/15225053/how-to-store-an-image-into-redis-using-python-pil
                        img_buffer = BytesIO(job_entry[b"img"])
                        img = Image.open(img_buffer)
                        # simulate processing
                        time.sleep(random.randint(1, 3))
                        print(f"finished processing {job_id}")

                        # prevent race condition between load, append and store
                        with rdb.lock(im_hash, ttl=5000):
                            # update job entry with final question and answer
                            QnA_list: list = json.loads(job_entry.get("qnas", "[]"))
                            QnA_list.append({
                                "question": str(question),
                                "answer": "haha ikr"
                            })
                            job_entry.update(
                                qnas=json.dumps(QnA_list)
                            )

                        consumer_group.vqa_jobs.ack(job_id)
                    else:
                        raise ValueError("Missing job entry!")
                    
                    
                    # if float(job[b"temp"]) > 0.7:
                        # these jobs will in pending state.
                        # https://redis.io/commands/XPENDING
                        # other consumers in the same group can claim with XCLAIM
                        # raise ValueError("High temperature")
                    print(f"{consumer_group.vqa_jobs.key} {consumer_group.vqa_jobs.group}")
                except:
                    print(f"Error occured in processing {job_id}")
                    traceback.print_exc()
                    

if __name__ == "__main__":
    app()
