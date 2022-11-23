# https://github.com/sureshdsk/redis-stream-python-example/blob/main/consumergroup.py
# https://sureshdsk.dev/redis-streams-in-python-example
from walrus import Database
from typer import Typer
from enum import Enum
from io import BytesIO
from PIL import Image
import traceback

# for demo
import time
import random

# read timeout
BLOCK_TIME = 5000
STREAM_KEY = "VQG:jobs"
OUT_STREAM_KEY = "VQA:jobs"
GROUP_ID = "VQG:workers"

app = Typer()

class StartFrom(str, Enum):
    beginning = "0"
    latest = "$"

@app.command()
def start(consumer_id: str, start_from: StartFrom = StartFrom.beginning):
    rdb = Database(host="ict3102-redis-1")
    consumer_group = rdb.consumer_group(GROUP_ID, [STREAM_KEY], consumer=consumer_id)
    consumer_group.create()
    if start_from == StartFrom.beginning:
        consumer_group.set_id(start_from)
    
    out_stream = rdb.Stream(OUT_STREAM_KEY)

    while True:
        print("Reading stream...")
        streams = consumer_group.read(1, block=BLOCK_TIME)

        for stream_id, jobs in streams:
            for job_id, job in jobs:
                try:
                    print(f"processing {stream_id}::{job_id}::{job}")
                    im_hash = job[b"job_id"]
                    print(f"Processing image {im_hash}")

                    if rdb.hash_exists(im_hash):
                        job_entry = rdb.Hash(im_hash)
                        # https://stackoverflow.com/questions/15225053/how-to-store-an-image-into-redis-using-python-pil
                        img_buffer = BytesIO(job_entry[b"img"])
                        img = Image.open(img_buffer)
                        # simulate processing
                        time.sleep(random.randint(1, 3))
                        print(f"finished processing {job_id}")

                        # push generated question to VQA
                        msg_id = out_stream.add(
                            {
                                "job_id": im_hash,
                                "question": "haha123?"
                            },
                            id="*"
                        )
                        print(f"job event {msg_id} forwarded to VQA")

                        consumer_group.vqg_jobs.ack(job_id)
                    else:
                        raise ValueError("Missing job entry!")
                    
                    
                    # if float(job[b"temp"]) > 0.7:
                        # these jobs will in pending state.
                        # https://redis.io/commands/XPENDING
                        # other consumers in the same group can claim with XCLAIM
                        # raise ValueError("High temperature")
                    print(f"{consumer_group.vqg_jobs.key} {consumer_group.vqg_jobs.group}")
                except:
                    print(f"Error occured in processing {job_id}")
                    traceback.print_exc()
                    

if __name__ == "__main__":
    app()
