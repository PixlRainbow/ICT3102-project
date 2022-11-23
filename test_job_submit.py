# https://github.com/sureshdsk/redis-stream-python-example/blob/main/consumergroup.py
# https://sureshdsk.dev/redis-streams-in-python-example
from walrus import Database
from typer import Typer
from PIL import Image
from io import BytesIO
from hashlib import sha1
import json

app = Typer()

@app.command()
def main(in_stream_key, image_path):
    rdb = Database(host="ict3102-redis-1")
    stream = rdb.Stream(in_stream_key)

    # https://stackoverflow.com/questions/15225053/how-to-store-an-image-into-redis-using-python-pil
    img_buffer = BytesIO()
    im = Image.open(image_path)
    im.save(img_buffer, format=im.format)

    im_hash = sha1(img_buffer.getvalue()).hexdigest()

    print(f"Image {im_hash} received")

    if rdb.hash_exists(im_hash):
        print("job already previously submitted")
        job_entry = rdb.Hash(im_hash)
        QnAs = job_entry.get("qnas", "[]")
        if (QnA_list := json.loads(QnAs)) and (len(QnA_list) > 0):
            print("retrieving Questions and Answers")
            for qna in QnA_list:
                question = qna["question"]
                answer = qna["answer"]
                print(f"Q: {question}\nA: {answer}")
        else:
            print("processing not done yet, check back later")
    else:
        job_entry = rdb.Hash(im_hash)
        job_entry.update(
            img = img_buffer.getvalue()
        )
    
        msg_id = stream.add(
            {
                "job_id": im_hash
            },
            id="*"
        )
        print(f"job event {msg_id} sent")
        print("Run again later to check result")


if __name__ == "__main__":
    app()
