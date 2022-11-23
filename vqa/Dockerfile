FROM python:slim

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY vqa/main.py ./main.py

# for demo
COPY test_job_submit.py ./
COPY Cyber-woman-corn-closeup.jpg ./

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "-u", "./main.py", "VQA:worker:1" ]
