FROM python:slim

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY persistence/requirements.txt ./persist_requirements.txt
COPY persistence/main.py ./main.py
COPY persistence/util.py ./util.py

# for demo
COPY test_job_submit.py ./
COPY Cyber-woman-corn-closeup.jpg ./

RUN pip install --no-cache-dir -r requirements.txt -r persist_requirements.txt

CMD [ "python", "-u", "./main.py"]
