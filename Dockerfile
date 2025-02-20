FROM python:3.11.8

WORKDIR /usr/src/app    

COPY requirements.txt ./


RUN pip3 install --no-cache-dir -r requirements.txt


COPY . .


CMD ["scrapy", "crawl", "ThanhnienVN"]