import requests
import json
import base64
import pika
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


#From consul we're getting an dictionary of categories. Each category contains a publication
#rss feed.
r = requests.get('http://'+os.getenv("CONSUL_HOST")+':8500/v1/kv/categories', verify=False)
data  = base64.b64decode(json.loads(r.text)[0]['Value'])
categories=json.loads(data.decode('utf-8'))

feeds = {}

connection = pika.BlockingConnection(pika.ConnectionParameters(os.getenv("RABBIT_HOST")))
channel = connection.channel()
channel.queue_declare(queue='articles')

for key in categories:
    print(key)
    r = requests.get(os.getenv("RSS_URL_WITH_TRAILING_SLASH")+key,verify=False)
    print(r.text)
    try:
        feeds[key]=json.loads(r.text)
    except:
        pass

for category in categories:
    publications=feeds[category]["publications"]
    for key, pub in publications.items():
        articles=pub["items"]
        for article in articles:
            article['publication'] = key
            article['category'] = category
            channel.basic_publish(exchange='',
                      routing_key='articles',
                      body=json.dumps(article))

connection.close()
