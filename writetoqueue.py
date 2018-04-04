import requests
import json
import base64
import pika

r = requests.get('http://127.0.0.1:8500/v1/kv/categories', verify=False)
data  = base64.b64decode(json.loads(r.text)[0]['Value'])
categories=json.loads(data.decode('utf-8'))

feeds = {}

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='articles')

for key in categories:
    r = requests.get('http://rssparse.test/'+key,verify=False)
    feeds[key]=json.loads(r.text)

for key in categories:
    publications=feeds[key]["publications"]
    for key, pub in publications.items():
        articles=pub["items"]
        for article in articles:
            channel.basic_publish(exchange='',
                      routing_key='articles',
                      body=json.dumps(article))

connection.close()

# feeds=json.dumps(feeds)
