from newspaper import fulltext
from newspaper import Article
import base64
import requests
import json
import pika
import urllib.parse


def get(url):
    # r  = requests.get("http://localhost:3000/render/"+urllib.parse.quote_plus(url))
    article = Article("http://localhost:3000/render/"+urllib.parse.quote_plus(url))
    article.download()
    print(article.top_image)

while True:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    method_frame, header_frame, body = channel.basic_get('articles')
    if method_frame:
        get(json.loads(body.decode('utf-8'))["link"])
        channel.basic_ack(method_frame.delivery_tag)
    else:
        print('No message returned')
