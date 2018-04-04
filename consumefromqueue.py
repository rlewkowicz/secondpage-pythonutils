from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.common.exceptions import NoSuchElementException
from newspaper import fulltext
import facebook
import base64
import warnings
import requests
import json
import time
from bs4 import BeautifulSoup
import pika
import urllib.parse
# from __future__ import absolute_import
# from __future__ import division, print_function, unicode_literals
#
# from sumy.parsers.html import HtmlParser
# from sumy.parsers.plaintext import PlaintextParser
# from sumy.nlp.tokenizers import Tokenizer
# from sumy.summarizers.lsa import LsaSummarizer as Summarizer
# from sumy.nlp.stemmers import Stemmer
# from sumy.utils import get_stop_words


def get(url):
    r  = requests.get("http://localhost:3000/noproxy/"+urllib.parse.quote_plus(url))
    print(fulltext(r.text))

# r = requests.get('http://127.0.0.1:8500/v1/kv/fbkey')
# appkey  = base64.b64decode(json.loads(r.text)[0]['Value']).decode('utf-8')
# warnings.filterwarnings('ignore', category=DeprecationWarning)

while True:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    method_frame, header_frame, body = channel.basic_get('articles')
    if method_frame:
        get(json.loads(body.decode('utf-8'))["link"])
        # FACEBOOK_APP_ID     = '137314640440209'
        # FACEBOOK_APP_SECRET = '616d9c8b61a2750f69daec6b3c03d492'
        # FACEBOOK_PROFILE_ID = '100025108300957'
        # graph = facebook.GraphAPI(appkey)
        #         # wsj="http://wsj.com/articles/vermont-legislature-passes-gun-control-bill-1522439236?mod=nwsrl_politics_and_policy&cx_refModule=nwsrl"
        #
        # wsj=json.loads(body.decode('utf-8'))["link"]
        # Try to post something on the wall.
        # try:
        #     fb_response = graph.put_object(parent_object="199590953972923", connection_name='feed',
        #                      message=wsj)
        #     post_id=fb_response['id'].split("_")
        #     url='https://www.facebook.com/permalink.php?story_fbid='+post_id[1]+'&id='+post_id[0]
        #
        # except facebook.GraphAPIError as e:
        #     print('Something went wrong:', e.type, e.message)
        channel.basic_ack(method_frame.delivery_tag)
    else:
        print('No message returned')






# channel.start_consuming()
