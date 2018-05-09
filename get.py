from __future__ import division
from newspaper import fulltext
from newspaper import Article
import base64
import requests
import json
import pika
import urllib.parse
import urllib.request

import os
import multiprocessing
import sys
import logging
import hashlib

from metadata_parser import MetadataParser
import pdb
import pprint
import warnings
from bs4 import BeautifulSoup
import queue
warnings.filterwarnings("ignore")

log = logging.getLogger()
log.setLevel('WARN')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

import chunkcass
import parsearticle
from joblib import Parallel, delayed
import shutil


def get(ip, keyspace):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='articles')

    article = SimpleStatement("""
    INSERT INTO article (object_id, chunk_count, size, chunk_size, assets, title, publication)
    VALUES (%(object_id)s, %(chunk_count)s, %(size)s, %(chunk_size)s, %(assets)s, %(title)s, %(publication)s)
    """, consistency_level=ConsistencyLevel.ONE)
    method_frame, header_frame, body = channel.basic_get('articles')
    if method_frame:
        channel.basic_ack(method_frame.delivery_tag)
        cluster = Cluster([ip])
        session = cluster.connect()
        session.set_keyspace(keyspace)
        parsed = parsearticle.parsearticle(body)
        for asset in parsed['images']:
            thing=chunkcass.chunkandinsertimage(session=session, filepath=asset['imgpath'], imgname=asset['imgname'], imgurl=asset['imgurl'])
        shutil.rmtree(parsed['images'][0]['imgpath'].rsplit('/',1)[0])
        # session.execute(article, dict(object_id=str(parsed['articleurl']), chunk_count=thing['count'], size=thing['totalsize'], chunk_size=thing['chunksize'], assets=thing['objectid'], title=parsed['imgname'], publication=parsed['publication']))
    else:
        print('No message returned')
    articlequeue.get()
    # image = session.execute("SELECT object_id, chunk_count FROM image where image_url=\'"+parsed['imgurl']+"\'")[0]
