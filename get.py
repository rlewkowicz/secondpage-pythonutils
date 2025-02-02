import base64
import json
import pika
import urllib.parse
import urllib.request
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import os
import multiprocessing
import sys
import logging
import hashlib
import uuid

import warnings
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
import shutil
from time import sleep



def get(ip, keyspace):
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.getenv("RABBIT_HOST"), heartbeat=5, socket_timeout=1, blocked_connection_timeout=1))
    channel = connection.channel()
    channel.queue_declare(queue='articles')
    article = SimpleStatement("""
    INSERT INTO article (url, timeuuid, category, topimage, title, publication, summary, articletext, html, assets)
    VALUES (%(url)s, now(), %(category)s, %(topimage)s, %(title)s, %(publication)s, %(summary)s, %(articletext)s, %(html)s, %(assets)s)
    """, consistency_level=ConsistencyLevel.ONE)
    article_category = SimpleStatement("""
    INSERT INTO article_category (url, timeuuid, category, topimage, title, publication, summary, articletext, html, assets)
    VALUES (%(url)s, now(), %(category)s, %(topimage)s, %(title)s, %(publication)s, %(summary)s, %(articletext)s, %(html)s, %(assets)s)
    """, consistency_level=ConsistencyLevel.ONE)
    article_publication = SimpleStatement("""
    INSERT INTO article_publication (url, timeuuid, category, topimage, title, publication, summary, articletext, html, assets)
    VALUES (%(url)s, now(), %(category)s, %(topimage)s, %(title)s, %(publication)s, %(summary)s, %(articletext)s, %(html)s, %(assets)s)
    """, consistency_level=ConsistencyLevel.ONE)
    method_frame, header_frame, body = channel.basic_get('articles')
    if method_frame:
        pathuuid = str(uuid.uuid4())
        try:
            channel.basic_ack(method_frame.delivery_tag)
            cluster = Cluster([ip])
            session = cluster.connect()
            session.set_keyspace(keyspace)
            try:
                parsed = parsearticle.parsearticle(body, pathuuid)
            except:
                shutil.rmtree(pathuuid)
                articlequeue.get()
                print('failed')
                exit(1)
            try:
                for asset in parsed['assets']:
                    thing=chunkcass.chunkandinsertimage(session=session, filepath=asset['imgpath'], imgname=asset['imgname'], imgurl=asset['imgurl'], content_type=asset['content_type'])
                shutil.rmtree(pathuuid)
                session.execute(article, dict(url=str(parsed['articleurl']), title=parsed['title'], publication=parsed['publication'], category=parsed['category'], topimage=str(parsed['assets'][0]['imgurl']), summary=parsed['summary'], articletext=parsed['articletext'], html=parsed['html'], assets=str(parsed['assets'])))
                session.execute(article_category, dict(url=str(parsed['articleurl']), title=parsed['title'], publication=parsed['publication'], category=parsed['category'], topimage=str(parsed['assets'][0]['imgurl']), summary=parsed['summary'], articletext=parsed['articletext'], html=parsed['html'], assets=str(parsed['assets'])))
                session.execute(article_publication, dict(url=str(parsed['articleurl']), title=parsed['title'], publication=parsed['publication'], category=parsed['category'], topimage=str(parsed['assets'][0]['imgurl']), summary=parsed['summary'], articletext=parsed['articletext'], html=parsed['html'], assets=str(parsed['assets'])))
                session.close()
                cluster.close()
            except:
                session.close()
                cluster.close()
                shutil.rmtree(pathuuid)
                articlequeue.get()
                print('failed')
                exit(1)
        except:
            session.close()
            cluster.close()
            shutil.rmtree(pathuuid)
            articlequeue.get()
            print('failed')
            exit(1)
    else:
        session.close()
        cluster.close()
        connection.close()
        sleep(60)
        # print('No message returned')
    try:
        articlequeue.get()
    except:
        pass
