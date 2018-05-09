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
import multiprocessing as mp,os
import sys
import logging
import hashlib

from metadata_parser import MetadataParser
import pdb
import pprint
import warnings
from bs4 import BeautifulSoup
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

chunkcass = chunkcass.ChunkCass("yourmom", "127.0.0.1")

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

KEYSPACE = "yourmom"

rows = session.execute("SELECT * FROM system_schema.keyspaces")
if KEYSPACE in [row[0] for row in rows]:
    log.info("dropping existing keyspace...")
    session.execute("DROP KEYSPACE " + KEYSPACE)

log.info("creating keyspace...")
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """ % KEYSPACE)

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

log.info("creating table...")
session.execute("""
    CREATE TABLE IF NOT EXISTS article (
        object_id text,
        chunk_count int,
        size int,
        title text,
        publication text,
        chunk_size int,
        checksum text,
        assets text,
        PRIMARY KEY (object_id)
    )
    """)
# session.execute("""
#     CREATE TABLE IF NOT EXISTS blob_chunk (
#         object_id text,
#         chunk_id int,
#         chunk_size int,
#         data blob,
#         PRIMARY KEY (object_id, chunk_id)
#     )
#     """)


# session.execute("""
#     CREATE INDEX IF NOT EXISTS ON blob_chunk(chunk_id);
#     """)

# blob_chunk = SimpleStatement("""
# INSERT INTO blob_chunk (object_id, chunk_id, chunk_size, data)
# VALUES (%(object_id)s, %(chunk_id)s, %(chunk_size)s, %(data)s)
# """, consistency_level=ConsistencyLevel.ONE)

article = SimpleStatement("""
INSERT INTO article (object_id, chunk_count, size, chunk_size, checksum, assets, title, publication)
VALUES (%(object_id)s, %(chunk_count)s, %(size)s, %(chunk_size)s, %(checksum)s, %(assets)s, %(title)s, %(publication)s)
""", consistency_level=ConsistencyLevel.ONE)


def get(url):
    print(url)
    r  = requests.get("http://localhost:3000/render/"+urllib.parse.quote_plus(json.loads(url.decode('utf-8'))["link"]))
    a = MetadataParser(html=r.text)
    imgurl = str(a.get_metadata('image'))
    imgname = imgurl.rsplit('/', 1)[-1]
    publication = json.loads(url.decode('utf-8'))["publication"]
    title = json.loads(url.decode('utf-8'))["title"]
    articleurl = json.loads(url.decode('utf-8'))["link"]
    print(publication, title, articleurl, imgname)
    get = None
    try:
        get = urllib.request.urlretrieve(imgurl, imgname)
    except:
        pass
    while not get:
        r  = requests.get("http://localhost:3000/render/"+urllib.parse.quote_plus(json.loads(url.decode('utf-8'))["link"]))
        a = MetadataParser(html=r.text)
        imgurl = str(a.get_metadata('image'))
        imgname = imgurl.rsplit('/', 1)[-1]
        try:
            get = urllib.request.urlretrieve(imgurl, imgname)
        except:
            pass
    print(publication, title, articleurl, imgname)
    print(imgname)


    chunkcass.initblobchunk()
    chunkcass.chunkandinsert(filepath=imgname)
    # session.execute(article, dict(object_id=str(articleurl), chunk_count=count, size=total_size, chunk_size=CHUNK_SIZE, checksum=str(hashId), assets=sha1.hexdigest(), title=title, publication=publication))



    # chunk_size =
    # hashId = hashlib.sha1()
    # with open(imgname, 'rb') as source:
    #   block = source.read(2**16)
    #   while len(block) != 0:
    #     hashId.update(block)
    #     block = source.read(2**16)
    #
    # CHUNK_SIZE = 100
    # FILE_NAME = imgname
    # sha1 = hashlib.sha1()
    # f = open(imgname, 'rb')
    # chunk = f.read(CHUNK_SIZE)
    # count = 1
    # total_size = len(chunk)
    # while chunk: #loop until the chunk is empty (the file is exhausted)
    #     # log.info("inserting row %d" % count)
    #     sha1.update(chunk)
    #     # print(len(chunk)," postion", count)
    #     session.execute(blob_chunk, dict(object_id=str(hashId), chunk_id=count, chunk_size=len(chunk), data=chunk))
    #     # session.execute(prepared.bind(("key%d" % i, 'b', 'b')))
    #     chunk = f.read(CHUNK_SIZE) #read the next chunk
    #     total_size += len(chunk)
    #     count += 1
    # f.close()
    # session.execute(article, dict(object_id=str(articleurl), chunk_count=count, size=total_size, chunk_size=CHUNK_SIZE, checksum=str(hashId), assets=sha1.hexdigest(), title=title, publication=publication))
    #
    # image = session.execute("SELECT object_id, chunk_count FROM article LIMIT 1")[0]
    #
    # f = open("cass.jpg","w+")
    # f.write("")
    # f.close()
    # f = open("cass.jpg","ab")

    # for i in range(1, image.chunk_count):
    #     data = None
    #     while not data:
    #         try:
    #             data = session.execute("select * from blob_chunk where chunk_id="+str(i)+" AND object_id='"+str(hashId)+"'")[0]
    #             f.write(data.data)
    #         except:
    #             pass
    #
    # f.close()
    #
    # print("SHA1: {0}".format(sha1.hexdigest()))
    # print("SHA1: {0}".format(hashId.hexdigest()))

    # session.execute("DROP KEYSPACE " + KEYSPACE)


while True:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='articles')
    method_frame, header_frame, body = channel.basic_get('articles')
    if method_frame:
        get(body)
        channel.basic_ack(method_frame.delivery_tag)
    else:
        print('No message returned')
