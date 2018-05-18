from __future__ import division
import os
import multiprocessing as mp,os
import sys
import logging
import hashlib

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


def main():
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
        CREATE TABLE IF NOT EXISTS blob (
            object_id text,
            chunk_count int,
            size int,
            chunk_size int,
            checksum text,
            attributes text,
            PRIMARY KEY (object_id)
        )
        """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS blob_chunk (
            object_id text,
            chunk_id int,
            chunk_size int,
            data blob,
            PRIMARY KEY (object_id, chunk_id)
        )
        """)


    session.execute("""
        CREATE INDEX IF NOT EXISTS ON blob_chunk(chunk_id);
        """)


    blob_chunk = SimpleStatement("""
    INSERT INTO blob_chunk (object_id, chunk_id, chunk_size, data)
    VALUES (%(object_id)s, %(chunk_id)s, %(chunk_size)s, %(data)s)
    """, consistency_level=ConsistencyLevel.ONE)

    blob = SimpleStatement("""
    INSERT INTO blob (object_id, chunk_count, size, chunk_size, checksum, attributes)
    VALUES (%(object_id)s, %(chunk_count)s, %(size)s, %(chunk_size)s, %(checksum)s, %(attributes)s)
    """, consistency_level=ConsistencyLevel.ONE)

        # prepared = session.prepare("""
        #     INSERT INTO blob_chunk (object_id, chunk_id, chunk_size, data)
        #     VALUES (?, ?, ?, ?)
        #     """)

    hashId = hashlib.sha1()
    with open("face.jpg", 'rb') as source:
      block = source.read(2**16)
      while len(block) != 0:
        hashId.update(block)
        block = source.read(2**16)

    CHUNK_SIZE = 100
    FILE_NAME = "face.jpg"
    sha1 = hashlib.sha1()
    f = open("face.jpg", 'rb')
    chunk = f.read(CHUNK_SIZE)
    count = 1
    total_size = len(chunk)
    while chunk: #loop until the chunk is empty (the file is exhausted)
        log.info("inserting row %d" % count)
        sha1.update(chunk)
        session.execute(blob_chunk, dict(object_id=str(hashId), chunk_id=count, chunk_size=len(chunk), data=chunk))
        # session.execute(prepared.bind(("key%d" % i, 'b', 'b')))
        chunk = f.read(CHUNK_SIZE) #read the next chunk
        total_size += len(chunk)
        count += 1
    f.close()
    session.execute(blob, dict(object_id=str(hashId), chunk_count=count, size=total_size, chunk_size=CHUNK_SIZE, checksum=str(hashId), attributes='doot'))

    image = session.execute("SELECT object_id, chunk_count FROM blob LIMIT 1")[0]

    f = open("cass.jpg","w+")
    f.write("")
    f.close()
    f = open("cass.jpg","ab")

    for i in range(1, image.chunk_count):
        blob = session.execute("select * from blob_chunk where chunk_id="+str(i))[0]
        f.write(blob.data)

    f.close()




    # session.execute("DROP KEYSPACE " + KEYSPACE)



if __name__ == "__main__":
    main()
