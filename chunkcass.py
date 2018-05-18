from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import hashlib

def initimage(session):

    session.execute("""
        CREATE TABLE IF NOT EXISTS image (
            object_id text,
            chunk_count int,
            size int,
            name text,
            checksum text,
            image_url text,
            metadata text,
            PRIMARY KEY (image_url)
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

def chunkandinsertimage(session, filepath, imgurl, imgname, content_type, chunksize = 2**16, threads = 2):

    blob_chunk = SimpleStatement("""
    INSERT INTO blob_chunk (object_id, chunk_id, chunk_size, data)
    VALUES (%(object_id)s, %(chunk_id)s, %(chunk_size)s, %(data)s)
    IF NOT EXISTS
    """, consistency_level=ConsistencyLevel.ONE)

    image = SimpleStatement("""
    INSERT INTO image (object_id, chunk_count, size, name, checksum, image_url, metadata, content_type)
    VALUES (%(object_id)s, %(chunk_count)s, %(size)s, %(name)s, %(checksum)s, %(image_url)s, %(metadata)s, %(content_type)s)
    IF NOT EXISTS
    """, consistency_level=ConsistencyLevel.ONE)

    hashid = hashlib.sha1()
    with open(filepath, 'rb') as source:
        chunk = source.read(chunksize)
        while len(chunk) != 0:
            hashid.update(chunk)
            chunk = source.read(chunksize)

    count = 0
    totalsize = 0
    checksum = hashlib.sha1()

    with open(filepath, 'rb') as source:

        chunk = source.read(chunksize)
        while len(chunk) != 0:
            totalsize += len(chunk)
            checksum.update(chunk)
            count += 1

            session.execute(blob_chunk, dict(object_id=str(hashid.hexdigest()), chunk_id=count, chunk_size=len(chunk), data=chunk))

            chunk = source.read(chunksize)
        if hashid.hexdigest() != checksum.hexdigest():
            raise ValueError('The final object checksum for the chunked object does not match the original hash')

    session.execute(image, dict(object_id=hashid.hexdigest(), chunk_count=count, size=totalsize, name=imgname, checksum=hashid.hexdigest(), image_url=imgurl, metadata="null", content_type=content_type))

    thing = {}
    thing['objectid'] = hashid.hexdigest()
    thing['chunksize'] = chunksize
    thing['count'] = count
    thing['totalsize'] = totalsize

    return thing
