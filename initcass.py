from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

def initarticle(ip, keyspace):
    cluster = Cluster([ip])
    session = cluster.connect()

    rows = session.execute("SELECT * FROM system_schema.keyspaces")
    if keyspace in [row[0] for row in rows]:
        session.execute("DROP keyspace " + keyspace)

    session.execute("""
        CREATE keyspace IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % keyspace)

    session.set_keyspace(keyspace)


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
