import multiprocessing
import initcass
import get
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import os

def get_init(q):
    get.articlequeue = q
if os.getenv("ENV") == "dev":
    initcass.initarticle(os.getenv("CASSANDRA_HOST"), os.getenv("CASSANDRA_KEYSPACE"))


articlequeue=multiprocessing.Queue(maxsize=5)
pool = multiprocessing.Pool(initializer=get_init, initargs=[articlequeue])

def main():
    # get.get(os.getenv("CASSANDRA_HOST"), os.getenv("CASSANDRA_KEYSPACE"))
    while True:
        if not articlequeue.full():
            articlequeue.put_nowait(1)
            pool.apply_async(get.get,  kwds={'ip':os.getenv("CASSANDRA_HOST"), 'keyspace':os.getenv("CASSANDRA_KEYSPACE")})
if __name__ == "__main__":
    main()
