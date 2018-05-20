import multiprocessing
import initcass
import get

def get_init(q):
    get.articlequeue = q

initcass.initarticle(os.getenv("CASSANDRA_HOST"), os.getenv("CASSANDRA_KEYSPACE"))
articlequeue=multiprocessing.Queue(maxsize=15)
pool = multiprocessing.Pool(initializer=get_init, initargs=[articlequeue])

def main():
    # get.get('127.0.0.1', 'yourmom')
    while True:
        if not articlequeue.full():
            articlequeue.put_nowait(1)
            pool.apply_async(get.get,  kwds={'ip':os.getenv("CASSANDRA_HOST"), 'keyspace':os.getenv("CASSANDRA_KEYSPACE")})
if __name__ == "__main__":
    main()
