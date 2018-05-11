import multiprocessing
import damn
import initcass
import get
    # f = open("cass.jpg","w+")
    # f.write("")
    # f.close()
    # f = open("cass.jpg","ab")
    #
    # for i in range(1, image.chunk_count+1):
    #     data = None
    #     while not data:
    #         try:
    #             data = session.execute("select * from blob_chunk where chunk_id="+str(i)+" AND object_id='"+str(thing['objectid'])+"'")[0]
    #             f.write(data.data)
    #         except:
    #             pass
    #
    # f.close()

def get_init(q):
    get.articlequeue = q

initcass.initarticle('127.0.0.1', "yourmom")
articlequeue=multiprocessing.Queue(maxsize=15)
pool = multiprocessing.Pool(initializer=get_init, initargs=[articlequeue])


def main():
    # get.get('127.0.0.1', 'yourmom')
    while True:
        if not articlequeue.full():
            articlequeue.put_nowait(1)
            pool.apply_async(get.get,  kwds={'ip':'127.0.0.1', 'keyspace':'yourmom'})
if __name__ == "__main__":
    main()
#
# while True:
#     if not articlequeue.full():
#         articlequeue.put(1)
