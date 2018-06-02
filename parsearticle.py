from newspaper import fulltext
from newspaper import Article
import requests
import json
import urllib.parse
import urllib.request
from metadata_parser import MetadataParser
import uuid
import os
from bs4 import BeautifulSoup
from gensim.summarization import summarize
import time
import timeout_decorator
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
from bs4.element import Comment

def getimages(html, resources, images, pathuuid):
    soup = BeautifulSoup(html)
    for x in soup.findAll(True):
        try:
            if x.get('srcset') is not None:
                srcset = x.get('srcset')
                srcset = srcset.rsplit(',')
                for imgurl in srcset:
                    image = {}
                    if imgurl.startswith(" "):
                        imgurl = imgurl[1:]
                    imgurl = imgurl.rsplit(' ', 1)[0]
                    imgurlclean = imgurl.rsplit('?', 1)[0]
                    imgname = imgurlclean.rsplit('/', 1)[-1]
                    imgpath = pathuuid+'/'+imgname+str(uuid.uuid4())
                    geturl = urllib.request.urlretrieve(imgurl, imgpath)
                    image['imgurl'] = imgurl
                    image['imgname'] = imgname
                    image['imgpath'] = imgpath
                    image['content_type'] = geturl[1]['Content-Type']
                    images.append(image)
            if x.get('src') is not None:
                image = {}
                imgurl = x.get('src')
                if not imgurl.startswith("http"):
                    imgurl = 'https:'+imgurl
                imgurlclean = imgurl.rsplit('?', 1)[0]
                imgname = imgurlclean.rsplit('/', 1)[-1]
                imgpath = pathuuid+'/'+imgname+str(uuid.uuid4())
                geturl = urllib.request.urlretrieve(imgurl, imgpath)
                image['imgurl'] = imgurl
                image['imgname'] = imgname
                image['imgpath'] = imgpath
                image['content_type'] = geturl[1]['Content-Type']
                images.append(image)
            if x.get('itemid') is not None:
                image = {}
                imgurl = x.get('itemid')
                if not imgurl.startswith("http"):
                    imgurl = 'https:'+imgurl
                imgurlclean = imgurl.rsplit('?', 1)[0]
                imgname = imgurlclean.rsplit('/', 1)[-1]
                imgpath = pathuuid+'/'+imgname+str(uuid.uuid4())
                geturl = urllib.request.urlretrieve(imgurl, imgpath)
                image['imgurl'] = imgurl
                image['imgname'] = imgname
                image['imgpath'] = imgpath
                image['content_type'] = geturl[1]['Content-Type']
                images.append(image)
        except:
            pass
    for resource in resources:
        try:
            image = {}
            imgurl = resource['url']
            if imgurl.startswith("http"):
                imgurlclean = imgurl.rsplit('?', 1)[0]
                imgname = imgurlclean.rsplit('/', 1)[-1]
                imgpath = pathuuid+'/'+imgname+str(uuid.uuid4())
                geturl = urllib.request.urlretrieve(imgurl, imgpath)
                image['imgurl'] = imgurl
                image['imgname'] = imgname
                image['imgpath'] = imgpath
                image['content_type'] = geturl[1]['Content-Type']
                images.append(image)
        except:
            pass
    return images

@timeout_decorator.timeout(500)
def parsearticle(article, pathuuid):
    mainimage={}
    images = []
    req  = requests.get("http://"+os.getenv("RENDER_HOST")+":3000/render/"+urllib.parse.quote_plus(json.loads(article.decode('utf-8'))["link"]))
    print("http://"+os.getenv("RENDER_HOST")+":3000/render/"+urllib.parse.quote_plus(json.loads(article.decode('utf-8'))["link"]))
    articletext = MetadataParser(html=json.loads(req.text)['html'])
    imgurl = str(articletext.get_metadata('image'))
    if not imgurl.startswith("http"):
        imgurl = 'http:'+imgurl
    imgurlnopost = imgurl.rsplit('?', 1)[0]
    imgname = imgurlnopost.rsplit('/', 1)[-1]
    imgpath = pathuuid+'/'+imgname+str(uuid.uuid4())
    publication = json.loads(article.decode('utf-8'))["publication"]
    category = json.loads(article.decode('utf-8'))["category"]
    title = json.loads(article.decode('utf-8'))["title"]
    articleurl = json.loads(article.decode('utf-8'))["link"]
    geturl = None
    os.mkdir(pathuuid)
    count = 0
    try:
        geturl = urllib.request.urlretrieve(imgurl, imgpath)
    except:
        pass
    while not geturl:
        req  = requests.get("http://"+os.getenv("RENDER_HOST")+":3000/render/"+urllib.parse.quote_plus(json.loads(article.decode('utf-8'))["link"]))
        articletext = MetadataParser(html=json.loads(req.text)['html'])
        imgurl = str(articletext.get_metadata('image'))
        imgurlnopost = imgurl.rsplit('?', 1)[0]
        imgname = imgurlnopost.rsplit('/', 1)[-1]
        try:
            geturl = urllib.request.urlretrieve(imgurl, imgpath)
            count += 1
        except:
            if count > 10:
                raise ValueError('Article failed too many times')
            pass
    mainimage['imgurl'] = imgurl
    mainimage['imgname'] = imgname
    mainimage['imgpath'] = imgpath
    mainimage['content_type'] = geturl[1]['Content-Type']
    images.append(mainimage)
    images1 = getimages(json.loads(req.text)['html'], json.loads(req.text)['tree']['frameTree']['resources'], images, pathuuid)
    try:
        articletext = fulltext(json.loads(req.text)['html'], language='en')
    except:
        articletext = ""
    thing = {}
    thing['title'] = json.loads(article.decode('utf-8'))["title"]
    thing['articletext'] = articletext
    thing['summary'] = summarize(articletext)
    thing['assets'] = images1
    thing['publication'] = publication
    thing['category'] = category
    thing['articleurl'] = articleurl
    thing['html'] = json.loads(req.text)['html']

    return thing
