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


def getimages(articletext, images, pathuuid):
    soup = BeautifulSoup(articletext)
    for x in soup.findAll(True):
        try:
            if x.get('src') is not None:
                image = {}
                imgurl = x.get('src')
                imgurlclean = imgurl.rsplit('?', 1)[0]
                imgname = imgurlclean.rsplit('/', 1)[-1]
                imgpath = pathuuid+'/'+imgname+str(uuid.uuid4())
                urllib.request.urlretrieve(imgurl, imgpath)
                image['imgurl'] = imgurl
                image['imgname'] = imgname
                image['imgpath'] = imgpath
                images.append(image)
        except:
            pass
    return images


def parsearticle(article):
    mainimage={}
    images = []
    pathuuid = str(uuid.uuid4())
    req  = requests.get("http://localhost:3000/render/"+urllib.parse.quote_plus(json.loads(article.decode('utf-8'))["link"]))
    articletext = MetadataParser(html=req.text)
    imgurl = str(articletext.get_metadata('image'))
    imgurlnopost = imgurl.rsplit('?', 1)[0]
    imgname = imgurlnopost.rsplit('/', 1)[-1]
    imgpath = pathuuid+'/'+imgname+str(uuid.uuid4())
    publication = json.loads(article.decode('utf-8'))["publication"]
    title = json.loads(article.decode('utf-8'))["title"]
    articleurl = json.loads(article.decode('utf-8'))["link"]
    geturl = None
    os.mkdir(pathuuid)
    try:
        geturl = urllib.request.urlretrieve(imgurl, imgpath)
    except:
        pass
    while not geturl:
        req  = requests.get("http://localhost:3000/render/"+urllib.parse.quote_plus(json.loads(article.decode('utf-8'))["link"]))
        articletext = MetadataParser(html=req.text)
        imgurl = str(articletext.get_metadata('image'))
        imgurlnopost = imgurl.rsplit('?', 1)[0]
        imgname = imgurlnopost.rsplit('/', 1)[-1]
        try:
            geturl = urllib.request.urlretrieve(imgurl, imgpath)
        except:
            pass
    mainimage['imgurl'] = imgurl
    mainimage['imgname'] = imgname
    mainimage['imgpath'] = imgpath
    images.append(mainimage)
    images1 = getimages(req.text, images, pathuuid)
    articletext = fulltext(req.text)
    thing = {}
    thing['articletext'] = articletext
    thing['images'] = images1
    thing['publication'] = publication
    thing['articleurl'] = articleurl
    thing['body'] = req.text

    return thing
