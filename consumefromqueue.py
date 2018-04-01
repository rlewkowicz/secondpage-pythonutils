from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.common.exceptions import NoSuchElementException
from newspaper import fulltext

import facebook
import urllib
import urllib.parse as urlparse
import subprocess
import warnings
import json


# Hide deprecation warnings. The facebook module isn't that up-to-date (facebook.GraphAPIError).
warnings.filterwarnings('ignore', category=DeprecationWarning)


# Parameters of your app and the id of the profile you want to mess with.
FACEBOOK_APP_ID     = '137314640440209'
FACEBOOK_APP_SECRET = '616d9c8b61a2750f69daec6b3c03d492'
FACEBOOK_PROFILE_ID = '100025108300957'


# Trying to get an access token. Very awkward.
oauth_args = dict(client_id     = FACEBOOK_APP_ID,
                  client_secret = FACEBOOK_APP_SECRET,
                  grant_type    = 'client_credentials')
oauth_curl_cmd = ['curl',
                  'https://graph.facebook.com/oauth/access_token?' + urlparse.urlencode(oauth_args)]
oauth_response = subprocess.Popen(oauth_curl_cmd,
                                  stdout = subprocess.PIPE,
                                  stderr = subprocess.PIPE).communicate()[0]

try:
    oauth_access_token = json.loads(oauth_response.decode('utf-8'))["access_token"]
except KeyError:
    print('Unable to grab an access token!')
    exit()

facebook_graph = facebook.GraphAPI(oauth_access_token)


# Try to post something on the wall.
try:
    fb_response = facebook_graph.put_wall_post('Hello from Python', \
                                               profile_id = FACEBOOK_PROFILE_ID)
    print(fb_response)
except facebook.GraphAPIError as e:
    print('Something went wrong:', e.type, e.message)



#https://www.facebook.com/dialog/oauth?client_id=137314640440209&redirect_uri=https://rssparse.test/&scope=publish_actions,manage_pages,publish_pages



def get(url):
    driver = webdriver.Remote(
        command_executor="http://localhost:4444/wd/hub",
        desired_capabilities={
            "browserName": "chrome",
            "javascriptEnabled": "true",
            "maxSessions": 2,
            "browserTimeout": 5,
            "timeout": 5,
            "seleniumProtocol": "WebDriver"
    })
    driver.implicitly_wait(5)
    driver.get(url)
    email = driver.find_element_by_xpath("//input[@id='email' or @name='email']")
    email.send_keys('rlewkowiczge@gmail.com')
    print("Email Id entered...")
    password = driver.find_element_by_xpath("//input[@id='pass']")
    password.send_keys('H4ck3r123!!')
    print("Password entered...")
    button = driver.find_element_by_xpath("//label[@id='loginbutton']")
    button.click()
    driver.get('https://www.facebook.com/wsj.harvester.5')
    driver.close()


# get("http://facebook.com")
# get("https://www.nytimes.com/2018/03/29/world/europe/russia-expels-diplomats.html?hp&action=click&pgtype=Homepage&clickSource=story-heading&module=first-column-region&region=top-news&WT.nav=top-news")
