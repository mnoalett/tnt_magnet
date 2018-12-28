from requests import get
import requests
from requests.exceptions import RequestException
from time import gmtime, strftime
from contextlib import closing
from bs4 import BeautifulSoup
import urlparse
import pymongo
import time


def get_request(url):
    try:
        print("[+] Connecting to " + url)
        with closing(get(url, stream=True)) as resp:
            if is_html(resp):
                return resp.content
            else:
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None

def post_request(url,page):
    try:
        resp = requests.post(url, data=dict(
            page=page,
        ))
        if is_html(resp):
            return resp.content
        else:
            return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None

def is_html(resp):
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > -1)


def log_error(e):
    print(e)


def get_torrents(url,page):
    """
    Downloads the page with the list of torrent
    and returns a list of dictionaries, each one containing the torrent's info
    """
    torrent_list = []
    raw_html = post_request(url,page)
    html = BeautifulSoup(raw_html, 'html.parser')

    for tr in html.find_all('tr')[1:]:
        elem = tr.find_all('a')
        parsed = urlparse.urlparse(elem[2]['href'])

	try:
		category = (urlparse.parse_qs(parsed.query)['cat'])[0].encode('ascii','ignore')
	except:
		category = -1

        torrent = {
            'insert_date': strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            'magnet': elem[1]['href'].encode('ascii','ignore'),
            'category': category,
            'title': elem[3].text.encode('ascii','ignore')
        }

        torrent_list.append(torrent)

    #print(torrent_list)
    return torrent_list


def get_total_pages(url):
    """
    Returns the total number of pages to be downloaded.
    """
    raw_html = get_request(url)
    soup = BeautifulSoup(raw_html, 'html.parser')
    total_pages = soup.find("span", {"class": "total"})['a']
    return total_pages

url = 'http://tntvillage.scambioetico.org/src/releaselist.php'
client = pymongo.MongoClient("mongodb://localhost:27017/")


#create the database
db = client["tntvillage"]
col = db["torrents"]
session = db["session"]

col.create_index('magnet', unique=True, dropDups=True)
pages = get_total_pages(url)

print("[+] Database tntvillage created")
print("[+] Table torrents created")
print("[+] total pages: " + str(pages))

c = 1

#restoring previous session
if(session.find_one() == None):
    session.insert({'_id': 1, 'page': 1})
else:
    c = session.find_one({}, {"_id":0, "page": 1})
    c = int(c.get("page"))
    print("[+] Restoring previous session")


for count in range(c,int(pages)+1):
    print("[+] Fetching results from page " + str(count))
    t = get_torrents(url,count)
    for doc in t:
        try:
            col.insert_one(doc)
        except pymongo.errors.DuplicateKeyError, e:
            print("[-] Skipping dupplicate")

    session.update({'_id':1},{'$set':{'page':count}})
    time.sleep(3)

print("[+] Done")
