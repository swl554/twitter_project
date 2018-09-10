# load packages
import csv
import socket
import re
import pandas as pd
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
pd.options.mode.chained_assignment = None

# read in the cleaned dataset
df = pd.read_csv('data/df_clean.csv', header=0)
df = df[["external_author_id", "author", "content", "publish_date"]]
df = df.iloc[873684:,]
# read in information on news sites
news_info = pd.read_csv('news/news_info.csv', header=0)

# create columns that will supplement the original data
df["link_url"] = ""
df["link_type"] = ""
df["retweet_author"] = ""
df["news_domain"] = ""
df["news_bias"] = ""

# requests retry configuration
def requests_retry_session(retries=3, backoff_factor=0.2, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

socket.getaddrinfo('localhost', 8080)

# open csv file to write to iteratively
with open('supp_df.csv', 'a', encoding="utf-8", newline='') as f:
    WRITER = csv.writer(f)

    # twitter scraper
    for i in range(len(df)):
        print(f"now on {i}th row")

        # find all links within tweet content
        try:
            tweeted_link = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', df.iloc[i, 2])
        except Exception as e:
            print("type error:" + str(e))
            continue

        if tweeted_link:
            print("Link found")

            # request response from the found link
            try:
                r = requests_retry_session().get(tweeted_link[0], timeout=0.18)
            except Exception as e:
                print("type error:" + str(e))
                df.iloc[i, 5] = "timeout error"
                WRITER.writerow(df.iloc[i])
                continue

            # record the actual url for the links within tweet content. Originally they are encoded in t.co link format
            df.iloc[i, 4] = r.url

            # identify retweets from suspended accounts
            if "https://twitter.com/account/suspended" in r.url:
                df.iloc[i, 5] = "tweet from suspended account"

            # record author of retweets for retweet links
            elif "https://twitter.com/" in r.url:
                df.iloc[i, 5] = "retweet"
                soup = BeautifulSoup(r.content, 'html.parser')
                content_author = soup.find(attrs={'class':'username u-dir u-textTruncate'})
                if content_author is not None:
                    df.iloc[i, 6] = content_author.get_text()

            # record news domain and their political bias for news links
            elif any(news_domain in r.url for news_domain in news_info['news_domain'].tolist()):
                df.iloc[i, 5] = "news link"
                for j in range(len(news_info)):
                    if news_info.iloc[j, 1] in r.url:
                        df.iloc[i, 7] = news_info.iloc[j, 1]
                        df.iloc[i, 8] = news_info.iloc[j, 2]

            # record youtube links
            elif "youtube.com" in r.url:
                df.iloc[i, 5] = "youtube link"

            print(df.iloc[i])
            WRITER.writerow(df.iloc[i])
