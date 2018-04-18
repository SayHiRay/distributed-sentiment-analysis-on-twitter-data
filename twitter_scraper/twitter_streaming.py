#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener, Stream
from tweepy import OAuthHandler, API, Cursor
from pandas.io.json import json_normalize
import unicodecsv as csv
import sys
import json
import re
from emoji import UNICODE_EMOJI
from urllib3.exceptions import ProtocolError
from http.client import IncompleteRead


#Variables that contains the user credentials to access Twitter API 
access_token = "2370447853-BEfnpcunVUNzzlgEQtpnccRHpA2dF6RB3Ip5N8i"
access_token_secret = "XNtTrpck1GOUbE70zMHyxGeJSpMg8BZq1q0Tod81vOqgA"
consumer_key = "AGKhz4GBPGq7CWUhBrukSs0fa"
consumer_secret = "40gCMJ36UcUsTtkZ1UZqSrzQEropyXOMVJe1iA1SOA3LKtjITs"
print("Hello")
# Open/Create a file to append data
csv_file = open('twitter_data_final.csv', 'ab')
# test_file = open('twitter_data_test.txt', 'a', newline='\n')
# Use csv Writer
print("Test1")
csv_writer = csv.writer(csv_file, encoding='utf-8')
print("Test2")

def format_tweets(to_convert):
    # Possible issues:
    # 1. line breaks also exist in tweets, and break up a twitter message into several rows in the csv file.
    # 2. the delimiter is comma, however comma is very common in tweets.
    #    It is difficult to delimit a row from csv file.
    # 3. quotation marks for some tweets
    converted = ''
    for char in to_convert:
        if char in UNICODE_EMOJI:
            converted += UNICODE_EMOJI[char]
        else :
            converted += char
    converted = re.sub(r'&amp;', '&', converted)
    converted = re.sub(r'https?:\/\/[^\s]+', 'URL', converted)
    converted = re.sub(r'@[\w]+', 'USER_NAME', converted)
    converted = re.sub(r'â€™', '\'', converted)
    return converted


#This is a basic listener that just prints received tweets to stdout.
class MyStreamListener(StreamListener):
    def __init__(self, api=None):
        super().__init__(api)
        self.global_counter = 0

    def on_data(self, data):
        d = json.loads(data)
        try:
            created_at = d["created_at"]
            id_str = d["id_str"]
            try:
                text = format_tweets(d["extended_tweet"]["full_text"])
            except:
                text = format_tweets(d["text"])
                #text = format_tweets(d["text"])
            place_fullname = d["place"]["full_name"]
        except:
            return
        try:
            user_id = d["user"]["id"]
            user_name = d["user"]["name"]
            user_location = d["user"]["location"]
            user_url = d["user"]["url"]
            user_description = d["user"]["description"]
        except:
            user_id = None
            user_name = None
            user_location = None
            user_url = None
            user_description = None
        try:
            place_id = d["place"]["id"]
        except:
            place_id = None
        try:
            place_url = d["place"]["url"]
        except:
            place_url = None
        try:
            place_type = d["place"]["place_type"]
        except:
            place_type = None
        try:
            place_countrycode = d["place"]["country_code"]
        except:
            place_countrycode = None
        try:
            place_country = d["place"]["country"]
        except:
            place_country = None
        try:
            place_boundingboxtype = d["place"]["bounding_box"]["type"]
        except:
            place_boundingboxtype = None
        try:
            entities_hashtags = d["entities"]["hashtags"]
        except:
            entities_hashtags = None
        try:
            entities_urls = d["entities"]["urls"]
        except:
            entities_urls = None
        try:
            entities_mentions = d["entities"]["user_mentions"]
        except:
            entities_mentions = None
        try:
            entities_symbols = d["entities"]["symbols"]
        except:
            entities_symbols = None
        try:
            entities_media = d["entities"]["media"]
        except:
            entities_media = None
        try:
            entities_polls = d["entities"]["polls"]
        except:
            entities_polls = None

        data_line = []
        data_line.extend((str(created_at),str(id_str),str(text),str(place_fullname),str(user_id),str(user_name),str(user_location),str(user_url),str(user_description),\
                   str(place_id),str(place_url),str(place_type),str(place_countrycode),str(place_country),str(place_boundingboxtype),str(entities_hashtags),\
                   str(entities_urls),str(entities_mentions),str(entities_symbols),str(entities_media),str(entities_polls)))
        csv_writer.writerow(data_line)
        self.global_counter += 1

        if self.global_counter % 1000 == 0:
            print("{} tweets collected".format(self.global_counter))
        if self.global_counter % 2000000 == 0:
            exit(0)
        # print (d)
        # info = []
        # try:
        #     date = d['created_at']
        #     if date == '':
        #         return True
        # except:
        #     return True
        # try:
        #     text = format_tweets(d['extended_tweet']['full_text'])
        # except:
        #     text = format_tweets(d['text'])
        # # print(text)
        # user_id = d['user']['id_str']
        # if d['place'] is None:
        #     if d['coordinates'] is not None:
        #         if d['coordinates']['coordinates'] is not None:
        #             print("BOOYAH BITCHES")
        #             return;
        # if d['coordinates'] is None:
        #     if d['place'] is not None:
        #         print("DAMN SON")
        #         return;
        #
        # print("made it")
        # location = d['place']['full_name']
        # info.append(date)
        # info.append(text)
        # info.append(user_id)
        # info.append(location)
        # try:
        #     csv_writer.writerow(info)
        # except:
        #     return True

    def on_error(self, status_code):
        print(sys.stderr, 'Encountered error with status code:', status_code)
        return True # Don't kill the stream

    def on_timeout(self):
        print(sys.stderr, 'Timeout...')
        return True # Don't kill the stream


if __name__ == '__main__':
    while True:
        try:
            l = MyStreamListener()
            auth = OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            stream = Stream(auth, l, tweet_mode='extended')
            stream.filter(locations=[-161.75583, 19.50139, -68.01197, 64.85694])
        except ProtocolError:
            print("Connection error happened. Restarting.")
            continue
        except IncompleteRead:
            print("Connection error happened. Restarting.")
            continue
        break

#
# if __name__ == '__main__':
#     auth = OAuthHandler(consumer_key, consumer_secret)  # Handling Twitter authentication
#     auth.set_access_token(access_token, access_token_secret)
#     api = API(auth)
#
#     # Open/Create a file to append data
#     csvFile = open('twitter_data.csv', 'a')
#     # Use csv Writer
#     csvWriter = csv.writer(csvFile)
#
#     count = 0
#     for tweet in Cursor(api.search,
#                         q='geocode:"39.8,-95.583068847656,2500km" include:retweets',
#                         lang='en',
#                         result_type='recent',
#                         include_entities=True,
#                         monitor_rate_limit=False,
#                         wait_on_rate_limit=False).items(3000):
#         # print(dir(tweet))
#         # print(tweet.geo)
#         # print(tweet.place)
#         # print(tweet.created_at)
#         # print(tweet.coordinates)
#         # print([tweet.created_at, tweet.text.encode('utf-8'), tweet.user.id, tweet.geo])
#         # print([tweet.created_at, tweet.geo, tweet.text.encode('utf-8')])
#         count += 1
#         if count % 500 == 0:
#             print(count)
#             csvWriter.writerow([tweet.created_at, tweet.text.encode('utf-8'), tweet.user.id, tweet.geo, tweet.place, tweet.coordinates])
