#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener, Stream
from tweepy import OAuthHandler, API, Cursor
import csv
import sys
import json
import re
from emoji import UNICODE_EMOJI

#Variables that contains the user credentials to access Twitter API 
access_token = "2370447853-BEfnpcunVUNzzlgEQtpnccRHpA2dF6RB3Ip5N8i"
access_token_secret = "XNtTrpck1GOUbE70zMHyxGeJSpMg8BZq1q0Tod81vOqgA"
consumer_key = "AGKhz4GBPGq7CWUhBrukSs0fa"
consumer_secret = "40gCMJ36UcUsTtkZ1UZqSrzQEropyXOMVJe1iA1SOA3LKtjITs"

# Open/Create a file to append data
csv_file = open('twitter_data.csv', 'a', newline='')
# Use csv Writer
csv_writer = csv.writer(csv_file, delimiter=',')


def format_tweets(to_convert):
    # print(to_convert)
    converted = ''
    for char in to_convert:
        if char in UNICODE_EMOJI:
            converted += UNICODE_EMOJI[char]
        else :
            converted += char
    converted = re.sub(r'&amp;', '&', converted)
    converted = re.sub(r'https?:\/\/[^\s]+', 'URL', converted)
    converted = re.sub(r'@[\w]+', 'USER_NAME', converted)
    return converted


#This is a basic listener that just prints received tweets to stdout.
class MyStreamListener(StreamListener):

    def on_data(self, data):
        d = json.loads(data)
        # print (d)
        info = []
        try:

            date = d['created_at']
            if date == '':
                return True
        except:
            return True
        try:
            text = format_tweets(d['extended_tweet']['full_text'])
        except:
            text = format_tweets(d['text'])
        # print(text)
        user_id = d['user']['id_str']
        location = d['place']['full_name']
        info.append(date)
        info.append(text)
        info.append(user_id)
        info.append(location)
        try:
            csv_writer.writerow(info)
        except:
            return True

    def on_error(self, status_code):
        print(sys.stderr, 'Encountered error with status code:', status_code)
        return True # Don't kill the stream

    def on_timeout(self):
        print(sys.stderr, 'Timeout...')
        return True # Don't kill the stream


if __name__ == '__main__':
    # This handles Twitter authentication and the connection to Twitter Streaming API
    l = MyStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l, tweet_mode='extended')
    stream.filter(locations=[-161.75583, 19.50139, -68.01197, 64.85694])

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
