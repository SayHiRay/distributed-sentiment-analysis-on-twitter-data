#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener, Stream
from tweepy import OAuthHandler, API, Cursor
import csv
import sys


#Variables that contains the user credentials to access Twitter API 
access_token = "2370447853-BEfnpcunVUNzzlgEQtpnccRHpA2dF6RB3Ip5N8i"
access_token_secret = "XNtTrpck1GOUbE70zMHyxGeJSpMg8BZq1q0Tod81vOqgA"
consumer_key = "AGKhz4GBPGq7CWUhBrukSs0fa"
consumer_secret = "40gCMJ36UcUsTtkZ1UZqSrzQEropyXOMVJe1iA1SOA3LKtjITs"

# Open/Create a file to append data
csvFile = open('twitter_data.csv', 'a')
# Use csv Writer
csvWriter = csv.writer(csvFile)


#This is a basic listener that just prints received tweets to stdout.
class MyStreamListener(StreamListener):

    def on_data(self, data):
        print(data)
        csvWriter.writerow(data)
        return True

    def on_error(self, status_code):
        print(sys.stderr, 'Encountered error with status code:', status_code)
        return True # Don't kill the stream

    def on_timeout(self):
        print(sys.stderr, 'Timeout...')
        return True # Don't kill the stream


if __name__ == '__main__':
    # This handles Twitter authetification and the connection to Twitter Streaming API
    l = MyStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
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
