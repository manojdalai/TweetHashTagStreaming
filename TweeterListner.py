import requests
import requests_oauthlib
import socket
import sys
import json
from tweepy import Stream
from tweepy.streaming import StreamListener
from django.utils.encoding import smart_str, smart_unicode


# Twitter Credentials
ACCESS_TOKEN = 'Provide your access token'
ACCESS_SECRET = 'Provide your access secret'
CONSUMER_KEY = 'Provide your consumer key'
CONSUMER_SECRET = 'Provide your consumer secret'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

# Construct URL to fetch Tweets
def fetch_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    respons = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, respons)
    return respons

# Send Tweets having # Tags to Spark
def push_tweets_to_spark(httpresponse, tcpconnection):
    for line in httpresponse.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcpconnection.send(smart_str(tweet_text) + "\n")
        except:
            e = sys.exc_info()
            print("Error: %s" % e)


# Initializing Listener host and port
host = "localhost"
port = 9009
address = (host, port)

# Initializing Socket
conn = None
sockt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sockt.bind((host, port))
sockt.listen(5)
print("Waiting for TCP connection...")
conn, address = sockt.accept()
print("Connected... Starting getting tweets.")
response = fetch_tweets()
push_tweets_to_spark(response, conn)
