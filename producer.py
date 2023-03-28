import uvicorn, os, string, re
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import tweepy, configparser, json
from kafka import KafkaProducer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from types import SimpleNamespace as Namespace
from time import sleep

origins = ["*"]

app = FastAPI()
wn = WordNetLemmatizer()
listOfStopWords = stopwords.words('english')

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/tweet')
async def getTweets(query: str = '', limit: int = 10):
    if not query.startswith('#'):
        query = '#' + query
    print(query, limit)
    api = setup()
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))
    producer.send('tweet_stream', {'search_trigger': query})
    tweets = api.search_tweets(q=query, count=limit)
    for tweet in tweets:
        if tweet.lang == 'en':
            location = {}
            user = {'username' : tweet.user.screen_name}
            #print('Location: ', tweet.geo)
            if tweet.geo:
                print(tweet.geo)
                location = {}
            if tweet.user:
                user = {'id': tweet.user.id,'username': tweet.user.screen_name, 'display_name': tweet.user.name, 'description': tweet.user.description, 'followers': tweet.user.followers_count, 'friends': tweet.user.friends_count, 'verified': tweet.user.verified}
            data = {'date': tweet.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    'user': user,
                    'tweet': tweet._json['text'],
                    'clean_tweet': cleanText(tweet._json['text']),
                    'tweet_id': tweet.id,
                    'location': location,
                    'source': tweet.source,
                    'favourite_count': tweet.favorite_count,
                    'retweet_count': tweet.retweet_count}
            print('Sending..', data)
            producer.send('tweet_stream', data)
            sleep(1)
    return 'Data Sent to Pipeline by Producer'

def cleanText(text):
    if text:   
        lines = text.lower().split('\n')
        lines = [' '.join(re.split('\W+', line.strip())) for line in lines if not line.startswith('http')]
        lines = [''.join([char for char in line if char not in string.punctuation]) for line in lines]
        return ''.join(' '.join(wn.lemmatize(word) for word in line.split(' ') if word not in listOfStopWords and not word.isnumeric()) for line in lines)
    else:
        return ''

def setup():
    config = configparser.ConfigParser()
    path = os.getcwd()+'/producer/config.ini'
    config.read(path)
    api_key = config['twitter']['api_key']
    api_key_secret = config['twitter']['api_key_secret']
    access_token = config['twitter']['access_token']
    access_token_secret = config['twitter']['access_token_secret']
    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    return tweepy.API(auth)

if __name__ == '__main__':
    uvicorn.run(app)

class Tweet(BaseModel):
    date: str
    user: any
    tweet: str
    clean_tweet: str
    location: any
    tweet_id: str
    source: str
    favourite_count: int
    retweet_count: int

class User(BaseModel):
    id: str
    username: str
    display_name: str
    description: str
    verified: str
    followers: int
    friends: int

class Location(BaseModel):
    latitude: float
    longitude: float