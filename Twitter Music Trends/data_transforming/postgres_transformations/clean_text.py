import json
import spotipy
import time
import pandas as pd
import numpy as np
import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials
import datetime
import re
import pprint
import io
import psycopg2
import warnings
warnings.filterwarnings("ignore")
import datetime
import tweepy
from sqlalchemy import create_engine
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
#from tweeter  import *
from time import time,ctime
import simplejson
import profanity
from profanityfilter import ProfanityFilter
pf = ProfanityFilter()
from textblob import TextBlob


conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor()

cur.execute("select distinct tweet_id, tweet_text from tweets_new where sys_time = '2017-12-07' limit 10")
df = cur.fetchall()


df2 = []
for i in df:
    df2.append([i[0],i[1]])

df3 = pd.DataFrame(df2)
df3.columns = ['tweet_id','tweet_text']
df4=[]
for i,j in enumerate(df3['tweet_text']):
    tweet_id = df3['tweet_id'].iloc[i]
    text = df3['tweet_text'].iloc[i]    
    sentiments = TextBlob(j)
    df4.append([tweet_id,text,pf.censor(j),sentiments.sentiment.polarity,sentiments.sentiment.subjectivity])

df4 = pd.DataFrame(df4)
df4.columns = ['tweet_id','tweet_text','clean_text','sentiment','tone']

#print(df4)

#####################################################################################################################


conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor()

cur.execute("select distinct tweet_id, tweet_text  from tweets_new limit 50; ")
df = cur.fetchall()

df2 = []
for i in df:
    df2.append([i[0],i[1]])

df3 = pd.DataFrame(df2)
df3.columns = ['tweet_id','tweet_text']
print(len(df3))

df3.apply(lambda x: x.astype(str).str.lower())
#df3 = df3[df3.astype(str).ne('None').all(1)]

#df3.apply(lambda x: x.astype(str).str.lower())
#df3 = df3[df3.astype(str).ne('None').all(1)]

print(len(df3))

cur.execute("select distinct song_name from billboard_top_100_song  where chart_date :: date >= (select max(chart_date) from billboard_top_100_song):: date ")

df_song_list = cur.fetchall()


df_song_list2=[]
for i in df_song_list:
    df_song_list2.append(i[0])


df_song_list2 = pd.DataFrame(df_song_list2)
df_song_list2.columns = ['song_name']

df_song_list2.apply(lambda x: x.astype(str).str.lower())

#print(len(df3))
#print(len(df_song_list2))

cur.execute("select distinct channel_name from tweet_stream_channel_filter")
channel = cur.fetchall()

channel2 = []
for i in channel:
    j = str(i[0])
    channel_name = re.search('(listening\sto|now\splaying)\s(.*)',j)
    if channel_name: 
        channel2.append(channel_name.group(2))



channel2 = pd.DataFrame(channel2)
channel2.columns = ['channel']
channel2.apply(lambda x: x.astype(str).str.lower())

cur.execute("select distinct name from  billboard_top_100_artist where chart_date :: date >= (select max(chart_date) from billboard_top_100_artist):: date")
artist = cur.fetchall()

artist2= []
for i in artist:
    
    artist2.append(i[0])
df_artist = pd.DataFrame(artist2)
df_artist.columns = ['artist']


cur.execute("select distinct album  from billboard_top_200_album  where chart_date :: date >= (select max(chart_date) from billboard_top_200_album):: date")
album = cur.fetchall()

album2 = []
for i in album:
    album2.append(i[0])
df_album = pd.DataFrame(album2)
df_album.columns = ['album']

song_name = []
for i,j in enumerate(df3['tweet_text']):
    for k in df_song_list2['song_name']:
        m=  re.search(re.escape(k),j,re.IGNORECASE)
        if m:
            tweet_id = df3['tweet_id'].iloc[i]
            #hashtag = df3['hashtag'].iloc[i]  
            #song_name.append([i,tweet_id,j,k])
            #my_regex = r"\b(?=\w)" + re.escape(k) + r"\b(?!\w)"
            tweet_text2 = re.sub(m.group(0)," ",j)
            #print(m.group(0))
            #print(tweet_id,j,k,tweet_text2)
            song_name.append([i,tweet_id,j,k,tweet_text2])
        else:
            song_name.append([i,df3['tweet_id'].iloc[i],df3['tweet_text'].iloc[i],None,df3['tweet_text'].iloc[i]])

#print('#######################################################################')

song_name = pd.DataFrame(song_name)
song_name.columns= ['index','tweet_id','tweet_text','song_name','tweet_text_removed']


#print(song_name)

artist = []
for i,j in enumerate(df3['tweet_text']):
    for k in df_artist['artist']:
        m=  re.search(re.escape(k),j,re.IGNORECASE)
        if m:
            tweet_id = df3['tweet_id'].iloc[i]
            #hashtag = df3['hashtag'].iloc[i]
            tweet_text2 = re.sub(m.group(0)," ",j)
            artist.append([i,tweet_id,j,k,tweet_text2])
        else:
            artist.append([i,df3['tweet_id'].iloc[i],df3['tweet_text'].iloc[i],None,df3['tweet_text'].iloc[i]])

artist = pd.DataFrame(artist)
artist.columns = ['index','tweet_id','tweet_text','artist','tweet_text_removed']

#print(artist)

album = []
for i,j in enumerate(df3['tweet_text']):
    for k in df_album['album']:
        if k != None:
            m=  re.search(re.escape(k),j,re.IGNORECASE)
            if m:
                tweet_id = df3['tweet_id'].iloc[i]
               # hashtag = df3['hashtag'].iloc[i]
                tweet_text2 = re.sub(m.group(0)," ",j)
                album.append([i,tweet_id,j,k,tweet_text2])
            else:
                album.append([i,df3['tweet_id'].iloc[i],df3['tweet_text'].iloc[i],None,df3['tweet_text'].iloc[i]])
album = pd.DataFrame(album)
album.columns = ['index','tweet_id','tweet_text','album','tweet_text_removed']


channel = []
for i,j in enumerate(df3['tweet_text']):
    for k in channel2['channel']:
        if k != None:
            if k.lower() in j.lower():
                tweet_id = df3['tweet_id'].iloc[i]
               # hashtag = df3['hashtag'].iloc[i]
                channel.append([i,tweet_id,j,k,j])
            else:
                channel.append([i,df3['tweet_id'].iloc[i],df3['tweet_text'].iloc[i],None,df3['tweet_text'].iloc[i]])
channel = pd.DataFrame(channel)
channel.columns = ['index','tweet_id','tweet_text','channel','tweet_text_removed']



song_name.drop_duplicates(['tweet_id','tweet_text','song_name'], inplace=True)
artist.drop_duplicates(['tweet_id','tweet_text','artist'], inplace=True)
album.drop_duplicates(['tweet_id','tweet_text','album'], inplace=True)
channel.drop_duplicates(['tweet_id','tweet_text','channel'], inplace=True)

#print(song_name)

final = pd.merge(song_name,artist, on=['tweet_id','tweet_text'],how = 'outer')
final.drop_duplicates(['tweet_id','tweet_text'], inplace=True)

#print(final)
final = pd.merge(final,album, on=['tweet_id','tweet_text'],how = 'outer')
final.drop_duplicates(['tweet_id','tweet_text'], inplace=True)

final = pd.merge(final,channel, on=['tweet_id','tweet_text'],how = 'outer')
final.drop_duplicates(['tweet_id','tweet_text'], inplace=True)

#print(final)
final2 = final[['tweet_id','tweet_text','song_name','artist','album','channel','tweet_text_removed_x']]

print(final2)








