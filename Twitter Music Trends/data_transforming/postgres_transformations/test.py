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



conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor()

cur.execute("select distinct tweet_id, tweet_text from tweets_new where sys_time = '12/09/17' limit 90000")
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

#cur.execute("select distinct song_name from billboard_top_100_song  where chart_date :: date >= (select max(chart_date) from billboard_top_100_song):: date ")
cur.execute("select distinct song_name from billboard_top_100_song   ")
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

#cur.execute("select distinct name from  billboard_top_100_artist where chart_date :: date >= (select max(chart_date) from billboard_top_100_artist):: date")
cur.execute("select distinct name from  billboard_top_100_artist")
artist = cur.fetchall()

artist2= []
for i in artist:

    artist2.append(i[0])
df_artist = pd.DataFrame(artist2)
df_artist.columns = ['artist']


#cur.execute("select distinct album  from billboard_top_200_album  where chart_date :: date >= (select max(chart_date) from billboard_top_200_album):: date")
cur.execute("select distinct album  from billboard_top_200_album  ")

album = cur.fetchall()

album2 = []
for i in album:
    album2.append(i[0])
df_album = pd.DataFrame(album2)
df_album.columns = ['album']



song_name = []
for i,j in enumerate(df3['tweet_text']):
    for k in df_song_list2['song_name']:
        #song = re.escape(k) 
        #print(song)
        if k != 'Yours' and k!= 'Congratulations' and k!= 'When We':
            m=  re.search(re.escape(k),j,re.IGNORECASE)
            if m:
                song_name.append([j,k,m.group(0)])
        elif k == 'Yours':
            m = re.search(r'\bYours\b',j,re.IGNORECASE)
            if m:
                #print(m.group(0),len(m.group(0)),k, len(k))
                if len(m.group(0)) == len(k):
                    n = re.search(r'\bRussell Dickerson\b',j,re.IGNORECASE)
                    if n:
                        song_name.append([j,k,m.group(0)])
        elif k =='Congratulations':
            m = re.search(r'\bCongratulations\b',j,re.IGNORECASE)
            if m:
                #print(m.group(0),len(m.group(0)),k, len(k))
                if len(m.group(0)) == len(k):
                    n = re.search(r'\bPost Malone\b',j,re.IGNORECASE)
                    if n:
                   
                        song_name.append([j,k,m.group(0)])
        elif k =='When We':
            m = re.search(r'\bWhen We\b',j,re.IGNORECASE)
            if m:
                #print(m.group(0),len(m.group(0)),k, len(k))
                if len(m.group(0)) == len(k):
                    n = re.search(r'\bTank\b',j,re.IGNORECASE)
                    if n:

                        song_name.append([j,k,m.group(0)])

song_name = pd.DataFrame(song_name)
song_name.columns= ['text','song_name','extract']

engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')
song_name.to_sql('tmp_test', engine, if_exists='append',index=False)
