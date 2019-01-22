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

cur.execute("select distinct tweet_id, tweet_text,  entities_hashtags from tweets_hash where sys_time = '12/09/17' and id >= 550287  and id <=600287")
df = cur.fetchall()

df2 = []
for i in df:
    df2.append([i[0],i[1],i[2]])

df3 = pd.DataFrame(df2)
df3.columns = ['tweet_id','tweet_text','hashtag']
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
        if k.lower() in j.lower():
            tweet_id = df3['tweet_id'].iloc[i]
            hashtag = df3['hashtag'].iloc[i]
            song_name.append([i,tweet_id,j,k])
        else:
            song_name.append(['index','tweet_id','tweet_text',None])

song_name = pd.DataFrame(song_name)
song_name.columns= ['index','tweet_id','tweet_text','song_name']

artist = []
for i,j in enumerate(df3['tweet_text']):
    for k in df_artist['artist']:
        if k.lower() in j.lower():
            tweet_id = df3['tweet_id'].iloc[i]
            hashtag = df3['hashtag'].iloc[i]
            artist.append([i,tweet_id,j,k])
        else:
            artist.append(['index','tweet_id','tweet_text',None])


artist = pd.DataFrame(artist)
artist.columns = ['index','tweet_id','tweet_text','artist']

album = []
for i,j in enumerate(df3['tweet_text']):
    for k in df_album['album']:
        if k != None:
            if k.lower() in j.lower():
                tweet_id = df3['tweet_id'].iloc[i]
                hashtag = df3['hashtag'].iloc[i]
                album.append([i,tweet_id,j,k])
            else:
                album.append(['index','tweet_id','tweet_text',None])
album = pd.DataFrame(album)
album.columns = ['index','tweet_id','tweet_text','album']



channel = []
for i,j in enumerate(df3['tweet_text']):
    for k in channel2['channel']:
        if k != None:
            if k.lower() in j.lower():
                tweet_id = df3['tweet_id'].iloc[i]
                hashtag = df3['hashtag'].iloc[i]
                channel.append([i,tweet_id,j,k])
            else:
                channel.append(['index','tweet_id','tweet_text',None])
channel = pd.DataFrame(channel)
channel.columns = ['index','tweet_id','tweet_text','channel']


#print((song_name))
#print(song_name[:1])
#print(artist[:1])
#print(album[:1])
song_name.drop_duplicates(['tweet_id','tweet_text','song_name'], inplace=True)
artist.drop_duplicates(['tweet_id','tweet_text','artist'], inplace=True)
album.drop_duplicates(['tweet_id','tweet_text','album'], inplace=True)
channel.drop_duplicates(['tweet_id','tweet_text','channel'], inplace=True)
#print(len(song_name))
#print(song_name)

final = pd.merge(song_name,artist, on=['tweet_id','tweet_text'],how = 'outer')
final.drop_duplicates(['tweet_id','tweet_text'], inplace=True)

final = pd.merge(final,album, on=['tweet_id','tweet_text'],how = 'outer')
final.drop_duplicates(['tweet_id','tweet_text'], inplace=True)

final = pd.merge(final,channel, on=['tweet_id','tweet_text'],how = 'outer')
final.drop_duplicates(['tweet_id','tweet_text'], inplace=True)

final2 = final[['tweet_id','tweet_text','song_name','artist','album','channel']]

df_rest = df3[~df3.tweet_id.isin(final2.tweet_id)]
print('# of records left: ' ,len(df_rest))
print('Total # of records: ',len(df3))
print('# of records processed: ', len(final2))

#print(final2)

##########################################################################################
data_final=[]
for i,j in enumerate(df_rest['tweet_text']):
    text = str(j)
    m = re.search('NowPlaying\s(.+?)\sby\s(.+?)\son\s(.+)', text, re.IGNORECASE)
    if m:
        tweet_id = df_rest['tweet_id'].iloc[i]
        data_final.append([tweet_id, j,m.group(1),m.group(2),None,m.group(3)])

data_final = pd.DataFrame(data_final)
#print(len(data_final))

if len(data_final) >0:
    data_final.columns = ['tweet_id','tweet_text','song_name','artist','album','channel']
#print(len(data_final))
    df_all = pd.concat([final2,data_final],ignore_index=True)
else:
    df_all = final2

engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')
df_all.to_sql('tweets_hash_parse', engine, if_exists='append',index=False)




















































