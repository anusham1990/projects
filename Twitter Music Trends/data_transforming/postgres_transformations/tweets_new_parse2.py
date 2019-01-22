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
import simplejson
import profanity
from profanityfilter import ProfanityFilter
pf = ProfanityFilter()
from textblob import TextBlob




conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor()

cur.execute("select distinct tweet_id, tweet_text from tweets_new_sub where id >=  400000  and id <= 449999")
df = cur.fetchall()
engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')

df2 = []
for i in df:
    df2.append([i[0],i[1]])

df3 = pd.DataFrame(df2)
df3.columns = ['tweet_id','tweet_text']
print(len(df3))

df3.apply(lambda x: x.astype(str).str.lower())

#cur.execute("select distinct song_name from billboard_top_100_song  where chart_date :: date >= (select max(chart_date) from billboard_top_100_song):: date ")
cur.execute("select distinct song_name from billboard_top_100_song   ")
df_song_list = cur.fetchall()


df_song_list2=[]
for i in df_song_list:
    df_song_list2.append(i[0])


df_song_list2 = pd.DataFrame(df_song_list2)
df_song_list2.columns = ['song_name']
df_song_list2.apply(lambda x: x.astype(str).str.lower())


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
        if k.lower() in j.lower():
            tweet_id = df3['tweet_id'].iloc[i]
            #hashtag = df3['hashtag'].iloc[i]  
            song_name.append([i,tweet_id,j,k])
        else:
            song_name.append([i,df3['tweet_id'].iloc[i],df3['tweet_text'].iloc[i],None])


song_name = pd.DataFrame(song_name)
song_name.columns= ['index','tweet_id','tweet_text','song_name']

print('song', len(song_name)) 
#print(song_name['song_name'][song_name['song_name'] != None])
artist = []
for i,j in enumerate(df3['tweet_text']):
    for k in df_artist['artist']:
        if k.lower() in j.lower():
            tweet_id = df3['tweet_id'].iloc[i]
            #hashtag = df3['hashtag'].iloc[i]
            artist.append([i,tweet_id,j,k])
        else:
            artist.append([i,df3['tweet_id'].iloc[i],df3['tweet_text'].iloc[i],None])


artist = pd.DataFrame(artist)
artist.columns = ['index','tweet_id','tweet_text','artist']

print('artist',len(artist))
#print(artist)
album = []
for i,j in enumerate(df3['tweet_text']):
    for k in df_album['album']:
        if k != None:
            if k.lower() in j.lower():
                tweet_id = df3['tweet_id'].iloc[i]
               # hashtag = df3['hashtag'].iloc[i]
                album.append([i,tweet_id,j,k])
        else:
            album.append([i,df3['tweet_id'].iloc[i],df3['tweet_text'].iloc[i],None])
album = pd.DataFrame(album)
album.columns = ['index','tweet_id','tweet_text','album']

print('album',len(album))

channel = []
for i,j in enumerate(df3['tweet_text']):
    for k in channel2['channel']:
        if k != None:
            if k.lower() in j.lower():
                tweet_id = df3['tweet_id'].iloc[i]
               # hashtag = df3['hashtag'].iloc[i]
                channel.append([i,tweet_id,j,k])
        else:
            channel.append([i,df3['tweet_id'].iloc[i],df3['tweet_text'].iloc[i],None])
channel = pd.DataFrame(channel)
channel.columns = ['index','tweet_id','tweet_text','channel']


song_name.drop_duplicates(['tweet_id','tweet_text','song_name'],inplace=True)
print('song dd',len(song_name))
artist.drop_duplicates(['tweet_id','tweet_text','artist'],inplace=True)
print('artist dd',len(artist))
album.drop_duplicates(['tweet_id','tweet_text','album'],inplace=True)
print('album dd',len(album))
channel.drop_duplicates(['tweet_id','tweet_text','channel'],inplace=True)
print('channel',len(channel))

final = pd.merge(song_name,artist, on=['tweet_id','tweet_text'],how = 'outer')
final.drop_duplicates(['tweet_id','tweet_text','song_name','artist'],inplace=True)


final = pd.merge(final,album, on=['tweet_id','tweet_text'],how = 'outer')
final.drop_duplicates(['tweet_id','tweet_text','song_name','artist','album'],inplace=True)

final = pd.merge(final,channel, on=['tweet_id','tweet_text'],how = 'outer')
final.drop_duplicates(['tweet_id','tweet_text','song_name','artist','album','channel'],inplace=True)

############# final2 is the table for tweet_new_parse ######
final2 = final[['tweet_id','tweet_text','song_name','artist','album','channel']]
#print(final2)
#print(final2)
#final2.to_sql('tweets_new_parse_two', engine, if_exists='append',index=False)

################## get removed tweets ################
final3=[]
for i,j in enumerate(final2['tweet_text']):
    for k in df_song_list2['song_name']:
      
        m=  re.search(re.escape(k),j,re.IGNORECASE)
        #print(final2['song_name'].iloc[i])
        if m:
            tweet_text2 = re.sub(m.group(0)," ",j)
           # print(m.group(0),j,tweet_text2, final2['tweet_id'].iloc[i],final2['song_name'].iloc[i],final['artist'].iloc[i])
            tweet_id = final2['tweet_id'].iloc[i]
            final3.append([tweet_id,tweet_text2])
  

final3=pd.DataFrame(final3)
final3.columns = ['tweet_id','removed_text']
final3.drop_duplicates(['tweet_id'],inplace= True)

#print(final3)
final4 = final3[['tweet_id','removed_text']]

#print(final4)

####################### sentiment ################################################3

sentiment_removed = []
for i,j in enumerate(final4['removed_text']):
    tweet_id = final4['tweet_id'].iloc[i]
    j_str = str(j)
    sentiments = TextBlob(j_str)
    sentiment_removed.append([tweet_id,j,sentiments.sentiment.polarity])
   
sentiment_removed = pd.DataFrame(sentiment_removed)
sentiment_removed.columns = ['tweet_id','removed_tweet_text','sentiment']
sentiment_removed.to_sql('sentiment_new_removed', engine, if_exists='append',index=False)
#print(sentiment_removed)


sentiment_all = []
for i,j, in enumerate(final2['tweet_text']):
    tweet_id = final2['tweet_id'].iloc[i]
    j_str = str(j)
    sentiments = TextBlob(j_str)
    sentiment_all.append([tweet_id,j,sentiments.sentiment.polarity])

sentiment_all = pd.DataFrame(sentiment_all)
sentiment_all.columns = ['tweet_id','removed_tweet_text','sentiment']
sentiment_all.to_sql('sentiment_new_all', engine, if_exists='append',index=False)
print('sentiment finished')
############################## clean ##########################

#clean_tweet = []
#for i in final2['tweet_text']:
#    clean = pf.censor(i)
 #   clean_tweet.append(clean)

#final2['clean_tweet']= clean_tweet

engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')
#final2.to_sql('test_parse2', engine, if_exists='append',index=False)
#print(final2)

final2.to_sql('tweets_new_parse_two', engine, if_exists='append',index=False)


############# nowplaying ####################

df_rest = df3[~df3.tweet_id.isin(final2.tweet_id)]
print('# of records left: ' ,len(df_rest))
print('Total # of records: ',len(df3))
print('# of records processed: ', len(final2))







