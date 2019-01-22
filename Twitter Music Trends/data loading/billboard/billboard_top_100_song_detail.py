import billboard
import json
import spotipy
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import spotipy
import spotipy.util as util
from spotipy.oauth2 import SpotifyClientCredentials
import datetime
import psycopg2
import warnings
warnings.filterwarnings("ignore")
import itertools
import re


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor()


cur.execute("select distinct song_name  from  billboard_top_100_song")
df = cur.fetchall()


df2 = []
for i in df:
    df2.append([i[0]])

df3 = pd.DataFrame(df2)
df3.columns = ['song_name']


username = 'yrzhou'
scope =  'user-library-read'
client_id = '8890b85e0aaa472ab0a12ef15849ecff'
client_secret = '44f08fc8b0a74382b607ca1364b1d984'
redirect_uri = 'http://localhost:8888'

token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
sp = spotipy.Spotify(auth=token)

details = []
song_list = []
for i in df3['song_name']:
    track=(sp.search(q=i,type = 'track'))
    song_id = track['tracks']['items'][0]['id']
    song_name = track['tracks']['items'][0]['name']
    song_list.append([song_id,song_name])

song_list=pd.DataFrame(song_list)
song_list.columns=['song_id','song_name']

for inx, song in enumerate(song_list['song_id']):
    track = sp.track(str(song))
    features = sp.audio_features(str(song))
    song_name = song_list['song_name'].loc[inx]
    for i in track['artists']:
        token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
        sp = spotipy.Spotify(auth=token)
        danceability=0 if features == [None] else features[0]['danceability']
        energy=0 if features == [None] else features[0]['energy']
        key = 0 if features ==[None] else features[0]['key']
        loudness= 0 if features == [None] else features[0]['loudness']
        mode = 0 if features == [None] else features[0]['mode']              
        speechiness= 0 if features == [None] else features[0]['speechiness']
        acousticness= 0 if features == [None] else features[0]['acousticness']
        instrumentalness = 0 if  features == [None] else features[0]['instrumentalness']
        liveness = 0 if  features == [None] else features[0]['liveness']
        valence = 0 if  features == [None] else features[0]['valence']
        tempo = 0 if features == [None] else features[0]['tempo']
        type2 = 0 if features == [None] else features[0]['type']
        duration_ms = 0 if  features == [None] else features[0]['duration_ms']
        details.append([song,song_name,i['name'],i['id'],[re.search('album\:(.+)', track['album']['uri']).group(1)],
                        danceability,
                        energy,
                        key,
                        loudness,
                        mode,
                        speechiness,
                        acousticness,
                        instrumentalness,
                        liveness,
                        valence,
                        tempo,
                        type2,
                        duration_ms,
                        track['disc_number'],track['track_number']])

datails = pd.DataFrame(details)
details3=pd.DataFrame(details)
details3.columns=['song_id','song_name','artist_name','artist_id','album_id','danceability','energy','key','loudness','mode','speechiness',
                 'acousticness','instrumentalness','liveness','valence','tempo','type','duration_ms',
                 'disc_number','track_number']


album_list=[]
for i in details3['album_id']:
    album_list.append(i[0])
details3['album_id']=album_list

for i,j in enumerate(details3['song_name']):
    k=j.encode('ascii','ignore')
    details3['song_name'].loc[i]= k

for i,j in enumerate(details3['artist_name']):
    k=j.encode('ascii','ignore')
    details3['artist_name'].loc[i]= k


details3['song_id']=details3['song_id'].str.encode('ascii', errors='ignore')
#details3['artist_name']=details3['artist_name'].str.encode('utf-8', errors='ignore')
details3['artist_id']=details3['artist_id'].str.encode('ascii', errors='ignore')
details3['album_id']=details3['album_id'].str.encode('ascii', errors='ignore')
details3['type']=details3['type'].str.encode('ascii', errors='ignore')
#details3['available_markets']=details2['available_markets'].str.encode('utf-8')



engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')

details3.to_sql('billboard_top_100_song_detail', engine, if_exists='append',index=False)
print('billboard 100  song detail file completed')







