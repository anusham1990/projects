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
conn.autocommit = True
cur = conn.cursor()

cur.execute("select distinct album_id, artist_id  from  billboard_top_200_album")
df = cur.fetchall()

df2 = []
for i in df:
    df2.append([i[0],i[1]])

df3 = pd.DataFrame(df2)
df3.columns = ['albun_id','artist_id']

#print(df3)

username = 'yrzhou'
scope =  'user-library-read'
client_id = '8890b85e0aaa472ab0a12ef15849ecff'
client_secret = '44f08fc8b0a74382b607ca1364b1d984'
redirect_uri = 'http://localhost:8888'

token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
sp = spotipy.Spotify(auth=token)

#print(df3)

artist_details = []
for i in df3['artist_id']:
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    spotify = spotipy.Spotify(auth=token)
    if i != None:
        artist = spotify.artist(i)
        related = sp.artist_related_artists(i)
        related_artist = []
        for j in range(len(related['artists'])):
            related_artist.append([related['artists'][j]['name']])
        name = artist['name']
        artist_id = i
        follower = artist['followers']
        follower2 = follower['total']
        genre = artist['genres']
        spotify_id = artist['id']
        image = artist['images']
        popularity = artist['popularity']
        artist_details.append([artist_id,name,follower2,genre,spotify_id,image,popularity,related_artist])
    else:
        artist_details.append([i,None,None,None,None,None,None,None])

artist_details = pd.DataFrame(artist_details)
artist_details.columns = ['artist_id','name','follower','genre','spotify_id','image','popularity','related_artist']

#print(artist_details)

engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')
artist_details.to_sql('billboard_top_200_album_artist_detail', engine, if_exists='append',index=False)

conn.commit()
