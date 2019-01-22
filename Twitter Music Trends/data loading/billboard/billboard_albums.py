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

now = datetime.datetime.now().strftime("%Y-%m-%d")
date = now 
#print(date)
username = 'yrzhou'
scope =  'user-library-read'
client_id = '8890b85e0aaa472ab0a12ef15849ecff'
client_secret = '44f08fc8b0a74382b607ca1364b1d984'
redirect_uri = 'http://localhost:8888'

token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
sp = spotipy.Spotify(auth=token)

chart = billboard.ChartData('billboard-200',date = '2017-12-16') 

#print(chart)
album = []
for i in chart:
    album.append([i.artist,i.title])

album= pd.DataFrame(album)
album.columns = ['artist','album']

album_details = []
for i,j in enumerate( album['album']):
    results = sp.search(q = "album:" + j, type = "album")
    if len(results['albums']['items'])>1:
        album_id = results['albums']['items'][0]['uri']
        detail1 = sp.album(album_id)
        artist_id = detail1['artists'][0]['id']
        market = detail1['available_markets']
        genres = detail1['genres']
        popularity = detail1['popularity']
        release_date = detail1['release_date']
        artist = album['artist'].iloc[i]
        #chart_date = album['chart_date'].iloc[i]
        album_details.append([j,artist,album_id,artist_id,market,genres,popularity,release_date,date])
    else:
        album_details.append([j,artist,None,None,None,None,None,None,date])
album_details = pd.DataFrame(album_details)
album_details.columns = ['album','artist','album_id','artist_id','market','genres','popularity','release_date','chart_date']

album_details['rank']=album_details.index+1 
#print(album_details)

engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')
album_details.to_sql('billboard_top_200_album', engine, if_exists='append',index=False)
print('billboard top 200 album insert completed')

#print(album_details) 
