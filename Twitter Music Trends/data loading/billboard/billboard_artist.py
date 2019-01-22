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

artist = pd.read_csv('top_100_artist.csv',header=None)
artist.columns = ['artist']

artists=[]
for i in artist['artist']:
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    sp = spotipy.Spotify(auth=token)
    results = sp.search(q=i,type = 'artist')
    if len(results['artists']['items']) >0:
           artists.append((results['artists']['items'][0]['id']))

#print(artists)
artist_details = []
for i in artists:
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    spotify = spotipy.Spotify(auth=token)

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
   
    popularity = artist['popularity']
    artist_details.append([artist_id,name,follower2,genre,spotify_id,popularity,related_artist,date])

artist_details2 = pd.DataFrame(artist_details)
artist_details2.columns = ['artist_id','name','follower','genre','spotify_id','popularity','related_artist','chart_date']

artist_details2['rank']=artist_details2.index+1

for i,j in enumerate(artist_details2['name']):
    k=j.encode('ascii','ignore')
    artist_details2['name'].loc[i]= k

#artist_details2['related_artist']=artist_details2['related_artist'].str.encode('ascii','ignore')

#artist_details2.to_sql('billboard_100_artist_detail', engine, if_exists='append',index=False)
#print('billboard 100 artist detail file completed')

for i,j in enumerate(artist_details2['related_artist']):
    new_list = list(itertools.chain(*j))
    new_list2 = [x.encode('ascii','ignore') for x in new_list]
    artist_details2['related_artist'].loc[i]= new_list2
    
#print(artist_details2)
engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')
artist_details2.to_sql('billboard_top_100_artist', engine, if_exists='append',index=False)
print('billboard 100 artist detail file completed')





