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
from sqlalchemy import create_engine
import sys
from backports import csv
import io
import psycopg2
import warnings
warnings.filterwarnings("ignore")
import datetime

now = datetime.datetime.now().strftime("%Y-%m-%d")
date = now

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")
cur = conn.cursor()



username = 'yrzhou'
scope =  'user-library-read'
client_id = '8890b85e0aaa472ab0a12ef15849ecff'
client_secret = '44f08fc8b0a74382b607ca1364b1d984'
redirect_uri = 'http://localhost:8888'

token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
sp = spotipy.Spotify(auth=token)

new_release_data=[]

response = sp.new_releases()
while response :
    next_album = response['albums']
    albums = response['albums']['items'][0]
    #album_name  = response['albums']['items'][0]['name']
    album_id =  response['albums']['items'][0]['id']
    song_name = albums['name']
    artist = albums['artists'][0]['name']
    artist_id = response['albums']['items'][0]['artists'][0]['id']
   # market = response['albums']['items'][0]['available_markets']
    new_release_data.append([song_name,artist,artist_id,album_id])
    if next_album['next']:
        response = sp.next(next_album)
    else:
        response = None


new_release_data2 = pd.DataFrame(new_release_data)
new_release_data2.columns = ['song_name','artist','artist_id','album_id']

new_release_data3=new_release_data2

new_release_data3['song_name']= new_release_data3['song_name'].str.encode('ascii','ignore')
new_release_data3['artist']= new_release_data3['artist'].str.encode('ascii','ignore')
new_release_data3['artist_id']= new_release_data3['artist_id'].str.encode('ascii','ignore')
new_release_data3['album_id'] = new_release_data3['album_id'].str.encode('ascii','ignore')
new_release_data3['insert_date']=date


engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')

#new_release_data3.to_sql('new_release', engine, if_exists='append',index=False)

print('new release file completed')


details = []
song_list = []


for i in new_release_data2['song_name']:
    track=(sp.search(q=i,type = 'track'))
    song_id = 0 if len(track['tracks']['items'])== 0 else track['tracks']['items'][0]['id']
    song_name = 0 if len(track['tracks']['items'])== 0 else track['tracks']['items'][0]['name']
    song_list.append([song_id,song_name])

song_list=pd.DataFrame(song_list)
song_list.columns=['song_id','song_name']
song_list = song_list[song_list.song_id != 0]
song_list = song_list[song_list.song_name !=0]
#print(song_list)


print(song_list)

for inx, song in enumerate(song_list['song_id']):
    track = sp.track(str(song))
    features = sp.audio_features(str(song))
    song_name = song_list['song_name'].iloc[inx]

