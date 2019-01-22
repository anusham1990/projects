# -*- coding: utf-8 -*-

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

new_release_data3.to_sql('new_release', engine, if_exists='append',index=False)

print('new release file completed')

####################################################################################################

artist_details = []
for i in new_release_data2['artist_id']:
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
    image = artist['images']
    popularity = artist['popularity']
    artist_details.append([artist_id,name,follower2,genre,spotify_id,image,popularity,related_artist])

artist_details2 = pd.DataFrame(artist_details)
artist_details2.columns = ['artist_id','name','follower','genre','spotify_id','image','popularity','related_artist']

artist_details3=artist_details2

artist_details3['artist_id']= artist_details3['artist_id'].str.encode('utf-8')
artist_details3['name']=artist_details3['name'].str.encode('utf-8')
#artist_details3['follower']=artist_details3['follower'].str.encode('utf-8')
artist_details3['genre']=artist_details3['genre'].str.encode('utf-8')
artist_details3['spotify_id']=artist_details3['spotify_id'].str.encode('utf-8')
artist_details3['image']=artist_details3['image'].str.encode('utf-8')
#artist_details3['popularity']=artist_details3['popularity'].str.encode('utf-8')
artist_details3['related_artist']=artist_details3['related_artist'].str.encode('utf-8')

engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')
artist_details3.to_sql('artist_detail', engine, if_exists='append',index=False)

print('artist detail file completed')


###########################################################################################################
album_list=[]
for i in new_release_data2['artist_id']:
    albums = []
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    spotify = spotipy.Spotify(auth=token)
    results = sp.artist_albums(i, album_type='album')
    albums.extend(results['items'])
    while results['next']:
        results = sp.next(results)
        albums.extend(results['items'])
    seen = set() # to avoid dups
    albums.sort(key=lambda album:album['name'].lower())
    for album in albums:
        name = album['name']
        album_id = album['id']
        if name not in seen:
            album_list.append([ name, album_id,i])
            seen.add(name)

album_list2=pd.DataFrame(album_list)
album_list2.columns=['album_name','album_id','artist_id']

album_details=[]
for i in album_list2['album_id']:
    
    #album_id = i
    #album_id = re.search('album\:(.+)', album_id).group(1)
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    spotify = spotipy.Spotify(auth=token)
    detail1 = sp.album(i)
    artist_id = detail1['artists'][0]['id']
    album_name = detail1['name']
    market = detail1['available_markets']
    #print(artist_id)
    #artist_name = sp.artist(artist_id)
    #artist_name2 = artist_name['name']
    genres = detail1['genres']
    popularity = detail1['popularity']
    release_date = detail1['release_date']
    image = detail1['images']
    tracks = detail1['tracks']
    album_details.append([artist_id,
                          album_name,
                          i,market,genres,popularity,release_date])

album_details=pd.DataFrame(album_details)
album_details.columns=['Artist_id','Album_name','Album_id','available_markets','genres','popularity',
                   'release_date']


album_details['Artist_id']=album_details['Artist_id'].str.encode('utf-8')
album_details['Album_name']=album_details['Album_name'].str.encode('utf-8')
album_details['Album_id']=album_details['Album_id'].str.encode('utf-8')
album_details['available_markets']=album_details['available_markets'].str.encode('utf-8')
album_details['genres']=album_details['genres'].str.encode('utf-8')
#album_details['popularity']=album_details['popularity'].str.encode('utf-8')
album_details['release_date']=album_details['release_date'].str.encode('utf-8')

album_details.to_sql('album_details', engine, if_exists='append',index=False)
print('artists album detais file completed')

########################################################################################################3
tids = []
for artist in new_release_data2['artist_id']:
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    sp = spotipy.Spotify(auth=token)
    results = sp.artist_top_tracks(artist)
    
    #print(results)
    for i in (results['tracks']):
        #print(i['name'])
        tids.append([i['name'],i['id'],artist])
#print(tids)
tids = pd.DataFrame(tids)
tids.columns=['song_name','song_id','artist_id']

details = []

for song in tids['song_id']:
    track = sp.track(str(song))
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    sp = spotipy.Spotify(auth=token)
    features = sp.audio_features(str(song))
    #features[0]['danceability'] = 0 if features is None
    for i in track['artists']:
    #for j in i['Artist']:
        #print(song)
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
        details.append([song,i['name'],i['id'],[re.search('album\:(.+)', track['album']['uri']).group(1)],
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
                        track['available_markets'],track['disc_number'],track['track_number']])


datails = pd.DataFrame(details)
details2=pd.DataFrame(details)
details2.columns=['song_id','artist_name','artist_id','album_id','danceability','energy','key','loudness','mode','speechiness',
                 'acousticness','instrumentalness','liveness','valence','tempo','type','duration_ms','available_markets',
                 'disc_number','track_number']


details2['song_id']=details2['song_id'].str.encode('utf-8')
details2['artist_name']=details2['artist_name'].str.encode('utf-8')
details2['artist_id']=details2['artist_id'].str.encode('utf-8')
details2['album_id']=details2['album_id'].str.encode('utf-8')
details2['type']=details2['type'].str.encode('utf-8')
details2['available_markets']=details2['available_markets'].str.encode('utf-8')

details2.to_sql('artist_top_song_detail', engine, if_exists='append',index=False)
print('artists top songs detail file completed')


#########################################################################################################
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
print(song_list)

for inx, song in enumerate(song_list['song_id']):
    track = sp.track(str(song))
    features = sp.audio_features(str(song))
    song_name = song_list['song_name'].iloc[inx]
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


#details3['song_id'] = details3['song_id'].map(lambda x: x.encode('unicode-escape')
#details3['song_name'] = details3['song_name'].map(lambda x: x.encode('unicode-escape').decode('utf-8'))
#details3['artist_name'] = details3['artist_name'].map(lambda x: x.encode('unicode-escape').decode('utf-8'))
#details3['artist_id'] = details3['artist_id'].map(lambda x: x.encode('unicode-escape').decode('utf-8'))
#details3['album_id'] = details3['album_id'].map(lambda x: x.encode('unicode-escape').decode('utf-8'))
#details3['type'] = details3['type'].map(lambda x: x.encode('unicode-escape').decode('utf-8'))

details3['song_id']=details3['song_id'].str.encode('utf-8', errors='ignore')
#details3['artist_name']=details3['artist_name'].str.encode('utf-8', errors='ignore')
details3['artist_id']=details3['artist_id'].str.encode('utf-8', errors='ignore')
details3['album_id']=details3['album_id'].str.encode('utf-8', errors='ignore')
details3['type']=details3['type'].str.encode('utf-8', errors='ignore')
#details3['available_markets']=details2['available_markets'].str.encode('utf-8')



print(details3)

details3.to_sql('new_release_song_detail', engine, if_exists='append',index=False)
print('new released song detail file completed')










