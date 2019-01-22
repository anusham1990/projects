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

chart = billboard.ChartData('hot-100', date='%s' % date) 

billboard=[]
for i in chart:
    billboard.append([i.title,i.artist])
#print(billboard)

billboard=pd.DataFrame(billboard)
billboard['chart_date']=date

billboard.columns=['song_name','artists','chart_date']
#print(billboard)

for i,j in enumerate(billboard['song_name']):
    k=j.encode('ascii','ignore')
    billboard['song_name'].loc[i]= k

for i,j in enumerate(billboard['artists']):
    k=j.encode('ascii','ignore')
    billboard['artists'].loc[i]= k


engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')

billboard.to_sql('billboard_top_100_song', engine, if_exists='append',index=False)
print('billboard top 100 insert completed')


###########################################################################################3

artists=[]
for i in billboard['artists']:
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    sp = spotipy.Spotify(auth=token)
    results = sp.search(q=i,type = 'artist')
    if len(results['artists']['items']) >0:
           artists.append((results['artists']['items'][0]['id']))


##### get artists details  ######
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
    artist_details.append([artist_id,name,follower2,genre,spotify_id,popularity,related_artist])

artist_details2 = pd.DataFrame(artist_details)
artist_details2.columns = ['artist_id','name','follower','genre','spotify_id','popularity','related_artist']

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

artist_details2.to_sql('billboard_100_artist_detail', engine, if_exists='append',index=False)
print('billboard 100 artist detail file completed')

#############################################################################################################3

album_list=[]
for i in artists:
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
 
   
    album_details.append([artist_id,
                          album_name,
                          i,market,genres,popularity,release_date])

album_details=pd.DataFrame(album_details)
album_details.columns=['artist_id','album_name','album_id','available_markets','genres','popularity',
                   'release_date']

album_details['album_name']=album_details['album_name'].str.encode('ascii', errors='ignore')

album_details.to_sql('billboard_100_artists_album_detail', engine, if_exists='append',index=False)
print('album detail file completed')	

##############################################################################################################

tids = []
for i in artists:
    token = util.prompt_for_user_token(username, scope, client_id, client_secret, redirect_uri)
    sp = spotipy.Spotify(auth=token)
    results = sp.artist_top_tracks(i)
    
    #print(results)
    for j in (results['tracks']):
        #print(i['name'])
        tids.append([j['name'],j['id'],i])

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


details2['song_id']=details2['song_id'].str.encode('ascii','ignore')
details2['artist_name']=details2['artist_name'].str.encode('ascii','ignore')
details2['artist_id']=details2['artist_id'].str.encode('ascii','ignore')
details2['album_id']=details2['album_id'].str.encode('ascii','ignore')
details2['type']=details2['type'].str.encode('ascii','ignore')
details2['available_markets']=details2['available_markets'].str.encode('ascii','ignore')

details2.to_sql('billboard_100_artists_top_song_detail', engine, if_exists='append',index=False)
print('billboard_100_artists_top_song_detail  file completed')


#############################################################################################################

details = []
song_list = []
for i in billboard['song_name']:
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


details3.to_sql('billboard_100_song_detail', engine, if_exists='append',index=False)
print('billboard 100  song detail file completed')






