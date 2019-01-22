import pandas as pd

import numpy as np

import psycopg2

from sqlalchemy import create_engine



conn =psycopg2.connect(database="final_project", user="w205", password="1234", host="127.0.0.1", port="5432")

cur = conn.cursor()

cur.execute("SELECT * FROM billboard_top_100_artist")

results = cur.fetchall()

engine = create_engine('postgresql://w205:1234@localhost:5432/final_project')



df = pd.DataFrame(results)



categories = pd.concat([df[[0,1]].rename(columns={0: 'artist_id', 1:'artist'}), df[3].str[1:-1].str.split(',', expand=True)], axis=1)

categories = pd.melt(categories, id_vars=['artist_id', 'artist'], value_vars=[0, 1, 2])[['artist_id', 'artist', 'value']]

categories.to_sql('billboard_top_100_artist_genre', engine, if_exists='append',index=False)



related_artists = pd.concat([df[[0,1]].rename(columns={0: 'artist_id', 1:'artist'}), df[6].str[1:-1].str.split(',', expand=True)], axis=1)

related_artists = pd.melt(related_artists, id_vars=['artist_id', 'artist'], value_vars=[0, 1, 2])[['artist_id', 'artist', 'value']]

related_artists.value = related_artists.value.str.replace('"', '')

related_artists.to_sql('billboard_top_100_related_artist', engine, if_exists='append',index=False)
