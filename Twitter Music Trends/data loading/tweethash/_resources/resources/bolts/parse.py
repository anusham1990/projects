from __future__ import absolute_import, print_function, unicode_literals

#import pandas as pd

import re

from streamparse.bolt import Bolt

import json

################################################################################

# Function to check if the string contains only ascii chars

################################################################################

def ascii_string(s):

  return all(ord(c) < 128 for c in s)



class ParseTweet(Bolt):



    def process(self, tup):
		#tweet = tweet  # extract the tweet
		#tweet = pd.DataFrame()
		tweet = json.loads(tup.values[0])
		#tweet = tup.values[0]
                created_at= tweet['created_at'] if tweet['created_at'] != None else None
                tweet_id= tweet['id']
                tweet_text= tweet['text'] if tweet['text'] != None else None
                source= tweet['source'] if tweet['source'] != None else None
                quote_count= tweet['quote_count'] if tweet['quote_count'] != None else 0
                reply_count= tweet['reply_count'] if tweet['reply_count'] != None else 0
                retweet_count= tweet['retweet_count'] if tweet['retweet_count'] != None else 0
                favorite_count= tweet['favorite_count'] if tweet['favorite_count'] != None else 0
                #possibly_sensitive= tweet['possibly_sensitive'] if tweet['possibly_sensitive'] != None else None
                lang= tweet['lang'] if tweet['lang'] != None else None
                #coming from coordinates object
		if tweet['coordinates'] != None:
                	coordinates= tweet['coordinates']
                else: 
			coordinates = None
		#coming from place object
		if tweet['place'] != None:
			place_coordinates= tweet['place']['bounding_box'] if tweet['place']['bounding_box'] != None else None
                	place_country= tweet['place']['country'] if tweet['place']['country'] != None else None
                	place_country_code= tweet['place']['country_code'] if tweet['place']['country_code'] != None else None
               	 	place_full_name= tweet['place']['full_name'] if tweet['place']['full_name'] != None else None
                	place_id= tweet['place']['id'] if tweet['place']['id'] != None else None
                	place_name= tweet['place']['name'] if tweet['place']['name'] != None else None
                	place_type= tweet['place']['place_type'] if tweet['place']['place_type'] != None else None
                else :
		
			place_coordinates= None
                        place_country= None
                        place_country_code = None
                        place_full_name = None
                        place_id = None
                        place_name= None
                        place_type= None
		#coming from entities object
		if tweet['entities'] != None:
			entities_hashtags= tweet['entities']['hashtags'] if tweet['entities']['hashtags'] != None else None
                	entities_urls= tweet['entities']['urls'] if tweet['entities']['urls'] != None else None
                	entities_expanded_url= tweet['entities']['urls'] if tweet['entities']['urls'] != None else None
                	entities_user_mentions= tweet['entities']['user_mentions'] if tweet['entities']['user_mentions'] != None else None
                	entities_symbols= tweet['entities']['symbols'] if tweet['entities']['symbols'] != None else None
                else :
			entities_hashtags= None
                        entities_urls= None
                        entities_expanded_url= None
                        entities_user_mentions= None
                        entities_symbols= None
		user_id= tweet['user']['id'] if tweet['user']['id'] != None else None
                user_name= tweet['user']['name'] if tweet['user']['name'] != None else None
                user_screen_name= tweet['user']['screen_name'] if tweet['user']['screen_name'] != None else None
                user_location= tweet['user']['location'] if tweet['user']['location'] != None else None
                user_url= tweet['user']['url'] if tweet['user']['url'] != None else None
                user_description= tweet['user']['description'] if tweet['user']['description'] != None else None
                #user_created_at= tweet['user']['created_at'] if tweet['user']['created_at'] != None else None
                user_time_zone= tweet['user']['time_zone'] if tweet['user']['time_zone'] != None else None
                user_lang= tweet['user']['lang'] if tweet['user']['lang'] != None else None
                geo= tweet['geo'] if tweet['geo'] != None else None
                timestamp= tweet['timestamp_ms'] if tweet['timestamp_ms'] != None else None

		
        # Split the tweet into words
            # now check if the word contains only ascii

            #if len(aword) > 0 and ascii_string(word):

               # valid_words.append([aword])

        #if not valid_words: return

        # Emit all the words

        #self.emit_many(valid_words)
		self.emit([created_at,tweet_id,tweet_text,source,quote_count,reply_count,retweet_count,favorite_count,lang,coordinates,place_coordinates,place_country,place_country_code,place_full_name,place_id,place_name,place_type,entities_hashtags,entities_urls,entities_expanded_url,entities_user_mentions,entities_symbols,user_id,user_name,user_screen_name,user_location,user_url,user_description,user_time_zone,user_lang,geo,timestamp]) 

        # tuple acknowledgement is handled automatically
