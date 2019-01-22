from __future__ import absolute_import, print_function, unicode_literals

import unicodedata

import textwrap

from collections import Counter

from streamparse.bolt import Bolt

import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_READ_UNCOMMITTED 

import time


def safe_unicode(obj, *args):
    	 	# return the unicode representation of obj """
	try:
        	return unicode(obj, *args)
    	except UnicodeDecodeError:
        	# obj is byte string
        	ascii_text = str(obj).encode('string_escape')
        	return unicode(ascii_text)

def safe_str(obj):
    		# return the byte string representation of obj """
    	try:
        	value = str(obj).strip()
		if value == '[]':
			value = None
		return value
    	except UnicodeEncodeError:
        	# obj is unicode
        	return  unicode(obj).encode('unicode_escape').strip()


class WordCounter(Bolt):


    def initialize(self, conf, ctx):

        #self.counts = Counter()

	self.conn = psycopg2.connect(database="final_project", user="postgres", password="pass", host="localhost", port="5432")



    def process(self, tup):
        #word = tup.values[0]
	self.conn.set_isolation_level(ISOLATION_LEVEL_READ_UNCOMMITTED)
	cur = self.conn.cursor()
	cur.execute("SELECT tweet_id from tweets_hash where tweet_id = %s",(safe_str(tup.values[1]),))
	if cur.rowcount == 0:
		try:
			cur.execute ("INSERT INTO tweets_hash (created_at, tweet_id, tweet_text,source,quote_count, reply_count, retweet_count, favorite_count, lang, coordinates,place_coordinates, place_country,place_country_code,place_full_name,place_id, place_name,place_type,entities_hashtags,entities_urls,entities_expanded_url,entities_user_mentions, entities_symbols, user_id,user_name,user_screen_name,user_location,user_url, user_description,user_time_zone,user_lang,geo,timestamp, sys_time)VALUES (%s,%s, %s, %s,%s,%s, %s,%s,%s, %s,%s, %s,%s, %s, %s, %s, %s,%s, %s, %s,%s,%s, %s,%s,%s, %s,%s, %s,%s, %s, %s, %s, %s)",
        (       safe_str(tup.values[0]),
                safe_str(tup.values[1]),
                safe_str(tup.values[2]),
                safe_str(tup.values[3]),
                tup.values[4],
                tup.values[5],
                tup.values[6],
                tup.values[7],
                safe_str(tup.values[8]),
                safe_str(tup.values[9]),
                safe_str(tup.values[10]),
                safe_str(tup.values[11]),
                safe_str(tup.values[12]),
                safe_str(tup.values[13]),
                safe_str(tup.values[14]),
                safe_str(tup.values[15]),
                safe_str(tup.values[16]),
                safe_str(tup.values[17]),
                safe_str(tup.values[18]),
                safe_str(tup.values[19]),
                safe_str(tup.values[20]),
                safe_str(tup.values[21]),
                safe_str(tup.values[22]),
                safe_str(tup.values[23]),
                safe_str(tup.values[24]),
                safe_str(tup.values[25]),
                safe_str(tup.values[26]),
                safe_str(tup.values[27]),
                safe_str(tup.values[28]),
                safe_str(tup.values[29]),
                safe_str(tup.values[30]),
                safe_str(tup.values[31]) if len(safe_str(tup.values[31]))<16 else str(safe_str(tup.values[31]))[:16],
		str(time.strftime("%x")))) 

			self.conn.commit()
	
		except psycopg2.IntegrityError:
			pass


        # Increment the local count

        #self.counts[word] += 1

        #self.emit([word, self.counts[word]])


        # Log the count - just to see the topology running

        # self.log('%s: %d' % (word, self.counts[word]))
