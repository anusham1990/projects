from __future__ import absolute_import, print_function, unicode_literals

import json

import itertools, time

import tweepy, copy

import Queue, threading

#import pandas as pd

from streamparse.spout import Spout



################################################################################

# Twitter credentials

################################################################################

twitter_credentials = {

    "consumer_key"        :  "LmHsZjJZaIWjKrKSKDHdusB99",

    "consumer_secret"     :  "G4fNgo0fLLhFtm9uOk2twO0XAQVjMTjocgCgY7yPXl5N2KJrcl",

    "access_token"        :  "3225709838-a2oXtV8MO4K7OBScOKtpsr3OoWZyw1EDjXMqVGx",

    "access_token_secret" :  "APVVWs4hTnDoyXfHSCoZZ63EixC9uG53edv5ph3AylDo0",

}



def auth_get(auth_key):

    if auth_key in twitter_credentials:

        return twitter_credentials[auth_key]

    return None



################################################################################

# Class to listen and act on the incoming tweets

################################################################################

class TweetStreamListener(tweepy.StreamListener):

    def __init__(self, listener):

        self.listener = listener

        super(self.__class__, self).__init__(listener.tweepy_api())



    def on_status(self, status):
        self.listener.queue().put(json.dumps(status._json), timeout = 0.01)
        return True



    def on_error(self, status_code):

        return True # keep stream alive



    def on_limit(self, track):

        return True # keep stream alive



class Tweets(Spout):



    def initialize(self, stormconf, context):

        self._queue = Queue.Queue(maxsize = 100)



        consumer_key = auth_get("consumer_key")

        consumer_secret = auth_get("consumer_secret")

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)



        if auth_get("access_token") and auth_get("access_token_secret"):

            access_token = auth_get("access_token")

            access_token_secret = auth_get("access_token_secret")

            auth.set_access_token(access_token, access_token_secret)



        self._tweepy_api = tweepy.API(auth)
		
		
		# Create the listener for twitter stream

        listener = TweetStreamListener(self)



        # Create the stream and listen for english tweets

        stream = tweepy.Stream(auth, listener, timeout=None)

        stream.filter(track=["#NowPlaying","#ListeningTo","#Spotify", "#listenlive", "#Grammy", "#GrammyNomination", "#Grammy2018", "#Grammys","listening to Amazon Prime Music", "listening to Amazon Music", "listening to Apple Music", "listening to Deezer", "listening to Gaana", "listening to Ghost Tunes", "listening to Ghost iTunes", "listening to Google Play All Access", "listening to Google Play Music", "listening to Google Music", "listening to Groove Music", "listening to Hungama", "listening to iHeartRadio", "listening to Line Music", "listening to MixRadio", "listening to MOG", "listening to Music Unlimited", "listening to NetEase", "listening to NPR", "listening to Guvera", "listening to hoopla", "listening to Jango", "listening to Joox", "listening to Musicovery", "listening to Patari", "listening to Qobuz", "listening to Raaga", "listening to Radical.fm", "listening to Yandex Music","now playing Amazon Prime Music", "now playing Amazon Music", "now playing Apple Music", "now playing Deezer", "now playing Gaana", "now playing Ghost Tunes", "now playing Ghost iTunes", "now playing Google Play All Access", "now playing Google Play Music", "now playing Google Music", "now playing Groove Music", "now playing Hungama", "now playing iHeartRadio", "now playing Line Music", "now playing MixRadio", "now playing MOG", "now playing Music Unlimited", "now playing NetEase", "now playing NPR", "now playing Guvera", "now playing hoopla", "now playing Jango", "now playing Joox", "now playing Musicovery", "now playing Patari", "now playing Qobuz", "now playing Raaga", "now playing Radical.fm", "now playing Yandex Music"], async = True)

	
    def queue(self):

        return self._queue
		



    def tweepy_api(self):

        return self._tweepy_api



    def next_tuple(self):

        try:

            tweet = self.queue().get(timeout = 0.1)

            if tweet:

                self.queue().task_done()

                self.emit([tweet])



        except Queue.Empty:

            self.log("Empty queue exception ")

            time.sleep(0.1)



    def ack(self, tup_id):

        pass  # if a tuple is processed properly, do nothing



    def fail(self, tup_id):

        pass  # if a tuple fails to process, do nothing

