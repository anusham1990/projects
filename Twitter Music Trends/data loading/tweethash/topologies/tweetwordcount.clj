(ns tweetwordcount
  (:use     [streamparse.specs])
  (:gen-class))

(defn tweetwordcount [options]
   [
    ;; spout configuration
    {"tweet-spout" (python-spout-spec
          options
          "spouts.tweets.Tweets"
          ["tweet"]
          :p 3
          )
    }
    ;; bolt configuration
    {"parse-tweet-bolt" (python-bolt-spec
          options
          {"tweet-spout" :shuffle}
          "bolts.parse.ParseTweet"
          ["created_at" "tweet_id" "tweet_text" "source" "quote_count" "reply_count" "retweet_count" "favorite_count" "lang" "coordinates" "place_coordinates" "place_country" "place_country_code" "place_full_name" "place_id" "place_name" "place_type" "entities_hashtags" "entities_urls" "entities_expanded_url" "entities_user_mentions" "entities_symbols" "user_id" "user_name" "user_screen_name" "user_location" "user_url" "user_description" "user_time_zone" "user_lang" "geo" "timestamp"]
          :p 3
          )
     "count-bolt" (python-bolt-spec
          options
          {"parse-tweet-bolt" ["created_at" "tweet_id" "tweet_text" "source" "quote_count" "reply_count" "retweet_count" "favorite_count" "lang" "coordinates" "place_coordinates" "place_country" "place_country_code" "place_full_name" "place_id" "place_name" "place_type" "entities_hashtags" "entities_urls" "entities_expanded_url" "entities_user_mentions" "entities_symbols" "user_id" "user_name" "user_screen_name" "user_location" "user_url" "user_description" "user_time_zone" "user_lang" "geo" "timestamp"]}
          "bolts.wordcount.WordCounter"
          ["created_at" "tweet_id" "tweet_text" "source" "quote_count" "reply_count" "retweet_count" "favorite_count" "lang" "coordinates" "place_coordinates" "place_country" "place_country_code" "place_full_name" "place_id" "place_name" "place_type" "entities_hashtags" "entities_urls" "entities_expanded_url" "entities_user_mentions" "entities_symbols" "user_id" "user_name" "user_screen_name" "user_location" "user_url" "user_description" "user_time_zone" "user_lang" "geo" "timestamp"]
          :p 3
          )
    }
  ]
)
