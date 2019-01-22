DROP TABLE IF EXISTS tweets_all;

CREATE EXTERNAL TABLE IF NOT EXISTS tweets_all
(
 created_at string, 
 tweet_id  string,
 tweet_text string,
 source  string,
 quote_count string,
 reply_count string,
 retweet_count string,
 favorite_count  string,
 lang string,
 coordinates string,
 place_coordinates string,
 place_country string,
 place_country_code string,
 place_full_name string,
 place_name string,
 place_type string,
 entities_hashtags string,
 user_screen_name string,
 user_location string,
 user_lang string,
 geo  string,
 timestamp string,
 sys_time string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (    

	"separatorChar" = ",",    

	"quoteChar"     = '"',

	"escapeChar"    = "\\" )

STORED AS TEXTFILE
LOCATION '/user/w205/tweets';
