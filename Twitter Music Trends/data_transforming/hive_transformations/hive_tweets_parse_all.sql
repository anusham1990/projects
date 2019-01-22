CREATE EXTERNAL TABLE IF NOT EXISTS tweets_parse_all
(
 tweet_id string, 
 tweet_text string, 
 song_name  string, 
 artist   string, 
 album   string, 
 channel  string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (    

	"separatorChar" = ",",    

	"quoteChar"     = '"',

	"escapeChar"    = "\\" )

STORED AS TEXTFILE
LOCATION '/user/w205/tweets_parse_all';
