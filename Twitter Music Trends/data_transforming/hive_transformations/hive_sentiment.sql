DROP TABLE IF EXISTS sentiment_new_removed;
CREATE EXTERNAL TABLE IF NOT EXISTS sentiment_new_removed
(
 tweet_id  string,
 tweet_text string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (    

	"separatorChar" = ",",    

	"quoteChar"     = '"',

	"escapeChar"    = "\\n" )

STORED AS TEXTFILE
LOCATION '/user/w205/sentiment_new_removed';
