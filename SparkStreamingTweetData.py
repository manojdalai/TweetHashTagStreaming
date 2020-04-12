from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row
import sys
import requests
from django.utils.encoding import smart_str, smart_unicode

# create spark configuration
sc = SparkContext(master="local[2]", appName="WindowWordCount")
sc.setLogLevel("ERROR")

# creat the Streaming Context from the above spark context with window size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost",9009)


def tags_count_aggregator(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current context
        sql_context = sql_context_instance(rdd.context)

        # convert the RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=smart_unicode(w[0]), hashtag_count=w[1]))

        # create a DF from the Row RDD
        hashtag_df = sql_context.createDataFrame(row_rdd)

        # Register the dataframe as table
        hashtag_df.registerTempTable("hashtagsTable")

        # get the top 10 hashtags from the table using SQL and print them
        count_hashtag_df = sql_context.sql("select hash_tag, hash_tag_count from hashtagsTable order by hashtag_count desc limit 10")
        count_hashtag_df.show()
    except:
        e = sys.exc_info()
        print("Error: %s" % e)

# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
#hashtags = words.map(lambda x: (x, 1))
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))

# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(tags_count_aggregator)

# do processing for each RDD generated in each interval
tags_totals.foreachRDD(process)

# Streaming started
print("Streaming....... STARTED")
ssc.start()

# wait for the streaming to be finished
print("Streaming....... FINISHED")
ssc.awaitTermination()
