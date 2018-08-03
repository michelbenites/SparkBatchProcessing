#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 03/19/2018
# Descr. : Count unique clicks per url by hour
from pyspark import SparkContext, SparkConf
from datetime import datetime

# Define spark context.
#conf = SparkConf().setMaster("local[*]").setAppName("URLCountPython")
conf = SparkConf().setAppName("ClickCountPython")
sc = SparkContext(conf = conf)

# Function to split lines into variables.
def parse_log_line_w5(line):
    (uuid, timestamp, url, user) = line.strip().split(" ")
    hour = timestamp[0:13]
    # Concatenate hour, url and user, in order to create a key to use in redeucebykey action.
    return (hour + ',' + url + ',' + user, uuid, 1)

# Get all files from a directory 
#text_file = sc.textFile("file:///home/michelbenites/inputlab7/*.txt")
text_file = sc.textFile("inputlab7")

# Create RDD with distinct data.
pairRDD = text_file.map(parse_log_line_w5).distinct()

# Create a new RDD only with Hour and Count.
uniqueRDD = pairRDD.map(lambda x: (x[0],1))

# Sum the same key.
counts = uniqueRDD.reduceByKey(lambda a, b: a + b)

# Save the result on the directory.
counts.coalesce(1).saveAsTextFile("outputclick7")
