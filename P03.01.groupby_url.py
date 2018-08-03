#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 03/20/2018
# Descr. : Deduplicate UUID and count unique url by hour
from pyspark import SparkContext, SparkConf
from datetime import datetime
import time


# Define spark context.
#conf = SparkConf().setMaster("local[*]").setAppName("URLCountPython")
conf = SparkConf().setAppName("GroupbyURLPython")
sc = SparkContext(conf = conf)

# Function to split lines into variables.
def parse_log_line_w5(line):
    (uuid, timestamp, url, user) = line.strip().split(" ")
    hour = timestamp[0:13]
    return (uuid, hour + "," + url)

# Function to split the 2nd argument into hour and url.
def parse_log_line_w6(line):
    (hour, url) = line[1].strip().split(",")
    return (hour, url)

# Get all files from a directory 
#text_file = sc.textFile("file:///home/michelbenites/inputlog/*.txt")
#text_file = sc.textFile("inputlab7")
text_file = sc.textFile("s3a://e-88hw07/Logs/*.txt")


# Get the initial time
start_time = time.time()


# Create RDD grouping by UUID and then deduplicate the same UUID..... 
groupedRDD = text_file.map(parse_log_line_w5).groupByKey()
# .... and then use the max condition to select the highest value of hour and url
dedupRDD   = groupedRDD.mapValues(max).map(lambda x:(x[0],x[1]))
pairRDD    = dedupRDD.map(parse_log_line_w6).distinct()


# Create a new RDD only with Hour and Count.
uniqueRDD = pairRDD.map(lambda x: (x[0],1))

# Sum the same key.
counts = uniqueRDD.reduceByKey(lambda a, b: a + b)

# Elapsed time.
elapsed_time = time.time() - start_time

# Save the result on the directory.
counts.coalesce(1).saveAsTextFile("outputgroup7")

print 'Elapsed Time:' , elapsed_time

