#!/usr/bin/python
# Author : Michel Benites Nascimento
# Date   : 03/24/2018
# Descr. : Deduplicate UUID and count unique url by hour
from pyspark import SparkContext, SparkConf
from datetime import datetime

# Define spark context.
#conf = SparkConf().setMaster("local[*]").setAppName("URLCountPython")
conf = SparkConf().setAppName("JoinURLPython")
sc = SparkContext(conf = conf)

# Function to split lines into variables.
def parse_log_line_w5(line):
    (uuid, timestamp, url, user) = line.strip().split(" ")
    return ( user, url)

# Get all files from a directory and the community file from S3. 
#text_file = sc.textFile("file:///home/michelbenites/inputlog/*.txt")
text_file = sc.textFile("s3a://e-88hw07/Logs/*.txt")
#text_file = sc.textFile("testlog")
comm_file = sc.textFile("s3a://e-88hw07/Community/hw7_community.txt")

# Create RDD with log and community file
logRDD    = text_file.map(parse_log_line_w5)
commRDD   = comm_file.map(lambda x: x.split("\t"))
#commRDD   = comm_file.map(lambda x: (x.split("\t"))).map(lambda line: (line[0],line[1]))

# Join the two datasets and create a new RDD with url, community, 1 format
joinRDD = logRDD.join(commRDD)
pairRDD = joinRDD.map(lambda x: (x[1],1))

# Sum the same key.
counts = pairRDD.reduceByKey(lambda a, b: a + b)

# Save the result on the directory.
counts.coalesce(1).saveAsTextFile("outputjoin7")
#counts.coalesce(1).saveAsTextFile("outputtest")

