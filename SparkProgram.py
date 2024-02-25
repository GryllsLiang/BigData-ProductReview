# -*- coding: utf-8 -*-
# Configuration:Python 3.10.6, Spark 3.3.2, Scala 2.12.15, java version "1.8.0_341", Runs on MacOS X Version 12.5.1
# use command: spark-submit SparkProgram.py reviews_Video_Games.json meta_Video_Games.json out.txt to run the code

import pyspark
import sys

# Initialize context
conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)
sqlContext = pyspark.SQLContext(sc)

# Configure path
review_file_path = sys.argv[1]
metadata_file_path = sys.argv[2]
output_path = sys.argv[3]

######################################################################################
# Step 1. Find the number of reviews in each day, and calculate the average ratings 
# of the review in each day from the review file. Use pair RDD, reduceByKey and 
# map function to accomplish this step. The key is a tuple 
# (the product ID/asin, review time). The value is a tuple (#review, average_ratings).
######################################################################################
# Read review file
review_rdd = sqlContext.read.json(review_file_path).rdd

# Extract needed value
asin_date_rdd = review_rdd.map(lambda x: ((x['asin'], x['reviewTime']),(1, x['overall'])))
# Calculate count
asin_date_count_rdd = asin_date_rdd.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
# Calculate average
asin_data_average_rdd = asin_date_count_rdd.map(lambda a: (a[0], (a[1][0], round(a[1][1]/a[1][0],2))))

######################################################################################
# Step 2. Create an RDD where the key is the product ID/asin 
# and value is the brand name of the product. 
# Use the metadata for this step.
######################################################################################
# This is a function for mark data without 'brand' value
def get_brand_name(metadata):
    if 'brand' in metadata:
        return metadata['brand']
    else:
        return 'Unknown'

# Read meta file
metadata_rdd = sqlContext.read.json(metadata_file_path).rdd
# Extract needed value
metadata_asin_rdd = metadata_rdd.map(lambda x: (x['asin'], get_brand_name(x)))
# Filter those data without 'brand' value
metadata_asin_rdd = metadata_asin_rdd.filter(lambda x: x[1] != None)

######################################################################################
# Step 3. Join the pair RDD obtained in Step 1 and the RDD created in Step 2.
######################################################################################
# Extrat needed attrbutes, reset the key (only 'asin' as key)
asin_avgrating_rdd = asin_data_average_rdd.map(lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1])))
# Join the two tables
asin_id_brand_rdd = asin_avgrating_rdd.join(metadata_asin_rdd)

######################################################################################
# Step 4. Find the top 15 products with the greatest number of reviews in a day.
######################################################################################
# Sort items by # of review
products_sorted_rdd = asin_id_brand_rdd.sortBy(ascending=False, keyfunc=lambda k: k[1][0][1])
# Get top 15 products
top_15_products = products_sorted_rdd.take(15)

######################################################################################
# Step 5. Output the number of reviews, review time, average ratings and brand name 
# for the top 15 products identified in Step 4.
# output: <product ID > <the number of reviews> <review time> < average ratings > <product brand name >
######################################################################################
# Write into file
with open(output_path, 'w+') as f:
  for (asin, ((reviewtime, reviewcount, rating),brand)) in products_sorted_rdd.take(15):
      f.write(f"{asin} {reviewcount} {reviewtime} {rating} {brand}\n")

sc.stop()