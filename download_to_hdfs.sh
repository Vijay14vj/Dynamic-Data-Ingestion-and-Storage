#!/bin/bash

# Set variables
FILE_URL="https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/national/totals/nst-est2022-alldata.csv"
LOCAL_FILE="product_data.csv"
HDFS_DIR="/user/$USER/product_data"

# Download CSV
wget -O $LOCAL_FILE $FILE_URL

# Create HDFS directory and put the file
hadoop fs -mkdir -p $HDFS_DIR
hadoop fs -put -f $LOCAL_FILE $HDFS_DIR/

echo "File successfully placed in HDFS: $HDFS_DIR"
