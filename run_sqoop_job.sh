#!/bin/bash

# Replace with actual DB info
DB_HOST="localhost"
DB_NAME="training"
DB_USER="root"
DB_PASS="yourpassword"
TABLE_NAME="Movies"
TARGET_DIR="/user/$USER/mysql_movies"

# Sqoop job
sqoop import \
  --connect jdbc:mysql://$DB_HOST/$DB_NAME \
  --username $DB_USER \
  --password $DB_PASS \
  --table $TABLE_NAME \
  --target-dir $TARGET_DIR \
  --as-textfile \
  --m 1

echo "Sqoop import completed: $TARGET_DIR"
