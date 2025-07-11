CREATE DATABASE IF NOT EXISTS product_db;

USE product_db;

DROP TABLE IF EXISTS product_data;

CREATE EXTERNAL TABLE product_data (
    SUMLEV INT,
    REGION INT,
    DIVISION INT,
    STATE INT,
    NAME STRING,
    POPESTIMATE2022 INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/${user.name}/product_data';

-- Verify
SELECT * FROM product_data LIMIT 10;
