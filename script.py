import os
import requests
import boto3
import snowflake.connector
from botocore.exceptions import NoCredentialsError

# Configurations
DATA_URL = "https://www2.census.gov/programs-surveys/popest/datasets/2020-2023/state/asrh/sc-est2023-agesex-civ.csv"
LOCAL_FILE = "census_data.csv"

# AWS S3
AWS_ACCESS_KEY = "YOUR_AWS_ACCESS_KEY"
AWS_SECRET_KEY = "YOUR_AWS_SECRET_KEY"
S3_BUCKET = "your-s3-bucket-name"
S3_KEY = "census/census_data.csv"

# Snowflake
SNOWFLAKE_USER = "your_username"
SNOWFLAKE_PASSWORD = "your_password"
SNOWFLAKE_ACCOUNT = "your_account.region.gcp/aws"
SNOWFLAKE_DATABASE = "your_database"
SNOWFLAKE_SCHEMA = "your_schema"
SNOWFLAKE_WAREHOUSE = "your_warehouse"
SNOWFLAKE_STAGE = "your_s3_stage"  # Pre-created stage pointing to your S3 bucket

# Step 1: Download data
def download_data():
    response = requests.get(DATA_URL)
    with open(LOCAL_FILE, 'wb') as f:
        f.write(response.content)
    print("✅ Data downloaded")

# Step 2: Upload to S3
def upload_to_s3():
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY)
    try:
        s3.upload_file(LOCAL_FILE, S3_BUCKET, S3_KEY)
        print("✅ File uploaded to S3")
    except NoCredentialsError:
        print("❌ AWS credentials not found")

# Step 3: Load into Snowflake
def load_to_snowflake():
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cs = ctx.cursor()

    try:
        # Create table (simplified structure — adapt to your CSV columns)
        cs.execute(f"""
            CREATE OR REPLACE TABLE census_data (
                SUMLEV STRING,
                REGION STRING,
                DIVISION STRING,
                STATE STRING,
                NAME STRING,
                SEX STRING,
                AGE INT,
                POPESTIMATE2023 INT
            );
        """)
        print("✅ Table created")

        # Copy data from S3 (external stage must be pre-configured in Snowflake)
        cs.execute(f"""
            COPY INTO census_data
            FROM @{SNOWFLAKE_STAGE}/{S3_KEY}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            ON_ERROR = 'CONTINUE';
        """)
        print("✅ Data loaded into Snowflake")

        # Query data
        cs.execute("SELECT * FROM census_data LIMIT 5;")
        for row in cs:
            print(row)

    finally:
        cs.close()
        ctx.close()

# Run pipeline
if __name__ == "__main__":
    download_data()
    upload_to_s3()
    load_to_snowflake()
