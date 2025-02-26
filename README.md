# AWS Glue Job: JSON to Parquet Conversion

## Overview
This AWS Glue job reads JSON files from an S3 bucket, processes them using PySpark, and writes the data as Parquet files to another S3 location. The job uses a watermark table stored in DynamoDB to track processed data.

## Technologies Used
- AWS Glue (PySpark)
- AWS S3
- AWS DynamoDB
- Python

## Job Workflow
1. Read the *watermark* from *DynamoDB*.
2. Fetch JSON data from *S3* (based on watermark).
3. Convert JSON to *Parquet*.
4. Save Parquet files to another *S3* bucket.
5. Update *DynamoDB watermark*.
6. Create athena table based on taskname and attach the partition accordingly

## How to Run Locally
1. Install dependencies:
   ```sh
   pip install -r requirements.txt
