import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, max as spark_max
from pyspark.sql import functions as F
from pyspark.sql.functions import udf,lit,length
from pyspark.sql.types import DateType,TimestampType,LongType,StringType,DoubleType
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import SparkSession

import boto3, re
import datetime
from array import array
from pyspark.sql.functions import *
from io import StringIO
import pandas as pd
from boto3.dynamodb.conditions import Key
import requests
import json
today = datetime.datetime.now()

ACCESS_KEY = "AWS_ACCESS_KEY_ID"
SECRET_KEY = "AWS_SECRET_ACCESS_KEY"

TABLE_NAME = "aws_glue_watermark"
missingdata = []
fileissue = []
completedtasks = []
# Creating the DynamoDB Table Resource
dynamodb = boto3.resource(
    "dynamodb",
    region_name="ap-south-1",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)
table = dynamodb.Table(TABLE_NAME)

# Creating the s3 Client
s3_client = boto3.client("s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def process_task(taskname: str):
    logs_data=[]
    
    logger.info("Ingestion started for - {}".format(taskname))
    taskname_pre = taskname
    lastrun_data = get_lastrun_status(taskname_pre)
    logger.info("lastrun_data - {}".format(lastrun_data))
    s3_filtered_list = get_filteredlist(taskname,lastrun_data)
    
    if not s3_filtered_list:
        missingdata.append(taskname)
    if len(s3_filtered_list):
        logger.info("list is not empty")
        try:
            # Define timestamp columns
            timestamp_columns ={}
            if taskname_pre == 'patient_demographics':
                timestamp_columns = {"regdate", "dob"}
            elif taskname_pre == 'pharmacy_billing_detail':
                timestamp_columns = {"billdate","expdate"}
            elif taskname_pre == 'ot_records':
                timestamp_columns = {"surgerydate","dob","admissiondate","dischargedate"}
            elif taskname_pre == 'opd_data':
                timestamp_columns = {"opddate"}
            elif taskname_pre == 'pharmacy_billing_master':
                timestamp_columns ={"billdate"}
            elif taskname_pre == 'patient_history_systemic':
                timestamp_columns ={"dob","startdate","entrydatetime"}
            elif taskname_pre == 'opd_optometrist_examination':
                timestamp_columns = {"opddate","dob","firstouttimetoop"}   
            elif taskname_pre == 'opd_doctor_examination':
                timestamp_columns = {"opddate","dob","firstouttimetodr"}
            elif taskname_pre in {'patient_history_surgical','patient_history_general'}:
                timestamp_columns ={"dob","entrydatetime"}
            elif taskname_pre in {'optical_billing_detail','optical_billing_master'}:
                timestamp_columns = {"billdate", "deliverydate","orderdate","promisdate"}    
            elif taskname_pre in {'intraocular_pressure_data','opd_complaints','opd_diagnosis','opd_advised_investigation','opd_advised_medicine','opd_advised_surgery','counselor_data'}:
                timestamp_columns = {"opddate", "dob"}
            elif taskname_pre in {'hospital_billing_detail', 'hospital_billing_master'}:
                timestamp_columns = {"billdate", "dob"}
                
            max_last_modified = create_df_from_s3(s3_filtered_list,taskname,lastrun_data,timestamp_columns)
            logger.info("max_last_modified - {}".format(max_last_modified))
            if max_last_modified: 
                write_max_modifieddate(taskname,lastrun_data,max_last_modified)
                logger.info("lastupload_date has been updated for - {}".format(taskname))
            logger.info("Ingestion done for - {}".format(taskname))
                
        except Exception as e:
                # Get current system exception
                ex_type, ex_value, ex_traceback = sys.exc_info()
                errormeta= {}
                errormeta["Taskname"]=str(taskname)
                errormeta["error"] = str(ex_value)
                errormeta["Filename"] = file if "file" in locals() else "Unknown"
                fileissue.append(errormeta)
                logger.error(f"Error processing file: {errormeta['Filename']}, Task: {taskname}, Error: {ex_value}")
                #raise TypeError("unexpected Error")
    return True    
    
def write_max_modifieddate(taskname,lastrun_data,lastdatestr):
    tasknamestr = task_lwr = taskname.lower()
    table.update_item(
        Key={"labname": "dummylabname", "tablename": "{}".format(lastrun_data["tablename"])},
        UpdateExpression="SET lastupload_date = :updated",
        ExpressionAttributeValues={":updated": f"{lastdatestr}"},
    )
    return True
    
def create_df_from_s3(s3_filtered_list,taskname,lastrun_data,timestamp_columns):   
    s3_input_files = [(file["Key"], file["LastModified"]) for file in s3_filtered_list]
    
    # Read all JSON files and add partition columns
    df_list = []
    for file, last_modified in s3_input_files:
        try:
            logger.info(f"Processing file: {file}")
            year, month, day = extract_partition_values(file)

            if year and month and day:
                # Read JSON file
                temp_df = spark.read.json(f"s3://{lastrun_data['source_bucket']}/{file}")
                
                # Cast specified columns to TimestampType, and all others to StringType
                for col_name in temp_df.columns:
                    if col_name in timestamp_columns:
                        temp_df = temp_df.withColumn(col_name, col(col_name).cast(TimestampType()))
                    else:
                        temp_df = temp_df.withColumn(col_name, col(col_name).cast(StringType()))
                
                temp_df = temp_df.withColumn("year", lit(int(year))) \
                                 .withColumn("month", lit(int(month))) \
                                 .withColumn("day", lit(int(day))) \
                                 .withColumn("LastModified", lit(str(last_modified))) 
                
                df_list.append(temp_df)
				
                row_count = temp_df.count()
                logger.info(f"datacount: {row_count}")
                rowdata={}
                rowdata["dataCount"]= row_count
                #rowdata["IngestingCount"] = row_count
                rowdata["tablename"] = taskname
                rowdata["filepath"] = file
                # logger.info(f"rowdata copy: {rowdata.copy()}")
                completedtasks.append(rowdata.copy())	

        except Exception as e:
            logger.error(f"Error processing file: {file}, Error: {str(e)}")
            continue  # Skip this file and continue processing the rest
   
    s3_output_path = "s3://{}/".format(lastrun_data["target_bucket"])+"{}".format(lastrun_data["target_prefix"])+"data/processed/{}".format(taskname)
    logger.info("s3_output_path - {}".format(s3_output_path))
    # Combine all dataframes
    if df_list:
        final_df = df_list[0]
        for df in df_list[1:]:
            final_df = final_df.union(df)
        logger.info("final_df - {}".format(final_df))    
        # Find max LastModified
        max_timestamp_df = final_df.agg(spark_max(col("LastModified")).alias("MaxLastModified"))
        # Show result
        max_last_modified = max_timestamp_df.collect()[0]["MaxLastModified"]
        # logger.info("Max last modified - {}".format(max_last_modified))
    
        # Write to S3 in Parquet format partitioned by year, month, day
        final_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(s3_output_path)    
    return max_last_modified        
    
def extract_partition_values(s3_path):
    match = re.search(r'year=(\d+)/month=(\d+)/day=(\d+)', s3_path)
    if match:
        return match.groups()
    return None, None, None
    
def get_filteredlist( taskname,last_rundata):
    datetime_object = datetime.datetime.strptime(last_rundata["lastupload_date"], "%Y-%m-%d %H:%M:%S%z")
    paginator = s3_client.get_paginator("list_objects_v2")
    s3_filtered_list = [
        obj
        for page in paginator.paginate(
            Bucket="{}".format(last_rundata["source_bucket"]),
            Prefix="{}".format(last_rundata["source_prefix"]) + "{}".format(taskname) ,
        )
        for obj in page["Contents"]
        if obj["LastModified"] > datetime_object
    ]
    sorted_contents = sorted(s3_filtered_list, key=lambda d: d["LastModified"], reverse=False)
    return sorted_contents    
    
    
def get_lastrun_status(taskname):
    tasknamestr = taskname.replace(" ", "_" )
    task_lwr = tasknamestr.lower()
    response = table.query(
        KeyConditionExpression=Key("labname").eq("asg")
        & Key("tablename").eq(f"{task_lwr}")
    )
    return response["Items"][0]    

def push_exceptions(message):
    data={}
    data["emailList"] =  ["nitesh.xortix@gmail.com"]
    data["subject"] = " DATA PIPELINE ALERT - " + str(today.strftime("%Y-%m-%d"))
    data["from"] = "Sync Alert"
    data["content"] = "<html><p>Hi Team,</p><p>"+str(message)+"</p></html>"
    headersparam={"Content-Type": "application/json"}
    url="https://mddr7aigo9.execute-api.ap-south-1.amazonaws.com/test/sync/emailalert"
    resp = requests.post(url, data=json.dumps(data),headers=headersparam)
    print (resp.status_code, resp.reason)
    return resp    
    
def start_ingesting():
    logger.info("#############starting the process################")
    tasklist = ["patient_demographics","optical_billing_detail","hospital_billing_detail","pharmacy_billing_detail","ot_records","opd_diagnosis","opd_data","opd_advised_investigation","opd_advised_medicine","opd_advised_surgery","counselor_data","hospital_billing_master","pharmacy_billing_master","patient_history_systemic","patient_history_surgical","patient_history_general","optical_billing_master","opd_optometrist_examination","opd_doctor_examination","opd_complaints","intraocular_pressure_data"]
    
    for x in tasklist:
        process_task(f"{x}")
    if fileissue:
      df_error = pd.DataFrame(fileissue)
      df_error_html =  "files with issues are : <br> " +  df_error.to_html()
    else:
      df_error_html = ''
	 
    data_status_missing = ','.join(missingdata)
	
    logger.info(str(completedtasks)) 
    if not completedtasks:
        df = pd.DataFrame(columns=["Taskname","Filepath","RowCount"])
        data_status_completed = df.to_html()
    else:
        df = pd.DataFrame(completedtasks)
        df.rename(columns={'tablename': 'Taskname','filepath': 'Filepath', 'dataCount': 'RowCount'}, inplace=True)
        df = df[["Taskname","Filepath","RowCount",]]
        # render dataframe as html
        data_status_completed = df.to_html()
        print(data_status_completed)
	
    push_exceptions(f"Data pipeline is completed successfully . <br>  <br> <strong><u>below task has the latest value - </u> </strong> <br> {data_status_completed} .<br>  <br> <strong><u>latest source data is not available for the below tasks - </u> </strong> <br>{data_status_missing} <br> {df_error_html}")
    logger.info("#############finished the process################")     

start_ingesting()
job.commit()