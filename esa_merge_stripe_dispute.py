"""
This module is to merge stripe billing data from an S3 path to a target tables.

Author: Om
"""

import os
import sys
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from merge_utils_v1_0 import *
from pyspark.sql.functions import from_json, explode, regexp_replace, col, to_timestamp, translate, current_timestamp
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType, ArrayType, DoubleType

# Define and retrieve job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']

logging.basicConfig(level='INFO')
logger = logging.getLogger(job_name)

CONF_BUCKET = "pettable-datalake-etl-staging-us-east-1"
CONF_FILE = "glue/conf/stripe_job_params_prod.json"
MERGE_HISTORY_TABLE = "esa-cdc-merge-history"
CDC_TRACKING_TABLE = "esa-dms-cdc-tracking"
DB_NAME = "dwh_stripe"
TABLE_NAME = "stripe_dispute"
DELIMITER = "|"

MERGE_STRIPE_DISPUTE_QUERY = f"""
        MERGE INTO glue_catalog.{DB_NAME}.{TABLE_NAME} t 
            USING UpdateDispute s
            ON s.payment_intent_id = t.payment_intent_id
            WHEN MATCHED THEN UPDATE SET
                t.dispute_id = s.dispute_id,
                t.charge_id = s.charge_id,
                t.dispute_amount = s.dispute_amount,
                t.dispute_created_at = s.dispute_created_at,
                t.record_created_at = s.record_created_at,
                t.dispute_data = s.dispute_data,
                t.reason = s.reason,
                t.created_at = s.created_at
            WHEN NOT MATCHED THEN INSERT
            (payment_intent_id, dispute_id, charge_id, dispute_amount, dispute_created_at, record_created_at, dispute_data,reason,created_at)
            VALUES
            (s.payment_intent_id, s.dispute_id, s.charge_id, s.dispute_amount, s.dispute_created_at, s.record_created_at, s.dispute_data,s.reason,s.created_at)
        """
        
conf_json = getJobConf(CONF_BUCKET, CONF_FILE)
raw_bucket = getConfigValue(conf_json, "raw_bucket")
db_items = getConfigValue(conf_json, DB_NAME)

merge_history = MergeHistory(MERGE_HISTORY_TABLE)

# Create a GlueContext & sparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.sparkSession
spark.sparkContext.setLogLevel("INFO")
# Set the configuration to handle timestamps without timezone
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def get_file_prefix():
    ## get file prefix from the table metadata json ##
    file_prefix = [item[TABLE_NAME]["file_path"] 
                    for item in db_items if TABLE_NAME in item][0]
    if file_prefix:
        logger.info(f"File path for {DB_NAME}.{TABLE_NAME}: {file_prefix}")
        return file_prefix
    else:
        logger.warning(f"File Path location not configured for {DB_NAME}.{TABLE_NAME}.")
        raise ValueError(f"File path location not configured for {DB_NAME}.{TABLE_NAME}.")


def get_file_path(bucket, prefix=None, filename=None):
    ## build s3 file path ##
    path_eles = ["s3:/", bucket, prefix, filename]
    path_eles = [e for e in path_eles if e != None]
    return "/".join(path_eles)


def read_files(spark, bucket, prefix, initial_flag,filename='', header=False, delimiter=',', escape="\"", multiline=False):
    ## read files from s3 file ##
    try:
        if initial_flag:
            load_files = get_file_path(bucket=bucket, prefix=prefix, filename="LOAD*")
        else:
            load_files = get_file_path(bucket=bucket, prefix=prefix, filename=filename)
        print(load_files)
        
        logger.info("Loading data files")
        if multiline:
            df = spark.read.format("csv").option("header", "true" if header else "false").option("inferSchema", "true").schema(explicit_schema).option("delimiter", delimiter).option("multiline", "true").option("escape", escape).load(load_files)
        else:
            df = spark.read.format("csv").option("header", "true" if header else "false").option("inferSchema", "true").option("delimiter", delimiter).load(load_files)
        ## define explicit_schema for timestamp column that can be null
        explicit_schema = StructType([
            StructField("dispute_amount", DoubleType(), False),
            StructField("dispute_created_at", TimestampType(), False)
        ])
        ## Convert timestamp of type string to timestamp
        for field in explicit_schema.fields:
            df = df.withColumn(field.name, df[field.name].cast(field.dataType))
        ## get json schema and convert json column from string to json
        df = df.withColumn("dispute_data", regexp_replace(col("dispute_data"), '\\"', "'"))
        df = df.withColumn("dispute_data", regexp_replace(col("dispute_data"), "''", '"'))
        ## add current timestamp column
        df = df.withColumn("created_at", current_timestamp())
        logger.info("Read Files completed.")
        return df
        
    except Exception as e:
        logger.error("Failed to merge file. Error: {}".format(str(e)))
        raise
    
def initial_load(spark, raw_bucket, file_path):
    # Initial load to billing item tables
    try:
        df = read_files(spark, raw_bucket, file_path,initial_flag=True,filename='', header=True, delimiter=DELIMITER, multiline=False)
        #df.createOrReplaceTempView('stripe_payment')
        #result = spark.sql("SELECT * FROM stripe_payment")
        df.write.insertInto(f"glue_catalog.{DB_NAME}.{TABLE_NAME}")
        
    except Exception as e:
        logger.error("Failed to perform intial load. Error: {}".format(str(e)))
        raise
                    
def apply_delta(spark, raw_bucket, file):
    ## apply delta merge logic to billing item tables ##
    try:
        # Precaution measure to ensure if code fails in update filter records are not inserted which will cause redundant records in merge table
        InsertFlag = False
        UpdateFlag = False
        #print("File "+str(file))
        df = read_files(spark, raw_bucket, file_path, initial_flag=False,filename=file, header=True, delimiter=DELIMITER, multiline=False)
        # New Payments (INSERT ONLY)
        insert_df = df.filter(col("Op") == "I").drop("Op")
        #insert_df.show()
        if not insert_df.isEmpty():
            InsertFlag = True
            
        # Updated payment (Update)
        update_df = df.filter(col("Op") == "U").drop("Op")
        #update_df.show()
        if not update_df.isEmpty():
            # Incase of multiple records with same payment_intent_id, keep one with latest timestamp
            # Drop exact duplicate records if any -- edge Incase
            update_df = update_df.dropDuplicates()
            # In the remaining records check for latest updated_at date
            update_df = update_df.groupBy("payment_intent_id").agg(max("dispute_created_at").alias("dispute_created_at")).join(update_df, ["payment_intent_id", "dispute_created_at"], "inner")
            update_df.createOrReplaceTempView('UpdatePayment')
            #result = spark.sql("SELECT * FROM UpdatePayment")
            UpdateFlag = True
        
        if InsertFlag:
            insert_df.write.insertInto(f"glue_catalog.{DB_NAME}.{TABLE_NAME}")
        if UpdateFlag:
            spark.sql(MERGE_STRIPE_DISPUTE_QUERY)
        
    except Exception as e:
        logger.error("Error while applying delta merge: {}".format(str(e)))
        raise

# Initialize a Glue job
job = Job(glueContext)
job.init(job_name, args)

# Processing
logger.info(f"""Starting merge load for stripe_dispute tables""")
try:
    file_path = get_file_prefix() 
    latest_ts = merge_history.get_latest_merge_ts(f"{DB_NAME}.{TABLE_NAME}", job_name)
    if latest_ts == "NA":
        # Check if table is empty
        qry = getLoadStatusQuery(DB_NAME, TABLE_NAME)
        df = spark.sql(qry)
        # Initial load if table is empty
        if df.isEmpty():
            logger.info("Target table is empty. Performing initial load.")
            initial_load(spark, raw_bucket, file_path)
            logger.info("Initial load complete.")
    logger.info(f"Last merge was applied at : {latest_ts}")
    
    # get all files from DB in the order of timestamp
    file_list = getFiles(CDC_TRACKING_TABLE, DB_NAME+"|"+TABLE_NAME, raw_bucket, file_path, latest_ts)
    logger.info(f"Retrieved {len(file_list)} files since {latest_ts}")
    if file_list:
        for file in file_list:
            logger.info(f"Processing file: {file}")
            fname = os.path.basename(file)
            #print(fname)
            file_name_ts = get_filename_time(fname)
            apply_delta(spark, raw_bucket, fname)
            logger.info(f"Processed file: {file}")
            merge_history.update_history(f"{DB_NAME}.{TABLE_NAME}", job_name, "delta merge", file_name_ts.isoformat())
            logger.info(f"Updated merge history with processed file timestamp: {file_name_ts.isoformat()}")
    else:
        logger.warning(f"No new files to be merged since: {latest_ts}")
except Exception as e:
    logger.error("Error while processing billing_items_event data: {}".format(str(e)))
    raise

# Commit the Glue job
job.commit()