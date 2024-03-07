"""
This module is to normalise data for billing_item_successful_refund.

Author: Avahi
"""

from datetime import datetime
import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from merge_utils_v1_0 import *
from payment_json_schema import *
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, when, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType, TimestampType
import json

# Define and retrieve job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']

logging.basicConfig(level='INFO')
logger = logging.getLogger(job_name)

MERGE_HISTORY_TABLE = "esa-cdc-merge-history"
PUBLIC_DB_NAME = "public"
DWH_DB_NAME = "dwh"
DB_NAME = "dwh_stripe"
STRIPE_DISPUTE_TABLE = "stripe_dispute"
STRIPE_PAYMENT_TABLE = "stripe_payment"
SUCCESSFUL_DISPUTE_TABLE = "dispute_norm_stg"
STRIPE_DISPUTE_TABLE_MERGE_TIMESTAMP_COLUMN = "created_at"

LATEST_MERGE_TS_SELECT = f"""
SELECT {STRIPE_DISPUTE_TABLE_MERGE_TIMESTAMP_COLUMN} 
FROM dispute_items_temp 
ORDER BY {STRIPE_DISPUTE_TABLE_MERGE_TIMESTAMP_COLUMN} DESC LIMIT 1
"""

NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
SPARK_TS_FORMAT = 'yyyy-MM-dd HH:mm:ss'

# v_product_history
V_PRODUCT_HISTORY = f"""
SELECT id,
    tag,
    name,
    healthie_offering_id,
    stripe_product_tag,
    type,
    category,
    purchase_location,
    purchase_type,
    operational_costs,
    is_active,
    starting_at,
    created_at,
    purchase_location = 'In-App' AS is_in_app_purchase,
    type = 'Renewal' AS is_renewal,
    purchase_location = 'Website' AS is_website_purchase
   FROM glue_catalog.{PUBLIC_DB_NAME}.product_history;
"""

# Get product tag from payments table. Join PaymentData JSON to refunds record
MERGE_PRODUCT_DISPUTE = f"""
SELECT 
p.payment_created_at,
d.charge_id,
p.email, 
d.dispute_id, 
d.dispute_created_at,
d.dispute_amount,
d.reason as dispute_reason,
d.dispute_data,
d.created_at
from dispute_items_temp d
LEFT JOIN glue_catalog.{DB_NAME}.{STRIPE_PAYMENT_TABLE} p
ON d.charge_id = p.charge_id
"""


# BILLING_ITEMS_SUCCESSFUL_REFUND
SUCCESSFUL_DISPUTE_SELECT_QUERY = f"""
SELECT
    'Stripe' AS payment_processor,
    d.payment_created_at,
    d.charge_id,
    d.email,
    d.dispute_id,
    d.dispute_created_at,
    d.dispute_response_required_by,
    d.dispute_amount AS disputed_amount,
    d.dispute_status,
    d.dispute_reason,
    to_timestamp('{NOW}', '{SPARK_TS_FORMAT}') AS processed_at
FROM  dispute_items_temp d
"""

SUCCESSFUL_DISPUTE_MERGE_QUERY = f"""
MERGE INTO glue_catalog.{PUBLIC_DB_NAME}.{SUCCESSFUL_DISPUTE_TABLE} t
USING ( SELECT 
            payment_processor,
            payment_created_at,
            charge_id,
            email,
            dispute_id,
            dispute_created_at,
            dispute_response_required_by,
            disputed_amount,
            dispute_status,
            dispute_reason,
            processed_at
            FROM successful_dispute_stage
    ) s
ON s.charge_id = t.charge_id AND s.dispute_id = t.dispute_id AND s.payment_processor = t.payment_processor
WHEN MATCHED THEN UPDATE SET
    t.payment_processor=s.payment_processor,
    t.payment_created_at=s.payment_created_at,
    t.charge_id=s.charge_id,
    t.email=s.email,
    t.dispute_id=s.dispute_id,
    t.dispute_created_at=s.dispute_created_at,
    t.dispute_response_required_by=s.dispute_response_required_by,
    t.disputed_amount=s.disputed_amount,
    t.dispute_status=s.dispute_status,
    t.dispute_reason=s.dispute_reason,
    t.processed_at=s.processed_at
WHEN NOT MATCHED THEN INSERT
    (payment_processor,
    payment_created_at,
    charge_id,
    email,
    dispute_id,
    dispute_created_at,
    dispute_response_required_by,
    disputed_amount,
    dispute_status,
    dispute_reason,
    processed_at)
VALUES
    (s.payment_processor,
    s.payment_created_at,
    s.charge_id,
    s.email,
    s.dispute_id,
    s.dispute_created_at,
    s.dispute_response_required_by,
    s.disputed_amount,
    s.dispute_status,
    s.dispute_reason,
    s.processed_at)
"""

# SUCCESSFUL_REFUND_DELETE_QUERY = f"""DELETE FROM glue_catalog.{PUBLIC_DB_NAME}.{SUCCESSFUL_DISPUTE_SELECT_QUERY}
#                    WHERE purchase_external_id IN (SELECT purchase_external_id FROM billing_item_payment_temp)
#                     AND user_id NOT IN (SELECT user_id FROM billing_item_payment_temp);"""

merge_history = MergeHistory(MERGE_HISTORY_TABLE)

# Create a GlueContext & sparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.sparkSession
spark.sparkContext.setLogLevel("INFO")
# Set the configuration to handle timestamps without timezone
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")



def load_billing_item_refunds(latest_merge_ts=None):
    SPARK_TS_FORMAT = 'yyyy-MM-dd HH:mm:ss'
    if latest_merge_ts:
        latest_merge_dt = datetime.fromisoformat(latest_merge_ts)
        merge_ts_str = latest_merge_dt.strftime("%Y-%m-%d %H:%M:%S")
        df = spark.sql(f"""SELECT * FROM glue_catalog.{DB_NAME}.{STRIPE_DISPUTE_TABLE} WHERE {STRIPE_DISPUTE_TABLE_MERGE_TIMESTAMP_COLUMN} > to_timestamp('{merge_ts_str}', '{SPARK_TS_FORMAT}')""")
    else:
        df = spark.sql(f"""SELECT * FROM glue_catalog.{DB_NAME}.{STRIPE_DISPUTE_TABLE}""")
    return df


# Initialize a Glue job
job = Job(glueContext)
job.init(job_name, args)

# Processing
logger.info(f"""Starting merge load for {PUBLIC_DB_NAME}.{SUCCESSFUL_DISPUTE_TABLE}""")
try:
    
    SUCCESSFUL_REFUND_DATASET = f"{PUBLIC_DB_NAME}.{SUCCESSFUL_DISPUTE_TABLE}"
      # Get latest merge timestamp
    latest_merge_ts = merge_history.get_latest_merge_ts(SUCCESSFUL_REFUND_DATASET, job_name)
    if latest_merge_ts == "NA":
        logger.info(f"Latest merge history for dataset {SUCCESSFUL_REFUND_DATASET} not found.")
        # Load all data
        dispute_items_temp = load_billing_item_refunds()
    else:
        logger.info(f"Latest timestamp retrieved from merge history for dataset: {SUCCESSFUL_REFUND_DATASET} as {latest_merge_ts}")
        # Load after latest merge timestamp
        dispute_items_temp = load_billing_item_refunds(latest_merge_ts=latest_merge_ts)
        
    if dispute_items_temp.isEmpty():
        logger.warning("There is no new data available to process since last  merge")
    else:
        # Preprocessing Dispute
        dispute_items_temp.createOrReplaceTempView("dispute_items_temp")
        dispute_items_temp = spark.sql(MERGE_PRODUCT_DISPUTE)
        
        # Extract dispute response required by date and dispute status
        dispute_items_temp = dispute_items_temp.withColumn("dispute_data",regexp_replace(col("dispute_data"), "^'|'$", ""))
        json_schema = dispute_json_schema()
        dispute_items_temp = dispute_items_temp.withColumn("dispute_data", from_json(col("dispute_data"),schema = json_schema))
        dispute_items_temp = dispute_items_temp.withColumn("dispute_response_required_by", from_unixtime(when(col("dispute_data.evidence_details").isNull() | col("dispute_data.evidence_details.due_by").isNull(),'').otherwise(col("dispute_data.evidence_details.due_by"))).cast(TimestampType()))
        dispute_items_temp = dispute_items_temp.withColumn("dispute_status", when(col("dispute_data.status").isNull(),'').otherwise(col("dispute_data.status")))
        dispute_items_temp.createOrReplaceTempView("dispute_items_temp")
        
        # Cache - billing_item_payment_temp
        logger.info("Caching view dispute_items_temp")
        spark.sql("CACHE TABLE dispute_items_temp")
        record_count = spark.sql("SELECT COUNT(*) as record_count FROM dispute_items_temp").collect()[0]
        if record_count['record_count'] == 0: 
            logger.warn("No new records to process")
        else:
            logger.info(f"Cached {record_count['record_count']} records.")
            logger.info(f"""PAYMENTS COUNT: {record_count['record_count']}""")
        
            # PRODUCT HISTORY
            # v_product_history = spark.sql(V_PRODUCT_HISTORY)
            # v_product_history.createOrReplaceTempView("v_product_history")
        
            # Get latest updated_at timestamp - Return type: datetime
            latest_updated_at = spark.sql(LATEST_MERGE_TS_SELECT).collect()[0][STRIPE_DISPUTE_TABLE_MERGE_TIMESTAMP_COLUMN]
            logger.info(f"Latest timestamp in loaded data is: {latest_updated_at.isoformat()}")
            successful_dispute = spark.sql(SUCCESSFUL_DISPUTE_SELECT_QUERY)
            logger.info(f"SUCCESSFUL_DISPUTE COUNT: {successful_dispute.count()}")
            
            successful_dispute.createOrReplaceTempView("successful_dispute_stage")
            spark.sql(SUCCESSFUL_DISPUTE_MERGE_QUERY)
            # Update history
            merge_history.update_history(SUCCESSFUL_REFUND_DATASET, job_name, "incremental etl", latest_updated_at.isoformat())
            logger.info("Updated latest merge history timestamp for dataset: {} to {}".format(SUCCESSFUL_REFUND_DATASET, latest_updated_at.isoformat()))
except Exception as e:
    logger.error("Error normalizing: {}".format(str(e)))
    raise

# Commit the Glue job
job.commit()

