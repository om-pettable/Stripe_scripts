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
from pyspark.sql.functions import regexp_replace, col, when, from_json
from pyspark.sql.functions import col, udf

# Define and retrieve job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']

logging.basicConfig(level='INFO')
logger = logging.getLogger(job_name)

MERGE_HISTORY_TABLE = "esa-cdc-merge-history"
PUBLIC_DB_NAME = "public"
DWH_DB_NAME = "dwh"
DB_NAME = "dwh_stripe"
STRIPE_REFUND_TABLE = "stripe_refund"
STRIPE_PAYMENT_TABLE = "stripe_payment"
SUCCESSFUL_REFUND_TABLE = "successful_refund_norm_stg"
STRIPE_REFUND_TABLE_MERGE_TIMESTAMP_COLUMN = "created_at"

LATEST_MERGE_TS_SELECT = f"""
SELECT {STRIPE_REFUND_TABLE_MERGE_TIMESTAMP_COLUMN} 
FROM refund_items_temp 
ORDER BY {STRIPE_REFUND_TABLE_MERGE_TIMESTAMP_COLUMN} DESC LIMIT 1
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
MERGE_PRODUCT_REFUND = f"""
SELECT 
r.payment_intent_id, 
r.dispute_id, 
r.charge_id, 
r.amount_refunded, 
r.is_partially_refunded, 
r.is_fully_refunded,
r.is_dispute, 
r.dispute_created_at, 
r.payment_refunded_at, 
r.refund_data, 
r.reason, 
r.created_at, 
p.payment_created_at, 
p.email, 
p.amount as payment_amount, 
p.payment_data
from refund_items_temp r
LEFT JOIN glue_catalog.{DB_NAME}.{STRIPE_PAYMENT_TABLE} p
ON r.payment_intent_id = p.payment_intent_id
"""


# BILLING_ITEMS_SUCCESSFUL_REFUND
SUCCESSFUL_REFUND_SELECT_QUERY = f"""
SELECT 1 AS q,
    'Stripe' AS payment_processor,
    r.payment_created_at,
    r.payment_refunded_at AS refund_created_at,
    r.charge_id,
    r.payment_intent_id AS purchase_external_id,
    r.email,
    r.payment_amount,
    r.amount_refunded AS refund_amount,
    r.amount_refunded >= 99.0 AS is_full_refund,
    ph.id AS product_history_id,
    ph.name AS product_name,
    r.payment_refunded_at AS refund_date,
    to_timestamp('{NOW}', '{SPARK_TS_FORMAT}') AS processed_at
FROM  refund_items_temp r
LEFT JOIN glue_catalog.{PUBLIC_DB_NAME}.product_history ph
ON ph.tag == r.productTag and ph.starting_at <= r.payment_created_at
WHERE r.payment_refunded_at IS NOT NULL AND r.email not ilike '%pettable%' AND r.email not ilike '%mechanism%'
"""

SUCCESSFUL_REFUND_MERGE_QUERY = f"""
MERGE INTO glue_catalog.{PUBLIC_DB_NAME}.{SUCCESSFUL_REFUND_TABLE} t
USING (SELECT q,
payment_processor,
payment_created_at,
payment_created_at as created_at,
refund_created_at,
charge_id,
purchase_external_id,
email,
payment_amount,
refund_amount,
is_full_refund,
product_history_id,
product_name,
processed_at FROM success_refund_stage
) s
ON s.purchase_external_id = t.purchase_external_id AND s.payment_processor = t.payment_processor AND s.payment_created_at = t.payment_created_at AND s.charge_id = t.charge_id
WHEN MATCHED THEN UPDATE SET
t.q=s.q,
t.payment_processor=s.payment_processor,
t.payment_created_at=s.payment_created_at,
t.created_at=s.created_at,
t.refund_created_at=s.refund_created_at,
t.charge_id=s.charge_id,
t.purchase_external_id=s.purchase_external_id,
t.email=s.email,
t.payment_amount=s.payment_amount,
t.refund_amount=s.refund_amount,
t.is_full_refund=s.is_full_refund,
t.product_history_id=s.product_history_id,
t.product_name=s.product_name,
t.processed_at=s.processed_at
WHEN NOT MATCHED THEN INSERT
(q,
payment_processor,
payment_created_at,
created_at,
refund_created_at,
charge_id,
purchase_external_id,
email,
payment_amount,
refund_amount,
is_full_refund,
product_history_id,
product_name,
processed_at)
VALUES
(s.q,
s.payment_processor,
s.payment_created_at,
s.created_at,
s.refund_created_at,
s.charge_id,
s.purchase_external_id,
s.email,
s.payment_amount,
s.refund_amount,
s.is_full_refund,
s.product_history_id,
s.product_name,
s.processed_at)
"""

# SUCCESSFUL_REFUND_DELETE_QUERY = f"""DELETE FROM glue_catalog.{PUBLIC_DB_NAME}.{SUCCESSFUL_REFUND_SELECT_QUERY}
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
        df = spark.sql(f"""SELECT * FROM glue_catalog.{DB_NAME}.{STRIPE_REFUND_TABLE} WHERE {STRIPE_REFUND_TABLE_MERGE_TIMESTAMP_COLUMN} > to_timestamp('{merge_ts_str}', '{SPARK_TS_FORMAT}')""")
    else:
        df = spark.sql(f"""SELECT * FROM glue_catalog.{DB_NAME}.{STRIPE_REFUND_TABLE}""")
    return df


# Initialize a Glue job
job = Job(glueContext)
job.init(job_name, args)

# Processing
logger.info(f"""Starting merge load for {PUBLIC_DB_NAME}.{SUCCESSFUL_REFUND_TABLE}""")
try:
    
    SUCCESSFUL_REFUND_DATASET = f"{PUBLIC_DB_NAME}.{SUCCESSFUL_REFUND_TABLE}"
      # Get latest merge timestamp
    latest_merge_ts = merge_history.get_latest_merge_ts(SUCCESSFUL_REFUND_DATASET, job_name)
    if latest_merge_ts == "NA":
        logger.info(f"Latest merge history for dataset {SUCCESSFUL_REFUND_DATASET} not found.")
        # Load all data
        refund_items_temp = load_billing_item_refunds()
    else:
        logger.info(f"Latest timestamp retrieved from merge history for dataset: {SUCCESSFUL_REFUND_DATASET} as {latest_merge_ts}")
        # Load after latest merge timestamp
        refund_items_temp = load_billing_item_refunds(latest_merge_ts=latest_merge_ts)
    
    if refund_items_temp.isEmpty():
        logger.warning("There is no new data available to process since last  merge")
    else:
        # preprocessing refunds
        refund_items_temp.createOrReplaceTempView("refund_items_temp")
        refund_items_temp = spark.sql(MERGE_PRODUCT_REFUND)
        #refund_items_temp.printSchema()
        #refund_items_temp.show()
        
        # Extract Prodcuts Tag
        refund_items_temp = refund_items_temp.withColumn("payment_data",regexp_replace(col("payment_data"), "^'|'$", ""))
        json_schema = payment_json_schema()
        refund_items_temp = refund_items_temp.withColumn("payment_data",from_json(col("payment_data"),schema = json_schema))
        refund_items_temp = refund_items_temp.withColumn("productTag",when(col("payment_data.metadata").isNull() | col("payment_data.metadata.packageTag").isNull(),'').otherwise(col("payment_data.metadata.packageTag")))
        refund_items_temp.createOrReplaceTempView("refund_items_temp")
        
        # Cache - billing_item_payment_temp
        logger.info("Caching view refund_items_temp")
        spark.sql("CACHE TABLE refund_items_temp")
        record_count = spark.sql("SELECT COUNT(*) as record_count FROM refund_items_temp").collect()[0]
        if record_count['record_count'] == 0: 
            logger.warn("No new records to process")
        else:
            logger.info(f"Cached {record_count['record_count']} records.")
            logger.info(f"""PAYMENTS COUNT: {record_count['record_count']}""")
        
            # PRODUCT HISTORY
            # v_product_history = spark.sql(V_PRODUCT_HISTORY)
            # v_product_history.createOrReplaceTempView("v_product_history")
        
            # Get latest updated_at timestamp - Return type: datetime
            latest_updated_at = spark.sql(LATEST_MERGE_TS_SELECT).collect()[0][STRIPE_REFUND_TABLE_MERGE_TIMESTAMP_COLUMN]
            logger.info(f"Latest timestamp in loaded data is: {latest_updated_at.isoformat()}")
            successful_refund = spark.sql(SUCCESSFUL_REFUND_SELECT_QUERY)
            logger.info(f"SUCCESSFUL_REFUND COUNT: {successful_refund.count()}")
            
            successful_refund.createOrReplaceTempView("success_refund_stage")
            #spark.sql("SELECT * FROM success_refund_stage").show()
            spark.sql(SUCCESSFUL_REFUND_MERGE_QUERY)
            # Update history
            merge_history.update_history(SUCCESSFUL_REFUND_DATASET, job_name, "incremental etl", latest_updated_at.isoformat())
            logger.info("Updated latest merge history timestamp for dataset: {} to {}".format(SUCCESSFUL_REFUND_DATASET, latest_updated_at.isoformat()))
            
except Exception as e:
    logger.error("Error normalizing: {}".format(str(e)))
    raise

# Commit the Glue job
job.commit()

