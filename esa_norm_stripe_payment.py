"""
This module is to generate normalised data for stripe_payment

Author: Om
"""

from datetime import datetime
import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import StringType
from awsglue.context import GlueContext
from awsglue.job import Job
from merge_utils_v1_0 import *
from payment_json_schema import *
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, when, from_json, from_unixtime
from pyspark.sql.types import TimestampType

# Define and retrieve job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']

logging.basicConfig(level='INFO')
logger = logging.getLogger(job_name)

MERGE_HISTORY_TABLE = "esa-cdc-merge-history"
PUBLIC_DB_NAME = "public"
DWH_DB_NAME = "dwh_stripe"
API_DB_NAME = "api"
STRIPE_DB_NAME = "dwh_stripe"
PAYMENTS_TABLE = "stripe_payment"
REFUNDS_TABLE = "stripe_refund"
SUCCESSFUL_PURCHASE_TABLE = "successful_purchase_norm_stg"
STRIPE_PAYMENTS_MERGE_TIMESTAMP_COLUMN = "created_at"
LATEST_MERGE_TS_SELECT = f"""
SELECT MIN(ts) AS {STRIPE_PAYMENTS_MERGE_TIMESTAMP_COLUMN}
FROM (
    SELECT MAX({STRIPE_PAYMENTS_MERGE_TIMESTAMP_COLUMN}) AS ts FROM billing_item_payment_temp 
    UNION 
    SELECT MAX(created_at) AS ts FROM glue_catalog.{PUBLIC_DB_NAME}.healthie_billing_item_clientside
    UNION 
    SELECT MAX(inserted_at) AS ts FROM glue_catalog.{API_DB_NAME}.referral_factory_event
) A
"""

NOW = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
SPARK_TS_FORMAT = 'yyyy-MM-dd HH:mm:ss'


STATES_MAPPING = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PR": "Puerto Rico",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming"
}

PURCHASE_REFUND_PRODUCT_MERGE = f"""
SELECT
p.payment_intent_id AS external_id,
p.payment_created_at AS created_at,
p.amount,
r.amount_refunded AS refunded_amount,
CASE
    WHEN (p.amount = r.amount_refunded) AND  ((r.payment_refunded_at - p.payment_created_at) <= INTERVAL 24 HOURS) THEN true
    ELSE false
END AS is_fully_refunded_short_term,
p.productTag AS product,
p.productTag AS product_tag,
COALESCE(p.productTag, 'homeRenewal') not like 'homeRenewal%' AS is_full_fare,
COALESCE(p.productTag, '') ilike '%renew%' AS is_renewal,
p.productTag = 'homeRenewalNew' AS is_renewal_new,
COALESCE(p.productTag, 'homeRenewalNew') = 'homeRenewalNew' AS is_other,
CASE
    WHEN ph.category = 'Add-on' THEN true
    ELSE false
END AS is_addon,
p.productTag not ilike 'psdTrain%' AS has_appointment,
p.email,
p.phone,
p.name,
p.state,
ph.operational_costs AS new_operational_costs,
ph.id as product_history_id,
ph.name AS product_name,
p.provider,
p.provider_id,
round(p.amount * 2.9 / 100 + 0.30, 2) AS payment_processing_fees
FROM billing_item_payment_temp p
LEFT JOIN glue_catalog.{STRIPE_DB_NAME}.{REFUNDS_TABLE} r
ON r.payment_intent_id = p.payment_intent_id
LEFT JOIN glue_catalog.{PUBLIC_DB_NAME}.product_history ph
ON ph.tag == p.productTag and ph.starting_at <= p.payment_created_at
"""

SUCCESSFUL_PURCHASE_SELECT_QUERY = f"""
     WITH successful_purchase AS (
        SELECT p.*,
            CASE WHEN (p.is_full_fare is True and r.event_type = 'user_qualified') THEN r.campaign_id ELSE '' END AS campaign_id,
            CASE WHEN (p.is_full_fare is True and r.event_type = 'user_qualified') THEN r.campaign_name ELSE '' END AS campaign_name,
            CASE WHEN (p.is_full_fare is True and r.event_type = 'user_qualified') THEN True ELSE False END AS is_referral_purchase,
            CASE 
                WHEN r.user_email is null THEN 1
                ELSE ROW_NUMBER() over (partition by p.external_id, r.user_email order by (UNIX_TIMESTAMP(r.event_date) - UNIX_TIMESTAMP(p.created_at))) 
            END AS event_rank
        FROM billing_item_payment_temp p
        LEFT JOIN glue_catalog.{API_DB_NAME}.referral_factory_event r
        ON p.email = r.user_email
        AND r.event_date > p.created_at
    )
    SELECT 1 AS q,
        'Stripe' AS payment_processor,
        p.external_id,
        p.created_at,
        p.amount,
        p.refunded_amount,
        p.is_fully_refunded_short_term,
        p.product,
        p.product_tag,
        p.is_full_fare,
        p.is_renewal,
        p.is_renewal_new,
        p.is_other,
        p.is_addon,
        p.has_appointment,
        p.email,
        p.phone AS phone_number,
        p.name,
        p.state,
        CASE
            WHEN NOT p.has_appointment THEN 0
            WHEN p.is_full_fare OR p.is_renewal THEN 38
            WHEN p.is_addon AND p.created_at < DATE '2022-10-31' THEN 18
            WHEN p.is_addon THEN 25
            ELSE 0
        END AS operational_costs,
        p.new_operational_costs,
        p.product_history_id,
        p.product_name,
        p.provider,
        p.provider_id,
        p.payment_processing_fees,
        p.amount - p.payment_processing_fees AS net_amount,
        to_timestamp('{NOW}', '{SPARK_TS_FORMAT}') AS processed_at,
        p.campaign_id,
        p.campaign_name,
        p.is_referral_purchase
    FROM successful_purchase p 
    WHERE p.event_rank == 1
    """

 
SUCCESSFUL_PURCHASE_MERGE_QUERY = f"""
    MERGE INTO glue_catalog.{PUBLIC_DB_NAME}.{SUCCESSFUL_PURCHASE_TABLE} t
    USING ( SELECT 
        q,
        payment_processor,
        external_id,
        created_at,
        amount,
        refunded_amount,
        is_fully_refunded_short_term,
        product,
        product_tag,
        is_full_fare,
        is_renewal,
        is_renewal_new,
        is_other,
        is_addon,
        email,
        phone_number,
        name,
        COALESCE(state, 'NULL') as state,
        operational_costs,
        new_operational_costs,
        product_history_id,
        product_name,
        provider,
        provider_id,
        payment_processing_fees,
        net_amount,
        processed_at,
        campaign_id,
        campaign_name,
        is_referral_purchase,
        stateName(COALESCE(state, 'NULL')) as state_name
    FROM successful_purchase_stage
    ) s
    ON s.external_id = t.external_id AND s.payment_processor = t.payment_processor AND s.created_at = t.created_at AND s.product_tag = t.product_tag
    WHEN MATCHED THEN UPDATE SET
        t.q=s.q,
        t.amount=s.amount,
        t.refunded_amount=s.refunded_amount,
        t.is_fully_refunded_short_term=s.is_fully_refunded_short_term,
        t.product=s.product,
        t.product_tag=s.product_tag,
        t.is_full_fare=s.is_full_fare,
        t.is_renewal=s.is_renewal,
        t.is_renewal_new=s.is_renewal_new,
        t.is_other=s.is_other,
        t.is_addon=s.is_addon,
        t.email=s.email,
        t.phone_number=s.phone_number,
        t.name=s.name,
        t.state=s.state,
        t.operational_costs=s.operational_costs,
        t.new_operational_costs=s.new_operational_costs,
        t.product_history_id=s.product_history_id,
        t.product_name=s.product_name,
        t.provider=s.provider,
        t.provider_id=s.provider_id,
        t.payment_processing_fees=s.payment_processing_fees,
        t.net_amount=s.net_amount,
        t.processed_at=s.processed_at,
        t.campaign_id=s.campaign_id,
        t.campaign_name=s.campaign_name,
        t.is_referral_purchase=s.is_referral_purchase,
        t.state_name = s.state_name
    WHEN NOT MATCHED THEN INSERT
        (q,
        payment_processor,
        id,
        external_id,
        created_at,
        amount,
        refunded_amount,
        is_fully_refunded_short_term,
        product,
        product_tag,
        is_full_fare,
        is_renewal,
        is_renewal_new,
        is_other,
        is_addon,
        email,
        phone_number,
        name,
        state,
        operational_costs,
        new_operational_costs,
        product_history_id,
        product_name,
        provider,
        provider_id,
        payment_processing_fees,
        net_amount,
        processed_at,
        campaign_id,
        campaign_name,
        is_referral_purchase,
        state_name
    )
    VALUES
    (
        s.q,
        s.payment_processor,
        NULL,
        s.external_id,
        s.created_at,
        s.amount,
        s.refunded_amount,
        s.is_fully_refunded_short_term,
        s.product,
        s.product_tag,
        s.is_full_fare,
        s.is_renewal,
        s.is_renewal_new,
        s.is_other,
        s.is_addon,
        s.email,
        s.phone_number,
        s.name,
        s.state,
        s.operational_costs,
        s.new_operational_costs,
        s.product_history_id,
        s.product_name,
        s.provider,
        s.provider_id,
        s.payment_processing_fees,
        s.net_amount,
        s.processed_at,
        s.campaign_id,
        s.campaign_name,
        s.is_referral_purchase,
        s.state_name
    )
    """


merge_history = MergeHistory(MERGE_HISTORY_TABLE)

# Create a GlueContext & sparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.sparkSession
spark.sparkContext.setLogLevel("INFO")
# Set the configuration to handle timestamps without timezone
spark.conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def state_name(st):
    UNKNOWN_STATE = "UNKNOWN"
    state_in = st.replace(" ", "").upper()
    # state_in = st.strip().upper()
    print(STATES_MAPPING)
    # When to attempt conversion
    if len(state_in) == 2:
        full_state_name = STATES_MAPPING.get(state_in)
        if full_state_name is None:
            state_out = UNKNOWN_STATE
        else:
            state_out = full_state_name
    else:
        for abbrev, state_name in STATES_MAPPING.items():
            if state_in == state_name.replace(" ", "").upper():
                state_out = STATES_MAPPING.get(abbrev)
                break
            else:
                state_out = UNKNOWN_STATE
    return state_out


def load_billing_item_payments(latest_merge_ts=None):
    SPARK_TS_FORMAT = 'yyyy-MM-dd HH:mm:ss'
    if latest_merge_ts:
        latest_merge_dt = datetime.fromisoformat(latest_merge_ts)
        merge_ts_str = latest_merge_dt.strftime("%Y-%m-%d %H:%M:%S")
        df = spark.sql(f"""SELECT * FROM glue_catalog.{STRIPE_DB_NAME}.{PAYMENTS_TABLE} WHERE {STRIPE_PAYMENTS_MERGE_TIMESTAMP_COLUMN} > to_timestamp('{merge_ts_str}', '{SPARK_TS_FORMAT}')""")
    else:
        df = spark.sql(f"""SELECT * FROM glue_catalog.{STRIPE_DB_NAME}.{PAYMENTS_TABLE} """)
    return df


# Initialize a Glue job
job = Job(glueContext)
job.init(job_name, args)

# Processing
logger.info(f"""Starting new normalization job for {PUBLIC_DB_NAME}.{SUCCESSFUL_PURCHASE_TABLE}""")
try:
    
    SUCCESSFUL_PURCHASE_DATASET = f"{PUBLIC_DB_NAME}.{SUCCESSFUL_PURCHASE_TABLE}"
    # Get latest merge timestamp
    latest_merge_ts = merge_history.get_latest_merge_ts(SUCCESSFUL_PURCHASE_DATASET, job_name)
    if latest_merge_ts == "NA":
        logger.info(f"Latest merge history for dataset {SUCCESSFUL_PURCHASE_DATASET} not found.")
        # Load all data
        billing_item_payment_temp = load_billing_item_payments()
    else:
        logger.info(f"Latest timestamp retrieved from merge history for dataset: {SUCCESSFUL_PURCHASE_DATASET} as {latest_merge_ts}")
        # Load after latest merge timestamp
        billing_item_payment_temp = load_billing_item_payments(latest_merge_ts=latest_merge_ts)
    
    if billing_item_payment_temp.isEmpty():
        logger.warning("There is no new data available to process since last  merge")
    else:
        # Turn on for Production
        #billing_item_payment_temp.filter("sender.email not ilike '%pettable%' AND sender.email not ilike '%mechanism%'").createOrReplaceTempView("billing_item_payment_temp")
        # Fetch ProductTag
        billing_item_payment_temp = billing_item_payment_temp.withColumn("payment_data",regexp_replace(col("payment_data"), "^'|'$", ""))
        json_schema = payment_json_schema()
        billing_item_payment_temp = billing_item_payment_temp.withColumn("payment_data",from_json(col("payment_data"),schema = json_schema))
        billing_item_payment_temp = billing_item_payment_temp.withColumn("productTag",when(col("payment_data.metadata").isNull() | col("payment_data.metadata.packageTag").isNull(),'').otherwise(col("payment_data.metadata.packageTag")))
        billing_item_payment_temp = billing_item_payment_temp.withColumn("provider",when(col("payment_data.metadata").isNull() | col("payment_data.metadata.provider.fullName").isNull(),'').otherwise(col("payment_data.metadata.provider.fullName")))
        billing_item_payment_temp = billing_item_payment_temp.withColumn("provider_id",when(col("payment_data.metadata").isNull() | col("payment_data.metadata.provider.providerId").isNull(),'').otherwise(col("payment_data.metadata.provider.providerId")))
        billing_item_payment_temp.createOrReplaceTempView("billing_item_payment_temp")
        
        billing_item_payment_temp = spark.sql(PURCHASE_REFUND_PRODUCT_MERGE)
        billing_item_payment_temp.createOrReplaceTempView("billing_item_payment_temp")
        # Cache - billing_item_payment_temp
        logger.info("Caching view billing_item_payment_temp")
        spark.sql("CACHE TABLE billing_item_payment_temp")
        record_count = spark.sql("SELECT COUNT(*) as record_count FROM billing_item_payment_temp").collect()[0]
        logger.info(f"Cached {record_count['record_count']} records.")
        
    
        # Get latest updated_at timestamp - Return type: datetime
        latest_updated_at = spark.sql(LATEST_MERGE_TS_SELECT).collect()[0][STRIPE_PAYMENTS_MERGE_TIMESTAMP_COLUMN]
        logger.info(f"Latest timestamp in loaded data is: {latest_updated_at.isoformat()}")
        successful_purchase = spark.sql(SUCCESSFUL_PURCHASE_SELECT_QUERY)
        successful_purchase.createOrReplaceTempView("successful_purchase_stage")
        spark.udf.register("stateName", state_name, StringType())
        successful_purchase.createOrReplaceTempView("successful_purchase_stage")
        spark.sql(SUCCESSFUL_PURCHASE_MERGE_QUERY)

        # Update history
        merge_history.update_history(SUCCESSFUL_PURCHASE_DATASET, job_name, "incremental etl", latest_updated_at.isoformat())
        logger.info("Updated latest merge history timestamp for dataset: {} to {}".format(SUCCESSFUL_PURCHASE_DATASET, latest_updated_at.isoformat()))
except Exception as e:
    logger.error("Error normalizing stripe_payment: {}".format(str(e)))
    raise

# Commit the Glue job
job.commit()
