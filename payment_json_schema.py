# -*- coding: utf-8 -*-
"""
Created on Sat Feb 24 19:53:17 2024

@author: bat_j
"""

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, LongType, IntegerType, ArrayType, MapType, TimestampType

def payment_json_schema():

    json_schema = StructType([
    StructField("id", StringType(), True),
    StructField("paid", BooleanType(), True),
    StructField("order", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("object", StringType(), True),
    StructField("review", StringType(), True),
    StructField("source", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created", IntegerType(), True),
    StructField("dispute", StringType(), True),
    StructField("invoice", StringType(), True),
    StructField("outcome", StructType([
        StructField("type", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("risk_level", StringType(), True),
        StructField("risk_score", IntegerType(), True),
        StructField("network_status", StringType(), True),
        StructField("seller_message", StringType(), True)
    ]), True),
    StructField("captured", BooleanType(), True),
    StructField("currency", StringType(), True),
    StructField("customer", StringType(), True),
    StructField("disputed", BooleanType(), True),
    StructField("livemode", BooleanType(), True),
    StructField("metadata", StructType([
        StructField("user", StructType([
            StructField("email", StringType(), True),
            StructField("state", StringType(), True),
            StructField("consent", StructType([
                StructField("timestamp", StringType(), True),
                StructField("idempotencyKey", StringType(), True)
            ]), True),
            StructField("lastName", StringType(), True),
            StructField("firstName", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("provider", StructType([
            StructField("fullName", StringType(), True),
            StructField("providerId", StringType(), True)
        ]), True),
        StructField("packageTag", StringType(), True),
        StructField("appointmentSlot", StructType([
            StructField("date", StringType(), True),
            StructField("user", StructType([
                StructField("userId", StringType(), True),
                StructField("full_name", StringType(), True)
            ]), True),
            StructField("user_id", StringType(), True),
            StructField("appointment_id", StringType(), True),
            StructField("is_fully_booked", BooleanType(), True),
            StructField("has_waitlist_enabled", BooleanType(), True)
        ]), True),
        StructField("pettablePackage", StructType([
            StructField("next_steps", StringType(), True),
            StructField("package_id", IntegerType(), True),
            StructField("package_tag", StringType(), True),
            StructField("provider_id", StringType(), True),
            StructField("total_price", IntegerType(), True),
            StructField("package_name", StringType(), True),
            StructField("appointment_type_id", StringType(), True),
            StructField("package_description", StringType(), True),
            StructField("primary_product_tag", StringType(), True),
            StructField("requires_scheduling", BooleanType(), True),
            StructField("healthie_offering_id", StringType(), True)
        ]), True)
    ]), True),
    StructField("refunded", BooleanType(), True),
    StructField("shipping", StringType(), True),
    StructField("application", StringType(), True),
    StructField("description", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("receipt_url", StringType(), True),
    StructField("failure_code", StringType(), True),
    StructField("on_behalf_of", StringType(), True),
    StructField("fraud_details", StringType(), True),  # Empty object, so represented as StringType
    StructField("radar_options", StringType(), True),  # Empty object, so represented as StringType
    StructField("receipt_email", StringType(), True),
    StructField("transfer_data", StringType(), True),  # Empty object, so represented as StringType
    StructField("payment_intent", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("receipt_number", StringType(), True),
    StructField("transfer_group", StringType(), True),
    StructField("amount_captured", IntegerType(), True),
    StructField("amount_refunded", IntegerType(), True),
    StructField("application_fee", StringType(), True),
    StructField("billing_details", StructType([
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StructType([
            StructField("city", StringType(), True),
            StructField("line1", StringType(), True),
            StructField("line2", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postal_code", StringType(), True)
        ]), True)
    ]), True),
    StructField("failure_message", StringType(), True),
    StructField("source_transfer", StringType(), True),
    StructField("balance_transaction", StringType(), True),
    StructField("statement_descriptor", StringType(), True),
    StructField("application_fee_amount", StringType(), True),
    StructField("payment_method_details", StructType([
        StructField("card", StructType([
            StructField("brand", StringType(), True),
            StructField("last4", StringType(), True),
            StructField("checks", StructType([
                StructField("cvc_check", StringType(), True),
                StructField("address_line1_check", StringType(), True),
                StructField("address_postal_code_check", StringType(), True)
            ]), True),
            StructField("wallet", StringType(), True),
            StructField("country", StringType(), True),
            StructField("funding", StringType(), True),
            StructField("mandate", StringType(), True),
            StructField("network", StringType(), True),
            StructField("exp_year", IntegerType(), True),
            StructField("exp_month", IntegerType(), True),
            StructField("fingerprint", StringType(), True),
            StructField("overcapture", StructType([
                StructField("status", StringType(), True),
                StructField("maximum_amount_capturable", IntegerType(), True)
            ]), True),
            StructField("installments", StringType(), True),
            StructField("multicapture", StringType(), True),
            StructField("network_token", StructType([
                StructField("used", BooleanType(), True)
            ]), True),
            StructField("capture_before", IntegerType(), True),
            StructField("three_d_secure", StringType(), True),
            StructField("amount_authorized", IntegerType(), True),
            StructField("extended_authorization", StructType([
                StructField("status", StringType(), True)
            ]), True),
            StructField("incremental_authorization", StructType([
                StructField("status", StringType(), True)
            ]), True)
        ]), True),
        StructField("type", StringType(), True)
    ]), True),
    StructField("failure_balance_transaction", StringType(), True),
    StructField("statement_descriptor_suffix", StringType(), True),
    StructField("calculated_statement_descriptor", StringType(), True)])
    
    return json_schema

def dispute_json_schema():
    
   json_schema = StructType([
    	StructField("id", StringType()),
    	StructField("amount", DoubleType()),
    	StructField("charge", StringType()),
    	StructField("object", StringType()),
    	StructField("reason", StringType()),
    	StructField("status", StringType()),
    	StructField("created", IntegerType()),
    	StructField("currency", StringType()),
    	StructField("evidence", StructType([
    		StructField("receipt", StringType()),
    		StructField("service_date", StringType()),
    		StructField("customer_name", StringType()),
    		StructField("refund_policy", StringType()),
    		StructField("shipping_date", StringType()),
    		StructField("billing_address", StringType()),
    		StructField("shipping_address", StringType()),
    		StructField("shipping_carrier", StringType()),
    		StructField("customer_signature", StringType()),
    		StructField("uncategorized_file", StringType()),
    		StructField("uncategorized_text", StringType()),
    		StructField("access_activity_log", StringType()),
    		StructField("cancellation_policy", StringType()),
    		StructField("duplicate_charge_id", StringType()),
    		StructField("product_description", StringType()),
    		StructField("customer_purchase_ip", StringType()),
    		StructField("cancellation_rebuttal", StringType()),
    		StructField("service_documentation", StringType()),
    		StructField("customer_communication", StringType()),
    		StructField("customer_email_address", StringType()),
    		StructField("shipping_documentation", StringType()),
    		StructField("refund_policy_disclosure", StringType()),
    		StructField("shipping_tracking_number", StringType()),
    		StructField("refund_refusal_explanation", StringType()),
    		StructField("duplicate_charge_explanation", StringType()),
    		StructField("cancellation_policy_disclosure", StringType()),
    		StructField("duplicate_charge_documentation", StringType())])),
    	StructField("livemode", BooleanType()),
    	StructField("metadata", StructType([])),
    	StructField("payment_intent", StringType()),
    	StructField("evidence_details", StructType([
    		StructField("due_by", IntegerType()),
    		StructField("past_due", BooleanType()),
    		StructField("has_evidence", BooleanType()),
    		StructField("submission_count", IntegerType())])),
    	StructField("balance_transaction", StringType()),
    	StructField("balance_transactions", ArrayType(StringType())),
    	StructField("is_charge_refundable", BooleanType()),
    	StructField("payment_method_details", StructType([
    		StructField("card", StructType([
    			StructField("brand", StringType()),
    			StructField("network_reason_code", StringType())])),
    		StructField("type", StringType())]))])
   
   return json_schema

def refund_json_schema():
    
    json_schema = StructType([
        StructField("id", StringType(), True),
        StructField("paid", BooleanType(), True),
        StructField("order", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("object", StringType(), True),
        StructField("review", StringType(), True),
        StructField("source", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created", IntegerType(), True),
        StructField("dispute", StringType(), True),
        StructField("invoice", StringType(), True),
        StructField("outcome", StructType([
            StructField("type", StringType(), True),
            StructField("reason", StringType(), True),
            StructField("risk_level", StringType(), True),
            StructField("risk_score", IntegerType(), True),
            StructField("network_status", StringType(), True),
            StructField("seller_message", StringType(), True)
        ]), True),
        StructField("refunds", StructType([
            StructField("url", StringType(), True),
            StructField("data", StringType(), True),  # StructType is unclear, so represented as StringType
            StructField("object", StringType(), True),
            StructField("has_more", BooleanType(), True),
            StructField("total_count", IntegerType(), True)
        ]), True),
        StructField("captured", BooleanType(), True),
        StructField("currency", StringType(), True),
        StructField("customer", StringType(), True),
        StructField("disputed", BooleanType(), True),
        StructField("livemode", BooleanType(), True),
        StructField("metadata", StringType(), True),  # StructType is unclear, so represented as StringType
        StructField("refunded", BooleanType(), True),
        StructField("shipping", StringType(), True),
        StructField("application", StringType(), True),
        StructField("description", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("receipt_url", StringType(), True),
        StructField("failure_code", StringType(), True),
        StructField("on_behalf_of", StringType(), True),
        StructField("fraud_details", StringType(), True),  # StructType is unclear, so represented as StringType
        StructField("radar_options", StringType(), True),  # StructType is unclear, so represented as StringType
        StructField("receipt_email", StringType(), True),
        StructField("transfer_data", StringType(), True),  # StructType is unclear, so represented as StringType
        StructField("payment_intent", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("receipt_number", StringType(), True),
        StructField("transfer_group", StringType(), True),
        StructField("amount_captured", IntegerType(), True),
        StructField("amount_refunded", IntegerType(), True),
        StructField("application_fee", StringType(), True),
        StructField("billing_details", StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StructType([
                StructField("city", StringType(), True),
                StructField("line1", StringType(), True),
                StructField("line2", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("postal_code", StringType(), True)
            ]), True)
        ]), True),
        StructField("failure_message", StringType(), True),
        StructField("source_transfer", StringType(), True),
        StructField("balance_transaction", StringType(), True),
        StructField("statement_descriptor", StringType(), True),
        StructField("application_fee_amount", StringType(), True),
        StructField("payment_method_details", StructType([
            StructField("card", StructType([
                StructField("brand", StringType(), True),
                StructField("last4", StringType(), True),
                StructField("checks", StructType([
                    StructField("cvc_check", StringType(), True),
                    StructField("address_line1_check", StringType(), True),
                    StructField("address_postal_code_check", StringType(), True)
                ]), True),
                StructField("wallet", StringType(), True),
                StructField("country", StringType(), True),
                StructField("funding", StringType(), True),
                StructField("mandate", StringType(), True),
                StructField("network", StringType(), True),
                StructField("exp_year", IntegerType(), True),
                StructField("exp_month", IntegerType(), True),
                StructField("fingerprint", StringType(), True),
                StructField("overcapture", StructType([
                    StructField("status", StringType(), True),
                    StructField("maximum_amount_capturable", IntegerType(), True)
                ]), True),
                StructField("installments", StringType(), True),
                StructField("multicapture", StringType(), True),
                StructField("network_token", StructType([
                    StructField("used", BooleanType(), True)
                ]), True),
                StructField("capture_before", IntegerType(), True),
                StructField("three_d_secure", StringType(), True),
                StructField("amount_authorized", IntegerType(), True),
                StructField("extended_authorization", StructType([
                    StructField("status", StringType(), True)
                ]), True),
                StructField("incremental_authorization", StructType([
                    StructField("status", StringType(), True)
                ]), True)
            ]), True),
            StructField("type", StringType(), True)
        ]), True),
        StructField("failure_balance_transaction", StringType(), True),
        StructField("statement_descriptor_suffix", StringType(), True),
        StructField("calculated_statement_descriptor", StringType(), True)])
    
    return json_schema