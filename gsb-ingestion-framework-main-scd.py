# Databricks notebook source
# MAGIC %md
# MAGIC ###About
# MAGIC This is notebook creates SCD Type 1 and 2 version of the bronze tables (append only) based on the values registered in the scd_type column in the entity config table. The notebook caters to various source file types i.e., full and delta files.
# MAGIC
# MAGIC The code contains mainly five functions:
# MAGIC * **check_table_exist**: Checks if a table exists in the given catalog and database and returns a True of the table DOES NOT exist.
# MAGIC * **get_att_hash_key**: Generates a string that would be used to generate a has value for the given list of attributes
# MAGIC * **get_config_file**: This function takes source_system_code as input and returns a filtered table of entity_config from the Hive metastore.
# MAGIC * **get_colinfo**: This function takes entity_id as input and returns metadata information about the columns, including their names, datatypes, and data classifications.
# MAGIC * **get_source_file_location**: This function takes source_system and _input_file_name as inputs and returns the file path in the raw-bucket.
# MAGIC * **get_encryption_expression**: This function takes key and colclassficationdict as inputs and returns a list of encrypted columns, if their data classification is Confidential (PII) or Highly Confidential.
# MAGIC * **get_expression**: This function takes colselectlistdict as input and returns a list of selected columns.
# MAGIC * **get_latest_process_date**: This functions takes the catalog, output_table_name and source_system_code as parameter and returns the max process date from the audit table.
# MAGIC
# MAGIC The code also contains a **main** function that takes catalog,database_name,source_system_code and key as input and calls the other functions to process the data and write it to the specified output table in Delta format.

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("source_system_code", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("database_name", "")
dbutils.widgets.text("key", "abcdefghijklmnop")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pyspark.sql.types as T
from delta.tables import *

# COMMAND ----------

ingest_timestamp = datetime.now()
ingest_date = int(ingest_timestamp.strftime("%Y%m%d"))
filename = udf(lambda x: x.rsplit("/", 1)[-1], StringType())
null_date = datetime.strptime("1900-01-01", "%Y-%m-%d")
high_date = datetime.strptime("9999-12-31", "%Y-%m-%d")

# COMMAND ----------

def check_table_exist(catalog, database_name, table_name):
    table_exist = True
    try:
        table_exist = (
            spark.sql(f"show tables in {catalog}.{database_name}_pii")
            .filter(col("tableName") == table_name)
            .isEmpty()
        )
    except:
        pass
    return table_exist


def get_att_hash_key(cols):
    return sha2(
        trim(concat_ws("|", *[coalesce(col(c).cast("string"), lit("")) for c in cols])),
        256,
    )


def get_config_file(source_system_code):
    return spark.table("hive_metastore.config.entity_config").filter(
        (col("is_current_version") == "Y")
        & (col("is_active") == "Y")
        & (col("scd_type") == "scd2")
        & (col("source_system_code") == source_system_code)
    )


def get_colinfo(entity_id):
    config_field = (
        spark.table("hive_metastore.config.field_config")
        .filter((col("entity_id") == entity_id) & (col("is_current_version") == "Y"))
        .orderBy("entity_id", "raw_column_order")
    )
    colinfo = []
    coltypelist = []
    colclassficationdict = {}
    colselectlistdict = {}
    primary_key = []
    att_list = []
    for row in config_field.collect():
        column_name = str(row["raw_column_name"])
        data_classification = str(row["data_classification"])
        colclassficationdict[column_name] = data_classification
        if row["bronze_column_name"] == None:
            bronze_column_name = str(row["raw_column_name"])
        else:
            bronze_column_name = str(row["bronze_column_name"])
        column_name = str(row["raw_column_name"])
        colselectlistdict[column_name] = bronze_column_name
        if row["raw_column_is_nullable"] == "N":
            null_constraint = "not null"
        else:
            null_constraint = ""
        column_name = str(row["raw_column_name"])
        data_type = str(row["raw_column_datatype"])
        coltype = column_name + " " + data_type + " " + null_constraint
        coltypelist.append(coltype)
        bronze_column_is_business_key = row["bronze_column_is_business_key"]
        bronze_column_is_included_in_cdc_hash = row[
            "bronze_column_is_included_in_cdc_hash"
        ]
        if bronze_column_is_business_key == "Y":
            primary_key.append(bronze_column_name)
        if bronze_column_is_included_in_cdc_hash == "Y":
            att_list.append(bronze_column_name)
    return coltypelist, colclassficationdict, colselectlistdict, primary_key, att_list


def get_source_file_location(source_system, _input_file_name):
    return f"/mnt/raw-bucket/{source_system}/{catalog}/IN/{_input_file_name}"


def get_encryption_expression(key, colclassficationdict):
    key = f"'{key}'"
    return [
        column
        if (
            classification != "Confidential (PII)"
            and classification != "Highly Confidential"
        )
        else f"aes_encrypt(cast({column} as string),{key}) as {column}"
        for column, classification in colclassficationdict.items()
    ]


def get_expression(colselectlistdict):
    return [
        f"{column} as {bronze_column}"
        for column, bronze_column in colselectlistdict.items()
    ]


def get_latest_process_date(catalog, output_table_name, source_system_code):
    recon_table_name = "edp_prc_cntrl_reconciliation_audit"
    return (
        spark.table(f"{catalog}.config.{recon_table_name}")
        .filter(
            (col("target_table_name") == output_table_name)
            & (col("is_current_run") == True)
            & (col("source_system_code") == source_system_code)
        )
        .agg(max(col("process_datetime")))
        .first()[0]
    )

# COMMAND ----------

def main(catalog, database_name, source_system_code, key):
    config_file = get_config_file(source_system_code)
    for row in config_file.collect():
        entity_id = row["entity_id"]
        source_system = row["source_system"].lower()
        # source_system_description = row["source_system_description"]
        input_file = row["input_file"].split("_")[0]
        source_file_description = "abcd"
        raw_file_format_type = row["raw_file_format_type"]
        raw_file_data_compression = row["raw_file_data_compression"]
        raw_file_row_sep = row["raw_file_row_sep"]
        raw_file_encryption = row["raw_file_encryption"]
        raw_file_escape = row["raw_file_escape"]
        raw_file_field_delimiter = row["raw_file_field_delimiter"]
        raw_file_field_quote = row["raw_file_field_quote"]
        raw_file_char_to_escape_quote = row["raw_file_char_to_escape_quote"]
        raw_file_header_count = row["raw_file_header_count"]
        header_flag = "true" if raw_file_header_count == 1 else "false"
        raw_file_footer_count = row["raw_file_footer_count"]
        raw_file_encoding = row["raw_file_encoding"]
        raw_file_date_format = row["raw_file_date_format"]
        raw_file_timestamp_format = row["raw_file_timestamp_format"]
        null_format = row["null_format"]
        cdc_enabled = row["cdc_enabled"]
        scd_type = row["scd_type"].lower()
        extract_type = row["extract_type"]
        frequency = row["frequency"]
        output_table_name = row["output_table_name"]
        number_of_files_per_load = row["number_of_files_per_load"]
        expectation = row["expectation"]
        # expectation = "'"+expectation.replace('""','"').replace('"{','{').replace('}"','}').replace('|',',')+"'"
        recon_threshold = row["recon_threshold"]

        source_file_location = get_source_file_location(source_system, input_file)

        colinfo = get_colinfo(entity_id)
        coltypelist = colinfo[0]
        colclassficationdict = colinfo[1]
        colselectlistdict = colinfo[2]
        primary_key = colinfo[3]
        att_list = colinfo[4]
        schema_string = ",".join(coltypelist)
        schema = T._parse_datatype_string(schema_string)
        # key = dbutils.preview.secret.get(scope = "aes_keys", key = "key")
        # key = spark.conf.get("spark.aes_key")
        # key = f"'{key}'"

        encryption_expression = get_encryption_expression(key, colclassficationdict)
        expression = get_expression(colselectlistdict)

        # "skipRows": raw_file_header,
        cloudfile = {
            "cloudFiles.format": raw_file_format_type,
            "header": "true",
            "TriggerOnce": "true",
            "escape": raw_file_char_to_escape_quote,
            "quote": raw_file_field_quote,
            "delimiter": raw_file_field_delimiter,
            "mergeSchema": "true",
        }

        if check_table_exist(catalog, database_name, f"{output_table_name}_{scd_type}_v"):

            control_columns = [
                "Source_File_Path",
                "Source_File_Name",
                "Processed_Date",
                "Load_Datetime",
            ]
            encryption_expression = encryption_expression + control_columns
            expression = expression + control_columns
            source_read_df = spark.table(
                f"{catalog}.{database_name}_pii.{output_table_name}_v"
            )

            latest_process_date = get_latest_process_date(
                catalog, output_table_name, source_system_code
            )

            source_df = (
                source_read_df.filter(col("Processed_Date") == latest_process_date)
                .selectExpr(encryption_expression)
                .selectExpr(expression)
                .withColumn("start_at", col("Processed_Date"))
                .withColumn("end_at", lit(high_date))
                .withColumn("is_current_version", lit(1))
            )

            source_df.write.mode("overwrite").saveAsTable(
                f"{catalog}.{database_name}.{output_table_name}_{scd_type}"
            )

            spark.sql(
                f"UPDATE {catalog}.config.edp_prc_cntrl_reconciliation_audit SET scd_load_datetime = '{latest_process_date}' WHERE is_current_run = True and process_datetime = '{latest_process_date}' and target_table_name = '{output_table_name}' and row_count_check == 'PASS' and summary_check == 'PASS' and daily_rec_count_variance_check == 'PASS'"
            )

        else:
            if extract_type == "full":
                scd_control_columns = [
                    "Source_File_Path",
                    "Source_File_Name",
                    "Processed_Date",
                    "Load_Datetime",
                    "start_at",
                    "end_at",
                    "is_current_version",
                    "pk_concat_key",
                ]
                encryption_expression = encryption_expression + scd_control_columns
                expression = expression + scd_control_columns

                latest_process_date = get_latest_process_date(
                    catalog, output_table_name, source_system_code
                )

                target_df = (
                    spark.table(
                        f"{catalog}.{database_name}_pii.{output_table_name}_{scd_type}_v"
                    )
                    .filter(col("is_current_version") == 1)
                    .withColumn("target_pk_concat_key", concat_ws("|", *primary_key))
                    .withColumn("att_hash_key", get_att_hash_key(att_list))
                )

                sourceread_df = spark.table(
                    f"{catalog}.{database_name}_pii.{output_table_name}_v"
                ).filter(col("Processed_Date") == latest_process_date)

                sourceread_final_df = (
                    sourceread_df
                    .withColumn("start_at", col("Processed_Date"))
                    .withColumn("end_at", lit(high_date))
                    .withColumn("is_current_version", lit(1))
                    .withColumn("pk_concat_key", concat_ws("|", *primary_key))
                    .withColumn("att_hash_key", get_att_hash_key(att_list))
                    .alias("sourceread_final_df")
                )

                delta_df = (
                    sourceread_final_df.join(
                        target_df,
                        target_df.target_pk_concat_key
                        == sourceread_final_df.pk_concat_key,
                        "fullouter",
                    )
                    .withColumn(
                        "delta_type",
                        when(target_df.target_pk_concat_key.isNull(), "I")
                        .when(sourceread_final_df.pk_concat_key.isNull(), "D")
                        .when(
                            (
                                target_df.target_pk_concat_key
                                == sourceread_final_df.pk_concat_key
                            )
                            & (
                                sourceread_final_df.att_hash_key
                                != target_df.att_hash_key
                            ),
                            "U",
                        ),
                    )
                    .withColumn(
                        "pk_concat_key",
                        when(
                            col("delta_type") == "D", target_df.target_pk_concat_key
                        ).otherwise(sourceread_final_df.pk_concat_key),
                    )
                    .select("sourceread_final_df.*", "delta_type", "pk_concat_key")
                    .withColumn(
                        "start_at",
                        when(
                            col("delta_type") == "D", lit(latest_process_date)
                        ).otherwise(sourceread_final_df.start_at),
                    )
                    .filter(col("delta_type").isin("I", "U", "D"))
                )

                updates_df = delta_df.filter(col("delta_type") == "U").withColumn(
                    "pk_concat_key", lit(None)
                )

                source_df = (
                    delta_df.unionAll(updates_df)
                    .alias("source_df")
                    .selectExpr(encryption_expression)
                    .selectExpr(expression)
                )
               
                deltaTable = DeltaTable.forName(
                    spark, f"{catalog}.{database_name}.{output_table_name}_{scd_type}"
                )

                tblMergeKey = "target." + ("||target.".join(primary_key))

                # Merge all the updates, inserts and deletes to the target
                deltaTable.alias("target").merge(
                    source_df.alias("updates"),
                    f"{tblMergeKey} = updates.pk_concat_key and target.is_current_version=1 ",
                ).whenMatchedUpdate(
                    set={"end_at": "updates.start_at", "is_current_version": lit(0)}
                ).whenNotMatchedInsertAll().execute()

                spark.sql(
                    f"UPDATE {catalog}.config.edp_prc_cntrl_reconciliation_audit SET scd_load_datetime = '{latest_process_date}' WHERE is_current_run = True and process_datetime = '{latest_process_date}' and target_table_name = '{output_table_name}' and row_count_check == 'PASS' and summary_check == 'PASS' and daily_rec_count_variance_check == 'PASS'"
                )

            elif extract_type == "delta":
                print("to be developed")

# COMMAND ----------

source_system_code = dbutils.widgets.get("source_system_code")
catalog = dbutils.widgets.get("catalog")
database_name = dbutils.widgets.get("database_name")
key = dbutils.widgets.get("key")

main(catalog, database_name, source_system_code, key)
