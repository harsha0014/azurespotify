import dlt
from pyspark.sql.functions import col

@dlt.table
def dimdate_stg():
    return spark.readStream.table('spotify.silver.dimdate')

dlt.create_streaming_table("dimdate")

dlt.create_auto_cdc_flow(
    target="dimdate",
    source="dimdate_stg",
    keys=["date_key"],
    sequence_by=col("date"),
    stored_as_scd_type=2
)