import dlt
from pyspark.sql.functions import col

@dlt.table
def dimuser_stg():
    return spark.readStream.table('spotify.silver.dimuser')

dlt.create_streaming_table("dimuser")

dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2
)