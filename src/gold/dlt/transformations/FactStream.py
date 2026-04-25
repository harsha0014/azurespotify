import dlt
from pyspark.sql.functions import col

@dlt.table
def factstream_stg():
    return spark.readStream.table('spotify.silver.factstream')

dlt.create_streaming_table("factstream")

dlt.create_auto_cdc_flow(
    target="factstream",
    source="factstream_stg",
    keys=["stream_id"],
    sequence_by=col("stream_timestamp"),
    stored_as_scd_type=1
)