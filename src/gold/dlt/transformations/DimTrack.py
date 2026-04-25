import dlt

@dlt.table
def dimtrack_stg():
    return spark.readStream.table('spotify.silver.dimtrack')

dlt.create_streaming_table("dimtrack")

dlt.create_auto_cdc_flow(
    target="dimtrack",
    source="dimtrack_stg",
    keys=["track_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)