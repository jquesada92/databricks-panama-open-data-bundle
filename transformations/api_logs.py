from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Target streaming table for latest API check logs

dp.create_streaming_table(name="ultima_actualizacion_contraloria")

# CDC flow to maintain only the latest record per institution and status
dp.create_auto_cdc_flow(
    target="ultima_actualizacion_contraloria",
    source="control_de_actualizaciones_contraloria",
    keys=["institution_name_spanish", "status_name_spanish"],
    sequence_by="checked_at",
    stored_as_scd_type=1,  # SCD Type 1: mantiene solo el último valor
)


# Reference table with unique institution names, English translations, and last update info
@dp.table(name="dim_instituciones_contraloria")
def reference_institution_names():
    # Get unique institution names with last source_update and checked_at
    institution_stats = (
        spark.readStream.table("control_de_actualizaciones_contraloria")
        .where('run_status = "OK"')
        .groupBy("institution_name_spanish")
        .agg(F.max("source_update").alias("last_source_update"), F.max("checked_at").alias("last_checked_at"))
    )

    # Add English translation using Databricks AI translate function
    return institution_stats.withColumn(
        "institution_name_english", F.expr("ai_translate(institution_name_spanish, 'en')")
    )


# Reference table with unique status names, English translations, and last update info
@dp.table(name="dim_estados_contraloria")
def reference_status_names():
    # Get unique status names with last source_update and checked_at
    status_stats = (
        spark.readStream.table("control_de_actualizaciones_contraloria")
        .where('run_status = "OK"')
        .groupBy("status_name_spanish")
        .agg(F.max("source_update").alias("last_source_update"), F.max("checked_at").alias("last_checked_at"))
    )

    # Add English translation using Databricks AI translate function
    return status_stats.withColumn("status_name_english", F.expr("ai_translate(status_name_spanish, 'en')"))


# Reference table for position names with translations (streaming table)
@dp.table(name="dim_cargos_contraloria")
def reference_position_names():
    """
    Streaming table with unique position names and English translations.
    Uses conditional anti-join to avoid reprocessing existing positions.
    """
    # Read unique positions from bronze (streaming)
    new_positions = (
        spark.readStream.table("bronze_planilla_contraloria")
        .select(F.col("cargo").alias("position_name_spanish"), F.col("fecha_actualizacion").alias("last_seen_at"))
        .filter(F.col("cargo").isNotNull())
        .dropDuplicates(["position_name_spanish"])
    )

    # Conditional anti-join: only filter if target table exists and has data
    try:
        # Attempt to read existing positions (batch read from same table)
        existing = spark.read.table("dim_cargos_contraloria")

        # Check if table has data
        if existing.count() > 0:
            # Table exists and has data - filter out existing positions
            only_new = new_positions.join(
                existing.select("position_name_spanish"), "position_name_spanish", "left_anti"
            )
        else:
            # Table exists but is empty - process all positions
            only_new = new_positions
    except:
        # Table doesn't exist yet - process all positions (first run)
        only_new = new_positions

    # Translate positions
    return only_new.withColumn("position_name_english", F.expr("ai_translate(position_name_spanish, 'en')"))
