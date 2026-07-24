from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Adjust this to your actual staging path
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
    TimestampType,
)


# ==============================================================================
# BRONZE LAYER: RAW DATA INGESTION (NO TRANSFORMATIONS, NO FILTERS)
# ==============================================================================
VOLUME = spark.conf.get("VOLUME").strip()


@dp.table(
    comment="Raw employee data from staging parquet files. Streaming ingestion with Auto Loader. RAW DATA - NO QUALITY FILTERS.",
    # ✅ LIQUID CLUSTERIhttps://dbc-f88938f0-02c4.cloud.databricks.com/editor/files/3086861285192182?contextId=pipeline%3Affbae848-bc88-4c0e-89a3-32768ee1fc79&o=138626662718526$0NG - mejor que partitioning
    cluster_by=["institucion", "fecha_consulta"],  # Auto-optimiza queries por estas columnas
)
def bronze_planilla_contraloria():
    """
    Reads Parquet files from the staging directory.
    This is the entry point for the pipeline.

    BRONZE LAYER PRINCIPLES:
    - NO data quality filters (keep bad data for audit)
    - NO transformations (raw as-is from source)
    - Schema enforcement only

    Quality checks are applied in SILVER layer.
    """
    SCHEMA = StructType(
        [
            StructField("nombre", StringType(), True),
            StructField("apellido", StringType(), True),
            StructField("cedula", StringType(), True),
            StructField("cargo", StringType(), True),
            StructField("salario", DoubleType(), True),
            StructField("gasto", DoubleType(), True),
            StructField("estado", StringType(), True),
            StructField("fecha_de_inicio", DateType(), True),
            StructField("fecha_actualizacion", TimestampType(), True),
            StructField("fecha_consulta", TimestampType(), True),
            StructField("archivo", StringType(), True),
            StructField("institucion", StringType(), True),
        ]
    )

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .schema(SCHEMA)
        .load(f"{VOLUME}/data")
        .withColumns(
            {
                "composite_key": F.concat_ws(
                    "-",
                    F.col("nombre"),
                    F.col("apellido"),
                    F.col("institucion"),
                    F.col("cargo"),
                    F.regexp_replace("cedula", "-", ""),
                ),
                "antiguedad": F.months_between(F.col("fecha_actualizacion"), F.col("fecha_de_inicio")) / 12,
            }
        )
    )


# ==============================================================================
# BRONZE LAYER: SCD TYPE 2 - HISTORICAL CHANGE TRACKING
# ==============================================================================

# Create the target streaming table for SCD Type 2
dp.create_streaming_table(
    name="bronze_planilla_contraloria_scd_type2",
    comment="Employee records with full change history (SCD Type 2). Tracks changes in salary, allowance, and start date.",
    # ✅ LIQUID CLUSTERING para tabla SCD
    # Optimiza queries por: empleado (keys) + rango de fechas
    cluster_by=["cedula", "institucion", "estado"],
)

# Create Auto CDC flow to track changes
dp.create_auto_cdc_flow(
    target="bronze_planilla_contraloria_scd_type2",
    source="bronze_planilla_contraloria",
    keys=[
        "cedula",
        "nombre",
        "apellido",
        "cargo",
        "estado",
    ],  # Primary keys for row identification
    sequence_by="fecha_consulta",  # Column for ordering events (timestamp)
    stored_as_scd_type=2,  # Enable SCD Type 2 - adds __START_AT and __END_AT columns
    track_history_column_list=[
        "salario",
        "gasto",
        "fecha_de_inicio",
    ],  # Columns to track history for
)


# ==============================================================================
# SILVER LAYER: VALIDATED RAW DATA (WITH QUALITY CHECKS)
# ==============================================================================


@dp.materialized_view(
    name="ultima_actualizacion_planilla_contraloria",
    comment="Latest employee snapshot with English translations from reference tables. Optimized with broadcast joins.",
    # ✅ LIQUID CLUSTERING para queries analíticas
    cluster_by=["institution_sp", "status_sp"],
)
def ultima_actualizacion_planilla_contraloria():
    """
    Creates a snapshot of the most recent employee data with English translations.

    OPTIMIZATIONS APPLIED:
    1. ✅ Uses WINDOW FUNCTION instead of collect() for latest timestamp
    2. ✅ Broadcast joins for dimension tables
    3. ✅ Reads from validated silver (not bronze)
    4. ✅ Liquid clustering for analytical queries

    This is a MATERIALIZED VIEW (batch processing) that:
    - Filters for the latest query date
    - Joins with reference tables for translations
    - Adds calculated columns (years_of_service)
    """

    # Read validated employee data
    employees_df = dp.read("bronze_planilla_contraloria")

    # Load reference tables for translations (small dimension tables)
    institutions_df = spark.read.table("dim_instituciones_contraloria")
    statuses_df = spark.read.table("dim_estados_contraloria")
    positions_df = spark.read.table("dim_cargos_contraloria")

    # ✅ Broadcast joins for small dimension tables
    return (
        employees_df.join(
            F.broadcast(institutions_df),
            on=[
                employees_df.institucion == institutions_df.institution_name_spanish,
                employees_df.fecha_consulta == institutions_df.last_source_update,
            ],
            how="inner",
        )
        .join(
            F.broadcast(statuses_df),
            on=[
                employees_df.estado == statuses_df.status_name_spanish,
                employees_df.fecha_consulta == statuses_df.last_source_update,
            ],
            how="inner",
        )
        .join(
            F.broadcast(positions_df),
            employees_df.cargo == positions_df.position_name_spanish,
            "left",
        )
        .select(
            # Translated columns from Spanish to English
            F.col("composite_key"),
            F.col("nombre").alias("first_name"),
            F.col("apellido").alias("last_name"),
            F.col("cedula").alias("id_number"),
            F.col("salario").alias("salary"),
            F.col("gasto").alias("allowance"),
            # Status columns (Spanish and English)
            F.col("estado").alias("status_sp"),
            F.col("status_name_english").alias("status_en"),
            # Institution columns (Spanish and English)
            F.col("institucion").alias("institution_sp"),
            F.col("institution_name_english").alias("institution_en"),
            # Position columns (Spanish and English)
            F.col("cargo").alias("position_sp"),
            F.col("position_name_english").alias("position_en"),
            F.col("fecha_de_inicio").alias("start_date"),
            F.col("fecha_actualizacion").alias("query_date"),
            F.col("fecha_consulta").alias("snapshot_date"),
            F.col("antiguedad").alias("years_of_service"),
            F.col("archivo").alias("file"),
        )
    )


# ==============================================================================
# SILVER LAYER: INACTIVE EMPLOYEES DETECTION (NEW)
# ==============================================================================


@dp.materialized_view(
    name="empleado_inactivo_planilla_contraloria",
    comment="Employees marked as active in SCD but not present in latest snapshot. Indicates terminations or data quality issues.",
)
def empleado_inactivo_planilla_contraloria():
    """
    Identifies employees that are:
    - Marked as ACTIVE in SCD Type 2 (__END_AT IS NULL)
    - BUT not present in the latest snapshot

    This indicates:
    - Employee termination
    - Data quality issues
    - Missing records in latest API pull

    Uses LEFT ANTI JOIN for optimal performance.
    """

    # Active employees in SCD (END_AT IS NULL means current version)
    active_in_scd = (
        spark.read.table("bronze_planilla_contraloria_scd_type2")
        .where("__END_AT IS NULL")
        .select(
            "cedula",
            "nombre",
            "apellido",
            "cargo",
            "estado",
            "institucion",
            "salario",
            "gasto",
            "fecha_de_inicio",
            "__START_AT",
        )
    )

    # Latest snapshot employees
    latest_snapshot = spark.read.table("ultima_actualizacion_planilla_contraloria").select(
        F.col("id_number").alias("cedula"),
        F.col("first_name").alias("nombre"),
        F.col("last_name").alias("apellido"),
        F.col("position_sp").alias("cargo"),
        F.col("status_sp").alias("estado"),
    )

    # ✅ LEFT ANTI JOIN - most efficient way to find non-matching records
    return (
        active_in_scd.join(
            latest_snapshot,
            on=["cedula", "nombre", "apellido", "cargo", "estado"],
            how="left_anti",
        )
        .withColumn("detected_at", F.current_timestamp())
        .select(
            "cedula",
            "nombre",
            "apellido",
            "cargo",
            "estado",
            "institucion",
            "salario",
            "gasto",
            "fecha_de_inicio",
            F.col("__START_AT").alias("last_active_date"),
            "detected_at",
        )
    )


# ==============================================================================
# GOLD LAYER: DATA QUALITY & AGGREGATED ANALYTICS
# ==============================================================================


@dp.materialized_view(
    name="resumen_planilla_por_empleados",
    comment="Per-employee aggregated summary with total salary/allowance, distinct position count, and JSON list of all positions with name variations included.",
    cluster_by=["id_number"],
)
def resumen_planilla_por_empleados():
    """
    Creates an aggregated summary per employee (cedula) from the latest snapshot.

    INCLUDES:
    - Total salary aggregated across all positions
    - Total allowance (gasto) aggregated
    - Count of distinct positions held by the employee
    - First start date (earliest)
    - Last start date (most recent)
    - JSON array with full details per position INCLUDING the name/surname variation used

    HANDLING NAME VARIATIONS:
    - Each position in the JSON includes the first_name and last_name as they appear in the source
    - A cedula may have multiple name spellings (data quality issue from source system)
    - This allows you to see which name variation was used for each specific position

    Use cases:
    - Data quality check: identify employees with multiple positions
    - Financial analysis: total compensation per person
    - Career progression: track position changes
    - Name variation auditing: see which name was used for each position
    """

    employees = dp.read("ultima_actualizacion_planilla_contraloria")

    return (
        employees.groupBy("id_number")
        .agg(
            F.countDistinct("first_name").alias("name_count"),
            F.countDistinct("last_name").alias("last_name_count"),
            # Aggregations: totals and counts
            F.sum("salary").alias("total_salary"),
            F.sum("allowance").alias("total_allowance"),
            F.countDistinct("position_sp").alias("distinct_position_count"),
            # Date range
            F.min("start_date").alias("first_start_date"),
            F.max("start_date").alias("last_start_date"),
            F.max("years_of_service").alias("total_years_of_service"),
            # ✅ JSON array with all position details INCLUDING name variations
            F.to_json(
                F.collect_list(
                    F.struct(
                        F.col("first_name"),  # ✅ Nombre como aparece para este cargo
                        F.col("last_name"),  # ✅ Apellido como aparece para este cargo
                        F.col("status_sp").alias("status"),
                        F.col("institution_sp").alias("institution"),
                        F.col("position_sp").alias("position"),
                        F.col("salary"),
                        F.col("allowance"),
                        F.col("start_date"),
                        F.col("years_of_service"),
                    )
                )
            ).alias("positions_details_json"),
        )
        .select(
            "id_number",
            F.when((F.col("name_count") + F.col("last_name_count")) > 2, F.lit(1)).otherwise(0).alias("multiple_names"),
            "total_salary",
            "total_allowance",
            (F.col("total_salary") + F.col("total_allowance")).alias("total_compensation"),
            "distinct_position_count",
            "first_start_date",
            "last_start_date",
            "total_years_of_service",
            "positions_details_json",
        )
    )


@dp.materialized_view(
    name="resumen_por_institucion_y_puesto",
    comment="Aggregated summary by institution, status, and position with salary, allowance, and tenure metrics.",
    cluster_by=["institution_sp", "status_sp"],
)
def resumen_por_institucion_y_puesto():
    """
    Creates an aggregated summary grouped by institution, status, and position.

    INCLUDES:
    - Total salary for the group
    - Total allowance (gasto) for the group
    - Total compensation (salary + allowance)
    - Employee count in the group
    - First start date (earliest hire in group)
    - Last start date (most recent hire in group)
    - Average years of service
    - Minimum years of service
    - Maximum years of service

    Use cases:
    - Institution-level payroll analysis
    - Position benchmarking across institutions
    - Workforce tenure analysis by role
    - Budget planning and forecasting
    - Organizational structure insights
    """

    employees = dp.read("ultima_actualizacion_planilla_contraloria")

    return (
        employees.groupBy(
            "institution_sp",
            "institution_en",
            "status_sp",
            "status_en",
            "position_sp",
            "position_en",
        )
        .agg(
            # Financial metrics
            F.sum("salary").alias("total_salary"),
            F.mean("salary").alias("avg_salary"),
            F.sum("allowance").alias("total_allowance"),
            F.mean("allowance").alias("avg_allowance"),
            F.sum(F.col("salary") + F.col("allowance")).alias("total_compensation"),
            # Headcount
            F.count("*").alias("employee_count"),
            F.countDistinct("id_number").alias("unique_employee_count"),
            # Date range
            F.min("start_date").alias("first_start_date"),
            F.max("start_date").alias("last_start_date"),
            # Tenure metrics (years of service)
            F.avg("years_of_service").alias("avg_years_of_service"),
            F.min("years_of_service").alias("min_years_of_service"),
            F.max("years_of_service").alias("max_years_of_service"),
        )
        .select(
            "institution_sp",
            "institution_en",
            "status_sp",
            "status_en",
            "position_sp",
            "position_en",
            "total_salary",
            "avg_salary",
            "total_allowance",
            "avg_allowance",
            "total_compensation",
            "employee_count",
            "unique_employee_count",
            "first_start_date",
            "last_start_date",
            "avg_years_of_service",
            "min_years_of_service",
            "max_years_of_service",
        )
    )
