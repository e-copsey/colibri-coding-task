# Orchestration of all the functions here
from utils.init_db import initialise_db, wipe_db
from a_raw.data_landing import land_turbine_data
from b_bronze.ingestion import ingest_turbine_data
from c_silver.data_cleaning import data_cleaning
from d_gold.turbine_summary import generate_turbine_summary
import duckdb


def full_pipeline_run():
    """Orchestrate the full pipeline run."""
    # HELPER TO INITIALISE DB FOR LATER  ANALYTICS
    initialise_db()

    # RAW INGESTION
    land_turbine_data()

    # BRONZE PROCESSING
    ingest_turbine_data()

    # SILVER PROCESSING
    data_cleaning()

    # GOLD PROCESSING
    generate_turbine_summary()
