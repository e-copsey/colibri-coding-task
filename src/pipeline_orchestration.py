# Orchestration of all the functions here
from utils.init_db import initialise_db, wipe_db
from a_raw.data_landing import land_turbine_data
from b_bronze.ingestion import ingest_turbine_data
from c_silver.data_cleaning import data_cleaning
from d_gold.turbine_summary import generate_turbine_summary
import duckdb

# import os
# print(os.path.abspath("db/windfarm.duckdb"))

# HELPERS TO RESET THE RUN
# wipe_db(con,wipe_data=True, wipe_layer="all")
# wipe_db(wipe_data=False, wipe_layer="all")
# con = duckdb.connect("C:\\dev\\colibri-coding-task\\db\\windfarm.duckdb")
initialise_db()


# RAW INGESTION
# land_turbine_data()

# BRONZE PROCESSING
# ingest_turbine_data()

# SILVER PROCESSING
# data_cleaning()

# GOLD PROCESSING
generate_turbine_summary()