# Orchestration of all the functions here

from a_raw.data_landing import land_turbine_data
from b_bronze.ingestion import ingest_turbine_data

# RAW INGESTION
land_turbine_data()

# BRONZE PROCESSING
ingest_turbine_data()

# SILVER PROCESSING


# GOLD PROCESSING