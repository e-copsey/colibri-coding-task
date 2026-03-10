# colibri-coding-task
Git Repo containing the data processing pipeline for the Colibri Digital coding task.

# Assumptions
- Restatements are not possible. If the data is delivered, it will remain at those values.
- If the time window for a data point to be submitted is missed, the record will not be added at a later date.
- Data is loaded daily for the previous day, so there will never be a case where only half a day's data is present.


# Modifications made to data
- I have altered the seconds value of the last timestamp entry per turbine per data file - to show how the ingestion logic updates the metadata table according to it's most recent delivery


```
data/
  a_raw/         # Raw CSV sensor data
  b_bronze/      # Ingestion metadata and processed outputs
  c_silver/      # (Reserved for cleaned data)
  d_gold/        # (Reserved for final analytics)
src/
  pipeline_orchestration.py  # Main pipeline runner
  a_raw/                    # Raw data landing logic
  b_bronze/                 # Bronze layer ingestion and transforms
  c_silver/                 # Silver layer cleaning
  d_gold/                   # Gold layer analytics
  utils/                    # Shared utilities (logging, configs, reading)
requirements.txt            # Python dependencies
tests/                      # (Reserved for unit tests)
```