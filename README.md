# colibri-coding-task
# Getting Started
## Install Dependencies
To install dependencies, run the below bash
```bash
pip install -r requirements.txt
```
## Run the Pipeline
Open the interactive notebook
```text
src/pipeline_orchestration.ipynb
```
Execute each cell sequentially to run the pipelines and inspect the outputs. Using similar code to what is present.

# Pipeline Design

The pipeline performs the following operations based on the task requirements:

### 1. Data Ingestion (Bronze Layer)
- Loads raw CSVs containing turbine power data (5 turbines per file, daily updates).
- Maintains incremental ingestion via metadata tables to track processed files.

### 2. Data Cleaning (Silver Layer)
- Handles missing values by imputation.
- Detects and removes outliers (values outside 2 standard deviations).
- Ensures each turbine’s dataset is complete for each 24-hour period.

### 3. Analytics & Summary Statistics (Gold Layer)
- Computes minimum, maximum, and average power output per turbine.
- Identifies anomalous turbines that deviate significantly from expected output.
- Stores cleaned data and calculated statistics in windfarm.duckdb for further analysis.

### 4. Configuration
- Pipeline configurations are stored per layer in config.json files.
- Easily extendable to handle multiple pipelines per layer.

# Assumptions
The following assumptions were made in alignment with the coding task:

- Raw data is delivered daily and never restated.
- Missing records indicate no data for the given time; partial-day data is not possible.
- Each CSV contains data for a fixed group of turbines (e.g., turbine 1 always in data_group_1.csv).
- Outliers are imputed after removal to maintain data continuity.
- The system is expected to scale for additional turbines and extended time periods.

# Repo Structure

```
data/
  a_raw/         # Raw CSV sensor data
  b_bronze/      # Bronze layer: ingested data + metadata
  c_silver/      # Silver layer: cleaned and imputed data
  d_gold/        # Gold layer: analytics and summary statistics
  mock_extra/    # Optional: additional/mock data for testing

db/
  windfarm.duckdb   # Central DuckDB database for processed data

src/
  pipeline_orchestration.py     # Script to run all pipelines
  pipeline_orchestration.ipynb  # Interactive notebook to run and inspect pipelines
  a_raw/                        # Logic to load raw CSVs
  b_bronze/
    config.json                 # Bronze layer pipeline configuration
    transforms.py               # Bronze layer transformation functions
    <pipeline_name>.py          # Orchestrates bronze pipeline processes
  c_silver/                     # Cleaning and imputation logic
  d_gold/                       # Summary statistics and anomaly detection
  utils/                        # Shared utilities (logging, configs, data reading)

tests/                          # Unit tests mirroring src structure
requirements.txt                # Python dependencies
```

## Key Design Decisions
- Incremental & Scalable: Metadata tracking ensures the pipeline can handle daily data without reprocessing previous files.
- Testable & Modular: Each layer has isolated functions for easier testing.
- Clear Separation of Concerns: Bronze handles raw ingestion, Silver handles cleaning, and Gold handles analytics.