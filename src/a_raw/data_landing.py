from utils.logger import log
from utils.configs import get_confs
from datetime import datetime

@log
def land_turbine_data(ingestion_date=datetime.now().strftime("%d/%m/%Y")):
    """
    This is a dummy function that simulates the ingestion of turbine data to the 'data/raw/' location
    """
    raw_configs = get_confs("a_raw","land_turbine_data")
    
    for i in range(4):
        print(f"INFO: Ingesting turbine data for data group {i} for {ingestion_date} ...")

    return None