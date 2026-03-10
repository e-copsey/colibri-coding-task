import pandas as pd

def read_files(input_configs:list):
    
    data_dict = {}

    for table_config in input_configs:
        dataframe_name = table_config.get("dataframe_name")
        file_path = table_config.get("file_path")
        file_format = table_config.get("file_format")
        read_options = table_config.get("read_options",{})

        if file_format == 'csv':
            data_dict[dataframe_name] = pd.read_csv(file_path,**read_options)
        elif file_format == 'excel':
            data_dict[dataframe_name] = pd.read_excel(file_path,**read_options)

    return data_dict