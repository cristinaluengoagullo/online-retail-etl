
from src.transform import transform_data, clean_data
from src.load import load_data
from config.config import load_config, conf
from pandas import read_excel
import yaml

if __name__ == "__main__":
    load_config('config/config.yaml')
    # Extract data from UCI repository
    df = read_excel(conf['data']['url'])
    # Annotate national holidays
    df = transform_data(df)
    # Load data into Snowflake db
    load_data(df,conf['snowflake-db']['table_name'])
