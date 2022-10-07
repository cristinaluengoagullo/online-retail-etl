import numpy as np
import pandas as pd
import pycountry

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from utils.utils import get_api_data
from holidays import country_holidays
from datetime import datetime as dt
from config.config import load_config, conf
from holidays import country_holidays
from pandas import read_excel


default_args = {
    'owner': 'airflow',
}

@dag(dag_id = 'etl_pandas', default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def taskflow_etl_pandas():
    @task()
    def extract():
        df = read_excel(conf['data']['url'])
        return df

    @task(multiple_outputs=True)
    def clean_data(df: pd.DataFrame):
            # Explicit cast to avoid wrong types in db
        df = df.astype({"InvoiceNo": str, "StockCode": str, 'Description': str})
        df.columns = map(lambda x: str(x).upper(), df.columns)

        # Timestamp conversion for compatibility with db
        def fix_date_cols(df, tz = 'UTC'):
           cols = df.select_dtypes(include = ['datetime64[ns]']).columns
           for col in cols:
               df[col] = df[col].dt.tz_localize(tz)
           return df

        return fix_date_cols(df)

    @task(multiple_outputs=True)
    def transform(df: pd.DataFrame):
        holidays = {}
        countries_not_found = set()

        # Determines whether a transaction was made on a national holiday
        # row: Dataframe row with columns 'InvoiceDate' and 'Country'
        # returns boolean
        def is_holiday(row):
            date = row[0]
            country_name = row[1]
            # Check country ISO alpha-2 code
            country = pycountry.countries.get(name=country_name)
            if country or country_name in countries_exceptions:
                country_code = country.alpha_2 if country else countries_exceptions[country_name]
                country_id = country_code + str(date.year)
                # Only call the api when there's a new pair year-country
                if not country_id in holidays:
                    params = {
                        'api_key': conf['api']['key'],
                        'country': country_code,
                        'year': conf['api']['year']
                    }
                    response = get_api_data(conf['api']['baseUrl'], params)
                    # Keep api responses for future lookups
                    parsed_holidays = set()
                    for holiday in response['holidays']:
                        # Only national holidays
                        if holiday['public']:
                            parsed_holidays.add(dt.strptime(holiday['date'], "%Y-%m-%d").date())
                    holidays[country_id] = parsed_holidays
                # The free api only allows previous year queries. This is just a fix for demonstration
                # purposes (all dates set to previous year). If we could use the correct year, we
                # could just use date.date() without replacement
                date = date.replace(year=int(conf['api']['year'])).date()
                return date in holidays[country_id]
            else:
                if country_name not in countries_not_found:
                    print("No standard code for " + country_name)
                    countries_not_found.add(country_name)
                return False

        # Apply is_holiday to each row of the 2 column dataset. List comprehension faster than 'apply' method.
        df['IS_NATIONAL_HOLIDAY'] = [is_holiday(row) for row in df.loc[:, ['INVOICEDATE', 'COUNTRY']].values]
        return df

    @task()
    def load(df: pd.DataFrame):
        dbCon = connect(
                user = conf['snowflake-db']['user'],
                password = conf['snowflake-db']['password'],
                account = conf['snowflake-db']['account'],
                database = conf['snowflake-db']['database'],
                schema = conf['snowflake-db']['schema'],
                warehouse = conf['snowflake-db']['warehouse']
            )
        # Create transactions table
        dbCon.cursor().execute("""
          create table {name} (
            InvoiceNo varchar (10) not null primary key,
            StockCode varchar (20) not null,
            Description varchar (255) not null,
            Quantity integer not null,
            InvoiceDate timestamp not null,
            UnitPrice float not null,
            CustomerID integer,
            Country varchar (20) not null,
            Is_national_holiday boolean not null
        );""".format(name=table_name)
        )
        return write_pandas(dbCon, df, table_name=table_name)

    transaction_data = extract()
    cleaned_data = clean_data(transaction_data )
    transformed_data = transform(cleaned_data)
    load(transformed_data)

taskflow_etl_pandas_dag = taskflow_etl_pandas()
