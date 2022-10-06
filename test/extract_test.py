from config.config import load_config, conf
from datetime import datetime
import pytest
import pandas as pd
import os;

load_config('config/config.yaml')

@pytest.fixture
def df_extract() -> pd.DataFrame:
    df_extract = pd.read_excel(conf['test']['extract'])
    return df_extract

def test_dataset_empty(df_extract):
    assert not df_extract.empty

def test_cols(df_extract):
    df_schema_columns = ['InvoiceNo',
                         'StockCode',
                         'Description',
                         'Quantity',
                         'InvoiceDate',
                         'UnitPrice',
                         'CustomerID',
                         'Country']
    assert df_extract.columns.tolist() == df_schema_columns

def test_rows(df_extract):
    assert df_extract.shape[0] == 21
