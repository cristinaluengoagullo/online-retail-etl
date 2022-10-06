from snowflake.connector import connect
from src.load import load_data
from config.config import load_config, conf
import pytest
import pandas as pd

load_config('config/config.yaml')

@pytest.fixture
def df_load() -> pd.DataFrame:
    df_load = pd.read_excel(conf['test']['load'])
    return df_load

def test_success(df_load):
    success, nchunks, nrows, _ = load_data(df_load, conf['test']['table_name'])
    dbCon = connect(
        user = conf['snowflake-db']['user'],
        password = conf['snowflake-db']['password'],
        account = conf['snowflake-db']['account'],
        database = conf['snowflake-db']['database'],
        schema = conf['snowflake-db']['schema'],
        warehouse = conf['snowflake-db']['warehouse']
    )
    sql = "select count(*) from {name} where Is_national_holiday;".format(name=conf['test']['table_name'])
    result = dbCon.cursor().execute(sql).fetchall()
    assert success
    assert nrows == 20
    assert result[0][0] == 5
