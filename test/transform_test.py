from src.transform import transform_data
from config.config import load_config, conf
import pytest
import pandas as pd

load_config('config/config.yaml')

@pytest.fixture
def df_national_holidays() -> pd.DataFrame:
    df_national_holidays = pd.read_excel(conf['test']['transform_national_holidays'])
    return df_national_holidays


@pytest.fixture
def df_regional_holidays() -> pd.DataFrame:
    df_regional_holidays = pd.read_excel(conf['test']['transform_regional_holidays'])
    return df_regional_holidays

@pytest.fixture
def df_execption_countries() -> pd.DataFrame:
    df_execption_countries = pd.read_excel(conf['test']['transform_exception_countries'])
    return df_execption_countries

def test_national_holidays(df_national_holidays):
    df = transform_data(df_national_holidays)
    df = df[df['is_national_holiday']]
    countries = ['United Kingdom', 'United Kingdom', 'Singapore', 'Italy']
    assert df.shape[0] == 4
    assert df['Country'].values.tolist() == countries

def test_regional_holidays(df_regional_holidays) -> pd.DataFrame:
    df = transform_data(df_regional_holidays)
    df = df[df['is_national_holiday']]
    countries = ['United Kingdom', 'Singapore', 'Italy']
    assert df.shape[0] == 3
    assert df['Country'].values.tolist() == countries

def test_exception_countries(df_execption_countries) -> pd.DataFrame:
    df = transform_data(df_execption_countries)
    df = df[df['is_national_holiday']]
    countries = ['United Kingdom', 'Singapore', 'Italy', 'USA', 'RSA']
    assert df.shape[0] == 5
    assert df['Country'].values.tolist() == countries
