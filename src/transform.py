from utils.api import get_api_data
from holidays import country_holidays
from datetime import datetime as dt
from config.config import conf
from holidays import country_holidays
import pycountry

# Countries formatted incorrectly in the dataset and their ISO-alpha-2 codes
countries_exceptions = {
    'EIRE': 'IE',
    'Channel Islands': 'GB',
    'USA': 'US',
    'RSA': 'ZA'
}

# National holidays annotation
# df: Pandas dataframe with UCI dataset data from online retail transactions
# returns Pandas dataframe
def transform_data(df):
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


# Alternative to transform_data without using the API (no year limitation).
# Only for demonstration purposes in case we could not use the API
def transform_data_alt(df):
    countries_holidays = {}
    countries_miss = set()

    def is_holiday(row):
        try:
            date = row[0]
            country = row[1]
            country_code = pycountry.countries.get(name=country).alpha_2
            country_id = country_code + str(date.year)
            if not country_id in countries_holidays:
                holidays = country_holidays(country_code, years=2021)
                parsed_holidays = set()
                for holiday_date, holiday_name in holidays.items():
                    parsed_holidays.add(holiday_date)
                countries_holidays[country_id] = parsed_holidays
            date = date.replace(year=int(config['api']['year'])).date()
            print(date)
            return date in countries_holidays[country_id]

        except AttributeError:
            if country not in countries_miss:
                print("No standard code for " + country)
                countries_miss.add(country)
            return False
        except NotImplementedError as e:
            print(e)
            return False

    df['is_national_holiday'] = [is_holiday(row) for row in df.loc[:, ['InvoiceDate', 'Country']].values]
    print(countries_holidays)
    #df['is_national_holiday'] = df.apply(lambda x: is_holiday(x['InvoiceDate'], x['Country']),axis=1)
    return df


# Clean data before loading to db
def clean_data(df):
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
