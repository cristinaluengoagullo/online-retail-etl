from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from config.config import conf
from src.transform import clean_data

# Load data into snowflake
def load_data(df, table_name):
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
