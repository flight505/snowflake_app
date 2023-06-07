import re
import pandas as pd
from snowflake.connector import connect
from snowflake.sqlalchemy import URL
from sqlalchemy import text
from sqlalchemy import create_engine

# Read CSV files
# csv_df = pd.read_csv( file.csv )

# Read Excel files
def load_hdm_mtx(df):
    df = pd.read_excel(df)
    print(f"Initial hdm_mtx {df.shape}")
    df = df.dropna(subset=['NOPHO_NR', 'MTX_INFDATE', 'MTX_INF_START'])
    df.columns = df.columns.str.lower()
    print(f"Cleaned hdm_mtx {df.shape}")

    return df

hdm_mtx_df = load_hdm_mtx("/Users/jesper/Projects/MTX/MTX data 2023/src/HDM_MTX_2023_modified.xlsx")

def load_hdm_plasma(df):
    df = pd.read_excel(df)
    print(f"Initial hdm_plasma {df.shape}")
    df = df.dropna(subset=['NOPHO_NR', 'PL_DATE', 'PL_TIME'])
    df.columns = df.columns.str.lower()

    print(f"Cleaned hdm_plasma {df.shape}")
    return df

hdm_plasma_df = load_hdm_plasma("/Users/jesper/Projects/MTX/MTX data 2023/src/HDM_PLASMA_2023_modified.xlsx")


def load_nopho_data(df):
    df = pd.read_csv(df)
    print(f"Initial nopho clinical data {df.shape}")
    df = df.dropna(subset=['nophonr'])
    df = df.dropna(axis=1, how="all")
    df.columns = df.columns.str.lower()
    print(f"Cleaned nopho clinical data {df.shape}")
    return df

nopho_df = load_nopho_data("/Users/jesper/Projects/MTX/MTX data 2023/ALL_2008_data_cleaned.csv")


# set up Snowflake credentials:
SNOWFLAKE_ACCOUNT =  'qpgirsc-cr13281'
SNOWFLAKE_USER =  'FLIGHT505'
SNOWFLAKE_PASSWORD =  'joXfyt-zojwom-behsi2'
SNOWFLAKE_ROLE =  'ACCOUNTADMIN'
SNOWFLAKE_WAREHOUSE =  'COMPUTE_WH'
SNOWFLAKE_DATABASE =  'Nordic_HDM_and_Nopho_database'
SNOWFLAKE_SCHEMA =  'PUBLIC'

engine = create_engine(URL(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
))


# Test the connection:
try:
    with engine.connect() as conn:
        # Execute a simple query
        result = conn.execute(text("SELECT current_version()"))

        # Fetch the result
        version = result.fetchone()[0]

        print(f"Snowflake connection established! Snowflake version: {version}")
except Exception as e:
    print(f"Error connecting to Snowflake: {e}")

with engine.connect() as conn:
    # Set the database and schema
    conn.execute("USE DATABASE Nordic_HDM_and_Nopho_database;")
    conn.execute("USE SCHEMA PUBLIC;")

    def create_table_with_dtype_text(df, table_name):
        # Define a mapping of data types
        if table_name == 'hdm_mtx_table':
            col = 'remark_h'
        else:
            col = None

        data_type_map = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'datetime64\[ns\]': 'TIMESTAMP',
            'object': 'VARCHAR(10000)' if col == 'remark_h' else 'VARCHAR(255)'
        }

        text_rows = []
        # Replace data types and reduce whitespace
        for col, dtype in df.dtypes.items():
            for key, value in data_type_map.items():
                dtype = re.sub(rf'^{key}$', value, str(dtype))
            text_rows.append(f"{col} {dtype}")

        # Create a string of the formatted rows
        rows_str = ",\n    ".join(text_rows)

        # Create the table if it does not exist
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {rows_str}
            );
        """)

        # Load data into tables
        df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=1000)

        if conn.dialect.has_table(conn, table_name):
            print(f"Table {table_name} updated/created successfully.")
        else:
            print(f"Table {table_name} failed to update/create.")

    # Create tables and load data
    create_table_with_dtype_text(hdm_mtx_df, 'hdm_mtx_table')
    create_table_with_dtype_text(hdm_plasma_df, 'hdm_plasma_table')
    create_table_with_dtype_text(nopho_df, 'nopho_all2008_table')

    print("Data loading complete!")