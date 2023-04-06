import pandas as pd
from snowflake.connector import connect
from snowflake.sqlalchemy import URL
from sqlalchemy import text
from sqlalchemy import create_engine

# Read CSV files
# csv_df = pd.read_csv('file.csv')

# Read Excel files
def load_hdm_mtx(df):
    df = pd.read_excel(df)
    print(f"Initial hdm_mtx {df.shape}")
    df = df.dropna(subset=['NOPHO_NR', 'MTX_INFDATE', 'MTX_INF_START']).drop(columns="Unnamed: 0")
    print(f"Cleaned hdm_mtx {df.shape}")
    
    return df

hdm_mtx_df = load_hdm_mtx('src/data/HDM_MTX_NOPHO_2023.xlsx')

def load_hdm_plasma(df):
    df = pd.read_excel(df)
    print(f"Initial hdm_plasma {df.shape}")    
    df = df.dropna(subset=['NOPHO_NR', 'PL_DATE', 'PL_TIME']).drop(columns="Unnamed: 0")
    print(f"Cleaned hdm_plasma {df.shape}")
    return df

hdm_plasma_df = load_hdm_plasma('/Users/jesper/Projects/MTX/MTX data 2023/src/data/hdm_plasma_clean.xlsx')

# HDM_MTX = pd.read_parquet('/Users/jesper/Projects/MTX/MTX data 2023/src/data/HDM_MTX_2023.parquet')

# set up Snowflake credentials:
SNOWFLAKE_ACCOUNT = 'qpgirsc-cr13281'
SNOWFLAKE_USER = 'FLIGHT505'
SNOWFLAKE_PASSWORD = 'joXfyt-zojwom-behsi2'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'Nordic_HDM_and_Nopho_database'
SNOWFLAKE_SCHEMA = 'PUBLIC'

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


# Create tables in Snowflake and load your DataFrames into those tables:
"""
Assuming you have three DataFrames (csv_df, excel_df, and parquet_df), 
you can create tables and load them into Snowflake as follows:

Replace the ... in the CREATE TABLE statements with appropriate column definitions,
based on your data.

Now your local data has been loaded into Snowflake, 
and you can proceed with creating a Streamlit application to interact with the data.
"""
with engine.connect() as conn:
    # Set the database and schema
    conn.execute("USE DATABASE Nordic_HDM_and_Nopho_database;")
    conn.execute("USE SCHEMA PUBLIC;")
    # Create tables
    conn.execute("""
    CREATE TABLE IF NOT EXISTS hdm_mtx_table (
        NOPHO_NR INTEGER,
        PATNUM INTEGER,
        GUID VARCHAR,
        HOSPITAL INTEGER,
        SEX INTEGER,
        INFNO INTEGER,
        MTX_INFDATE DATE,
        MTX_INF_START TIMESTAMP,
        MTXDO FLOAT,
        MTX_10DO INTEGER,
        MTXDO_1 FLOAT,
        AFVIG INTEGER,
        PREHYD_DATA DATE,
        VOL_PHYD INTEGER,
        WEIGHT FLOAT,
        HEIGHT FLOAT,
        BSA FLOAT,
        HYDR_VOL INTEGER,
        HYDR_INC INTEGER,
        HYDR_INC_DATE DATE,
        HYDR_INC_TIME TIMESTAMP,
        HYDR_INC_VOL INTEGER,
        ALAT_PRE INTEGER,
        PC_PRE INTEGER,
        P_PRE FLOAT,
        HB_PRE FLOAT,
        THR_PRE INTEGER,
        LEU_PRE FLOAT,
        NEU_PRE FLOAT,
        INFECT_PRE INTEGER,
        INFECT_POST INTEGER,
        INF_POST_DATE DATE,
        MP6_CHANGE INTEGER,
        MP6_WHY1 INTEGER,
        MP6_WHY2 INTEGER,
        MP6_WHY3 INTEGER,
        MP6_WHY4 INTEGER,
        MP6_WHY5 INTEGER,
        MP6_WHY6 INTEGER,
        MP6_PRE14 FLOAT,
        MP6_PRE13 FLOAT,
        MP6_PRE12 FLOAT,
        MP6_PRE11 FLOAT,
        MP6_PRE10 FLOAT,
        MP6_PRE9 FLOAT,
        MP6_PRE8 FLOAT,
        MP6_PRE7 FLOAT,
        MP6_PRE6 FLOAT,
        MP6_PRE5 FLOAT,
        MP6_PRE4 FLOAT,
        MP6_PRE3 FLOAT,
        MP6_PRE2 FLOAT,
        MP6_PRE1 FLOAT,
        MP6_DOSE0 FLOAT,
        MP6_POST1 FLOAT,
        MP6_POST2 FLOAT,
        MP6_POST3 FLOAT,
        MP6_POST4 FLOAT,
        MP6_POST5 FLOAT,
        MP6_POST6 FLOAT,
        MP6_POST7 FLOAT,
        MP6_POST8 FLOAT,
        MP6_POST9 FLOAT,
        MP6_POST10 FLOAT,
        MP6_POST11 FLOAT,
        MP6_POST12 FLOAT,
        MP6_POST13 FLOAT,
        MP6_POST14 FLOAT,
        MP6_POST15 FLOAT,
        MP6_POST16 FLOAT,
        MP6_POST17 FLOAT,
        MP6_POST18 FLOAT,
        MP6_POST19 FLOAT,
        MP6_POST20 FLOAT,
        MP6_POST21 FLOAT,
        MP6_POST_STOP INTEGER,
        MP6_POST_WHY1 INTEGER,
        MP6_POST_WHY2 INTEGER,
        MP6_POST_WHY3 INTEGER,
        MP6_POST_WHY4 INTEGER,
        MP6_POST_WHY5 INTEGER,
        SULFO INTEGER,
        INTRA_DATE DATE,
        INTRA_TIME TIMESTAMP,
        INTRA_TYPE INTEGER,
        INTRA_MTXDO FLOAT,
        FOLIN INTEGER,
        FOLIN42 FLOAT,
        FOLIN48 FLOAT,
        FOLIN54 FLOAT,
        FOLIN60 FLOAT,
        FOLIN66 FLOAT,
        FOLIN72 FLOAT,
        FOLIN78 FLOAT,
        FOLIN84 FLOAT,
        FOLIN90 FLOAT,
        FOLIN96 FLOAT,
        FOLIN102 FLOAT,
        FOLIN108 FLOAT,
        FOLIN114 FLOAT,
        FOLIN120 FLOAT,
        FOLIN126 FLOAT,
        FOLIN132 FLOAT,
        FOLIN138 FLOAT,
        ALAT_POST INTEGER,
        P_POST FLOAT,
        HB_POST FLOAT,
        THR_POST INTEGER,
        LEU_POST FLOAT,
        NEU_POST FLOAT,
        TRANSER INTEGER,
        TRANSTH INTEGER,
        TRANSCNS INTEGER,
        TRANSTYP VARCHAR,
        TRANSTDA TIMESTAMP,
        REMARK_H VARCHAR,
        M_ENTERED DATE,
        M_CHANGE DATE
    );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS hdm_plasma_table (
            NOPHO_NR INTEGER,
            PATNUM INTEGER,
            GUID VARCHAR,
            PATNUMMTX INTEGER,
            CPR INTEGER,
            INFNO INTEGER,
            HOSPITAL INTEGER,
            LNAME VARCHAR,
            FNAME FLOAT,
            PL_DATE DATE,
            PL_TIME TIMESTAMP,
            PL_PC FLOAT,
            PL_MTX FLOAT,
            P_ENTERED DATE,
            P_CHANGE DATE
        );
    """)


    #conn.execute('CREATE TABLE IF NOT EXISTS excel_table (...);')   # Replace (...) with column definitions
    #conn.execute('CREATE TABLE IF NOT EXISTS parquet_table (...);') # Replace (...) with column definitions

    # Load data into tables
    #csv_df.to_sql('csv_table', conn, if_exists='append', index=False)
    #excel_df.to_sql('excel_table', conn, if_exists='append', index=False)
    hdm_mtx_df.to_sql('hdm_mtx_table', conn, if_exists='append', index=False, chunksize=1000)
    hdm_plasma_df.to_sql('hdm_plasma_table', conn, if_exists='append', index=False, chunksize=1000)
