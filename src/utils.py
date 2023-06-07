

# download button for dataframes in streamlit
import base64
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect
from typing import Tuple





@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

def login_credentials(engine=None) -> Tuple[Engine, Inspector]:

    inspector = None
    if engine is None:
        with st.form(key='login_form'):
            account = st.text_input("Account", st.secrets["snowflake"]["account"], key="snowflake_login_account")
            user = st.text_input("User", st.secrets["snowflake"]["user"], key="snowflake_login_user")
            password = st.text_input("Password", st.secrets["snowflake"]["password"], key="snowflake_login_password", type="password")
            role = st.text_input("Role", st.secrets["snowflake"]["role"], key="snowflake_login_role")
            warehouse = st.text_input("Warehouse", st.secrets["snowflake"]["warehouse"], key="snowflake_login_warehouse")
            database = st.text_input("Database", st.secrets["snowflake"]["database"], key="snowflake_login_database")
            schema = st.text_input("Schema", st.secrets["snowflake"]["schema"], key="snowflake_login_schema")
            
            create_engine_button = st.form_submit_button(label='Create engine')

            if create_engine_button:
                try:
                    engine = create_engine(
                        f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}')
                    inspector = inspect(engine)
                    st.session_state['is_ready'] = True
                    st.session_state['engine'] = engine
                    st.session_state['schema'] = schema  # Add this line
                except Exception as e:
                    st.error(f'Could not create an engine to connect to the database. Error: {e}')
                    st.stop()

    return engine, inspector