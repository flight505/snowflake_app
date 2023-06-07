#!/usr/bin/env python3
# imports
import base64
import json
import math
from datetime import date
import re
from typing import List
import openai
import altair as alt
import numpy as np
import pandas as pd
import snowflake.connector as sf
import streamlit as st

from st_pages import Page, add_page_title, show_pages

import toml
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import inspect


from tenacity import retry, stop_after_attempt, wait_exponential

from src.constants import *
from src.utils import (
    convert_df,
    login_credentials,
)
from src.datasets import (
    fetch_hdm_mtx_data,
    fetch_hdm_plasma_data,
    load_infusion_times,
    manipulate_hdm_mtx,
    manipulate_hdm_plasma,
    melt_manipulate_hdm_plasma,
)
from src.diagnostics import (
    DiagnoseTypes,
    DiagnosticClasses,
    init_diagnostics,
    run_diagnostics,
)
from src.processing import (
    create_time_intervals,
    load_blood_samples_by_treatment,
    preview_sample,
)
from src.visualization import (
    beta_visualize_dme,
    visualize_detected,
    visualize_detected_by_patient,
    visualize_patient,
    visualize_summary_detection,
)



# 2. Constants
openai.api_key = st.secrets['OPENAI_API_KEY']["key"]


# Specify what pages should be shown in the sidebar, and what their titles and icons
# should be
show_pages(
    [
        Page("snowflake_app.py", "Nordic HDM database query", "💾"),
        Page("src/diagnosis_page2.py", "Diagnosis", "🩺"),
    ]
)
# Adds the title and icon to the current page
add_page_title(page_title="Nordic HDM database",
                page_icon="💊",
                  layout="wide"
                  )


db_type = 'custom Snowflake'
db_host = st.secrets["snowflake"]["account"]
# db_port = st.secrets["snowflake"]["port"]
db_user = st.secrets["snowflake"]["user"]
db_password = st.secrets["snowflake"]["password"]
db_name = st.secrets["snowflake"]["database"]
db_schema = st.secrets["snowflake"]["schema"]
db_warehouse = st.secrets["snowflake"]["warehouse"]
db_role = st.secrets["snowflake"]["role"]
    
def main(engine):
    if 'is_ready' not in st.session_state:
        st.session_state['is_ready'] = False

    if 'engine' not in st.session_state:
        st.session_state['engine'] = engine

    if 'inspector' not in st.session_state:
        st.session_state['inspector'] = None

    with st.sidebar:
        st.sidebar.title("Navigation")
        with st.expander("Database login"):
            if st.session_state['engine'] is None:
                st.session_state['engine'], inspector = login_credentials()
                if st.session_state['engine'] is not None:
                    st.session_state['is_ready'] = True
                    st.session_state['inspector'] = inspector
                st.divider()
        if st.session_state['engine'] is not None and st.session_state['is_ready'] is True:
            st.write("🟢 Connected to database")
        else:
            st.write("🔴 Not connected to database")
            st.stop()

    if st.session_state.get('is_ready', False):
        inspector = st.session_state['inspector']
        
        
    st.session_state

def execute_query(code, engine):
    try:
        df = pd.read_sql_query(sql=text(code), con=engine.connect())
        st.write("## The results")
        st.write(df)
    except Exception as e:
        st.error(f"An error occurred while executing the SQL query: {e}")
        return False
    return True

# load txt file with column descriptions for nopho_all2008_table only
with open("column_descriptions_hdm_mtx_table.txt", "r") as f:
    column_descriptions = json.load(f)

def process_text_in_chunks(text, openai_func, max_chunk_size=4096):
    chunks = [text[i:i + max_chunk_size] for i in range(0, len(text), max_chunk_size)]
    results = []

    for chunk in chunks:
        result = openai_func(chunk)
        results.append(result)

    return results

def message_probability(message):
    logits = np.array(message["logits"])
    probabilities = np.exp(logits) / np.sum(np.exp(logits))
    return probabilities.prod()
    
def generate_summarized_schema(table_names, column_names, column_descriptions):
    doc = f"Total Tables: {len(table_names)}\n"
    for table_name in table_names:
        doc += f"Table: {table_name}\nColumns:\n"
        for column_name in column_names[table_name]:
            description = column_descriptions.get(table_name, {}).get(column_name, {}).get("description", "")
            doc += f"{column_name} ({description})"
            details = column_descriptions.get(table_name, {}).get(column_name, {}).get("details", None)
            if details:
                doc += " ["
                for key, value in details.items():
                    doc += f"{key}: {value}, "
                doc = doc.strip(", ") + "]"
            doc += ", "
        doc = doc.strip(", ") + "\n"
    
    # Split the doc into smaller pieces if it's too long
    max_token_length = 3500  # Adjust this value based on your use case
    split_docs = [doc[i:i + max_token_length] for i in range(0, len(doc), max_token_length)]

    return split_docs


def generate_and_execute_query(user_request, engine):  # use the engine from the session state
    engine = st.session_state.get('engine')
    if engine is not None:
        inspector = inspect(engine)
        table_names = inspector.get_table_names(schema=st.session_state['schema'])
    else:
        st.error('Could not connect to the database.')
        st.stop()

    # create an empty dictionary to store the column names for each table
    column_names = {}

    # iterate through each table and get its column names
    for table_name in table_names:
        columns = inspector.get_columns(table_name)
        column_names[table_name] = [column['name'] for column in columns]

    # form an introspection doc
    doc = f"""
        AUTO-GENERATED DATABASE SCHEMA DOCUMENTATION\n
        Total Tables: {len(table_names)}
        """
    for table_name in table_names:
        doc += f"""
        ---
        Table Name: {table_name}\n
        Columns:
        """
        for column_name in column_names[table_name]:
            if table_name == 'nopho_all2008_table':
                description = column_descriptions.get(table_name, {}).get(column_name, {}).get("description", "")
                doc += f"{column_name} ({description})"
                details = column_descriptions.get(column_name, {}).get("details", None)
                if details:
                    doc += " ["
                    for key, value in details.items():
                        doc += f"{key}: {value}, "
                    doc = doc.strip(", ") + "]"
                doc += ", "
            else:
                doc += ""

        if table_name == 'nopho_all2008_table':
            doc += "\n"

    # Create a summarized schema
    summarized_doc = generate_summarized_schema(table_names, column_names, column_descriptions)

    intro = 'You are a helpful assistant. Your task is to complete a SQL query and provide the results.'
    prompt_base = 'Write a SQL query for the following request: '
    prompt_base += f'{user_request}.\n'
    prompt_base += 'Follow these guidelines:\n'
    prompt_base += '- Provide only SQL code, without comments or explanations.\n'
    prompt_base += '- Always use table names in column references to avoid ambiguity.\n'
    prompt_base += '- Use the Snowflake SQL dialect.\n'
    prompt_base += '- Only use columns and tables mentioned in the doc below.\n'

    # Send the request to the GPT-3.5-turbo model in a loop for each summarized_doc
    responses = []
    for doc in summarized_doc:
        prompt = prompt_base + f'Documentation:\n{doc}\n'
        prompt += 'Note: If the token size exceeds 4096, you made a mistake. Please reiterate the prompt.'

        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": intro},
                {"role": "user", "content": prompt}
            ]
        )
        choice = response['choices'][0]
        responses.append((choice["message"]["content"], choice["finish_reason"]))

    # Choose the response with the highest probability
    code = max(responses, key=lambda x: x[1])[0]
    pretty_code = '```sql\n' + code + '\n```'
    code = code.replace('\n', ' ')

    with st.expander("See executed code"):
        st.write(pretty_code)
    with st.expander("See introspected BD structure"):
        st.write(doc)

    # Check for placeholders in the generated SQL query
    placeholders = re.findall(r'\[.*?\]', code)
    if placeholders:
        st.warning("The generated SQL query contains placeholders. Please enter the required values.")
        for placeholder in placeholders:
            value = st.text_input(f"Enter the value for {placeholder}:")
            code = code.replace(placeholder, value)

    return execute_query(code, engine)

with st.form(key='my_form_to_submit'):
    user_request = st.text_area("Natural language SQL query")
    if submit_button := st.form_submit_button(label='Submit'):
        engine = st.session_state.get('engine')
        success = generate_and_execute_query(user_request, engine)
        if not success:
            st.write("Please submit a new query.")

if __name__ == "__main__":
    engine = None
    main(engine)
