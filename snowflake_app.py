#!/usr/bin/env python3

import base64
import math
from datetime import date
from typing import List

import altair as alt
import numpy as np
import pandas as pd
import snowflake.connector as sf
import streamlit as st
import toml
from tenacity import retry, stop_after_attempt, wait_exponential



from src.constants import *

from src.utils import (
    convert_df
)

from src.datasets import (
    load_infusion_times,
    manipulate_hdm_mtx,
    manipulate_hdm_plasma,
    melt_manipulate_hdm_plasma,
    merge_blood_samples_to_treatment,
    remove_patients_with_duplicate_treatments,
)
from src.diagnostics import (
    DiagnoseTypes,
    DiagnosticClasses,
    init_diagnostics,
    run_diagnostics,
)
from src.processing import (
    create_time_intervals,
    custom_sort,
    fetch_hdm_mtx_data,
    fetch_hdm_plasma_data,
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


def initialize_app_info():
    """Write Streamlit main panel and sidebar titles + tab info"""
    st.set_page_config(
        page_title="HDM database", page_icon="bar_chart", layout="wide"
    )
    st.subheader("Nordic HDM and Nopho database")

@retry(wait=wait_exponential(multiplier=1, min=2, max=60), stop=stop_after_attempt(5))
def connect_to_db(accnt, usr, pwd, rl, wh, db, sch):
    """Connect to Snowflake database and return cursor"""
    ctx = sf.connect(
        account=accnt,
        user=usr,
        password=pwd,
        role=rl,
        warehouse=wh,
        database=db,
        schema=sch,
    )
    cs = ctx.cursor()
    st.session_state['show_conn'] = cs
    st.session_state['is_ready'] = True
    return cs


def main():
    # Register your pages
    pages = {
        "Database frontend": page_first,
        "Database backend": page_second,
    }
    

    st.sidebar.title("Navigation")


    # Widget to select your page, radio buttons
    page = st.sidebar.radio("Select your page", tuple(pages.keys()))
    st.sidebar.write("---")
    pages[page]()


def page_first():

    with st.sidebar:
        with st.expander("Database login"):
            login_credentials()
    if 'is_ready' not in st.session_state:
        st.session_state['is_ready'] = False
    
    # when ready, show data, filter etc
    if st.session_state['is_ready'] is True:
        with st.sidebar:
            st.write("🟢 Connected to database")
            st.write("---")

        # Fetch data from Snowflake
        hdm_plasma_data = fetch_hdm_plasma_data(st.session_state['show_conn'])
        hdm_mtx_data = fetch_hdm_mtx_data(st.session_state['show_conn'])
    
        # st.dataframe(hdm_plasma_data)
        # st.dataframe(hdm_mtx_data)
        hdm_plasma = manipulate_hdm_plasma(hdm_plasma_data)
        hdm_mtx = manipulate_hdm_mtx(hdm_mtx_data)
        infusion_times = load_infusion_times(hdm_mtx_data)
        melt_hdm_plasma = melt_manipulate_hdm_plasma(hdm_plasma)
        # st.write("HDM Plasma")
        # st.dataframe(hdm_plasma)
        with st.expander("HDM Plasma"):
            st.dataframe(hdm_plasma)
        with st.expander("HDM Mtx"):
            st.dataframe(hdm_mtx)
        with st.expander("Infusion times"):
            st.dataframe(infusion_times)
        with st.expander("Melted HDM Plasma"):
            st.dataframe(melt_hdm_plasma)


        def create_time_intervals_df(hdm_mtx, hdm_plasma, infusion_times):
            time_intervals = create_time_intervals(hdm_plasma, infusion_times)
            merged_df = (
                pd.merge(hdm_mtx, time_intervals, on="NOPHO_NR", how="left")
                .drop(columns=['INFNO_x'])
                .rename(columns={'INFNO_y': 'INFNO'})
            )
            return merged_df

        pivot_hdm_plasma = create_time_intervals_df(hdm_mtx, hdm_plasma, infusion_times)
        
        with st.expander("Time intervals - pivot_hdm_plasma"):
            st.dataframe(pivot_hdm_plasma)
            # Create the download link for the merged_df DataFrame
            pivot_hdm_plasma_download_link = convert_df(pivot_hdm_plasma)
            st.download_button(
                label="Download pivot_hdm_plasma as CSV",
                data=pivot_hdm_plasma_download_link,
                file_name='pivot_hdm_plasma.csv',
                mime='text/csv',
            )
       
        samples_with_treatment_no = load_blood_samples_by_treatment(melt_hdm_plasma, infusion_times)


        preview_sample(samples_with_treatment_no, label="Blood samples with treatment no", n_samples=100, replace=True)

        def visualize_summary(diagnostics: List[DiagnoseTypes]):
            st.altair_chart(visualize_summary_detection(diagnostics), use_container_width=True)

        def visualize_diagnostic_samples(diagnostic_data: DiagnoseTypes):
            st.header(diagnostic_data.name)
            with st.expander("Visualize all samples"):
                st.altair_chart(visualize_detected(diagnostic_data), use_container_width=True)

        def visualize_diagnostic_positive_samples(
            diagnostic_data: DiagnoseTypes, detected_patient_ids: List[str]
        ):
            with st.expander("Visualize all positive samples"):
                st.altair_chart(
                    visualize_detected_by_patient(diagnostic_data, detected_patient_ids),
                    use_container_width=True,
                )

        def visualize_diagnostic_patient(
            diagnostic_data: DiagnoseTypes, detected_patient_ids: List[str]
        ):
            with st.expander("Visualize samples for a specific patient"):
                selected_patient_id = st.selectbox(
                    "Choose a detected patient ID (you can paste the ID you need) : ",
                    detected_patient_ids,
                    key=f"{diagnostic_data.name}_patient_id_slider",
                )
                st.altair_chart(
                    visualize_patient(diagnostic_data, selected_patient_id),
                    use_container_width=True,
                )

        def generate_diagnostic_per_patient(
            diagnostics: List[DiagnoseTypes],
        ) -> pd.DataFrame:
            all_dfs = (
                pd.concat([d.data[[PATIENT_ID, DETECTION]] for d in diagnostics])
                .groupby(PATIENT_ID)[DETECTION]
                .agg("max")
                .reset_index()
                .rename(columns={DETECTION: "PHONOTYPE"})
            )
            all_dfs["PHONOTYPE"] = all_dfs["PHONOTYPE"].astype(int)
            return all_dfs
                
            # Previews
        preview_sample(
            samples_with_treatment_no[samples_with_treatment_no[INFUSION_NO].notnull()],
            "Preview random blood samples + treatment",
        )

        # If necessary, remove data from selected treatment number
        selected_treatments_to_filter = st.multiselect(
            "Select treatment no (INFNO) to filter by:", range(1, 9)
        )
        if len(selected_treatments_to_filter) == 0:
            df = samples_with_treatment_no.copy()
        else:
            selected_treatments_to_filter = set(selected_treatments_to_filter)
            df = samples_with_treatment_no[    samples_with_treatment_no[INFUSION_NO].isin(selected_treatments_to_filter)
            ].copy()

        # Choose phenotype diagnostic to run
        st.sidebar.markdown("<br/>", unsafe_allow_html=True)
        st.sidebar.header("Configure diagnostics parameters")
        selected_diagnostics = st.sidebar.multiselect(
            "Choose the diagnostics you want to study",
            options=range(0, len(DiagnosticClasses)),
            format_func=lambda i: DiagnosticClasses[i].name,
        )

        diagnostics = init_diagnostics(df, selected_diagnostics)
        run_diagnostics(diagnostics)

        if len(selected_diagnostics) != 0:
            visualize_summary(diagnostics)
            with st.expander("Check DME graphs"):
                select_nopho_nr = st.selectbox(
                    "Select patient ID",
                    df.loc[df[P_CODE] == "NPU02739", PATIENT_ID].unique(),
                )
                st.plotly_chart(
                    beta_visualize_dme(df, select_nopho_nr), use_container_width=True
                )
            phenotype_df = generate_diagnostic_per_patient(diagnostics)
            preview_sample(phenotype_df, "Preview exported phenotype", 10, True)
            st.markdown(
                f"""
            * Number of patients in diagnostics : {len(phenotype_df)}
            * Number of patients with positive phenotype : {len(phenotype_df[phenotype_df['PHONOTYPE'] == 1])}
            * Number of patients with negative phenotype : {len(phenotype_df[phenotype_df['PHONOTYPE'] == 0])}
            """
            )
            # st.markdown(generate_download_link(phenotype_df), unsafe_allow_html=True)

            # Create the download link for the merged_df DataFrame
            phenotype_df_download_link = convert_df(phenotype_df)
            st.download_button(
                label="Download data as CSV",
                data=phenotype_df_download_link,
                file_name='phenotype_df.csv',
                mime='text/csv',
            )

            st.markdown("---")

        for diagnostic_data in diagnostics:
            visualize_diagnostic_samples(diagnostic_data)

            detected_positive_patient_ids = diagnostic_data.get_detected_ids()
            if len(detected_positive_patient_ids) == 0:
                continue

            visualize_diagnostic_positive_samples(
                diagnostic_data, detected_positive_patient_ids
            )
            visualize_diagnostic_patient(diagnostic_data, detected_positive_patient_ids)
            st.markdown("---")
            
def login_credentials():
    account = st.text_input("Account", st.secrets["snowflake"]["account"])
    user = st.text_input("User", st.secrets["snowflake"]["user"])
    password = st.text_input("Password", st.secrets["snowflake"]["password"], type="password")
    role = st.text_input("Role", st.secrets["snowflake"]["role"])
    warehouse = st.text_input("Warehouse", st.secrets["snowflake"]["warehouse"])
    db = st.text_input("Database", st.secrets["snowflake"]["database"])
    schema = st.text_input("Schema", st.secrets["snowflake"]["schema"])

    credentials = {
        "account": account,
        "user": user,
        "password": password,
        "role": role,
        "warehouse": warehouse,
        "db": db,
        "schema": schema,
    }

    connect = st.button(
        label="Connect to database",
        on_click=connect_to_db,
        args=(account, user, password, role, warehouse, db, schema)
    )


def page_second():
    st.title("Backend - Snowflake")
    # ...


if __name__ == "__main__":
    initialize_app_info()
    main()
