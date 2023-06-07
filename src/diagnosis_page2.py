#!/usr/bin/env python3
# imports
import base64
import json
import math
from datetime import date
import re
# from turtle import title
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

# Optional -- adds the title and icon to the current page
add_page_title()

st.session_state
if st.session_state.is_ready is False:
    st.error("Please connect to Snowflake first")
else:
    


    hdm_plasma_data = fetch_hdm_plasma_data(st.session_state['engine'])
    hdm_mtx_data = fetch_hdm_mtx_data(st.session_state['engine'])

    # Fetch data from Snowflake
    hdm_plasma = manipulate_hdm_plasma(hdm_plasma_data)
    hdm_mtx = manipulate_hdm_mtx(hdm_mtx_data)
    infusion_times = load_infusion_times(hdm_mtx_data)
    melt_hdm_plasma = melt_manipulate_hdm_plasma(hdm_plasma)
    with st.expander("HDM Plasma"):
        st.dataframe(hdm_plasma)
    with st.expander("HDM Mtx"):
        st.dataframe(hdm_mtx)
    with st.expander("Infusion times"):
        st.dataframe(infusion_times)
    with st.expander("Melted HDM Plasma"):
        st.dataframe(melt_hdm_plasma)

    st.session_state
    def create_time_intervals_df(hdm_mtx, hdm_plasma, infusion_times):
        time_intervals = create_time_intervals(hdm_plasma, infusion_times)
        return (
            pd.merge(hdm_mtx, time_intervals, on="NOPHO_NR", how="left")
            .drop(columns=['INFNO_x'])
            .rename(columns={'INFNO_y': 'INFNO'})
        )

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


    preview_sample(samples_with_treatment_no, label="Blood samples with treatment no", n_samples=100, replace=False)

    def visualize_summary(diagnostics: List[DiagnoseTypes]):
        st.altair_chart(visualize_summary_detection(diagnostics), use_container_width=True, theme="streamlit")

    def visualize_diagnostic_samples(diagnostic_data: DiagnoseTypes):
        st.header(diagnostic_data.name)
        with st.expander("Visualize all samples"):
            st.altair_chart(visualize_detected(diagnostic_data), use_container_width=False, theme="streamlit")

    def visualize_diagnostic_positive_samples(
        diagnostic_data: DiagnoseTypes, detected_patient_ids: List[str]
    ):
        with st.expander("Visualize all positive samples"):
            st.altair_chart(
                visualize_detected_by_patient(diagnostic_data, detected_patient_ids),
                use_container_width=False,
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
                use_container_width=False,
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

    # get unique patient ids
    patient_ids = df[PATIENT_ID].unique()
    st.write(f"Number of patients: {len(patient_ids)}")

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
            # df contain null or nan values
            # st.plotly_chart(
            #     beta_visualize_dme(df, select_nopho_nr), use_container_width=False
            # )
            # trying altair chart
            st.altair_chart(
                beta_visualize_dme(df, select_nopho_nr),
                use_container_width=True,
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

        # Create the download link for the merged_df DataFrame
        phenotype_df_download_link = convert_df(phenotype_df)
        st.download_button(
            label="Download data as CSV",
            data=phenotype_df_download_link,
            file_name='phenotype_df.csv',
            mime='text/csv',
        )

        st.divider()

    for diagnostic_data in diagnostics:
        visualize_diagnostic_samples(diagnostic_data)

        detected_positive_patient_ids = diagnostic_data.get_detected_ids()
        if len(detected_positive_patient_ids) == 0:
            continue

        visualize_diagnostic_positive_samples(
            diagnostic_data, detected_positive_patient_ids
        )
        visualize_diagnostic_patient(diagnostic_data, detected_positive_patient_ids)
        st.divider()