"""Helper class to compute diagnostics on input dataframe
"""
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
import base64
import math

from src.datasets import merge_blood_samples_to_treatment, remove_patients_with_duplicate_treatments

def preview_sample(df: pd.DataFrame, label: str = "Sample", n_samples: int = 100, replace: bool = True) -> None:
    """Generate a preview of a data frame. This is useful for debugging and to 
    make it easier to visualize the performance of an experiment.

    Parameters
    ----------
    df : pandas.DataFrame
        The data frame to sample.
    label : str, optional
        The label to use for the expander. Defaults to "Sample".
    n_samples : int, optional
        The number of samples to generate. Defaults to 100.
    replace : bool, optional
        Whether to replace existing samples or not. Defaults to True.
    """
    with st.expander(label):
        st.dataframe(df.sample(n_samples, replace=replace))

def custom_sort(col_name):
    if col_name == 'INFNO':
        return ("", -1)

    if isinstance(col_name, float) and math.isnan(col_name):
        return ("", float("inf"))

    parts = col_name.split('_')
    prefix = '_'.join(parts[:-1])
    time_group = parts[-1]

    if time_group == 'NR':  # Add this condition to handle 'NR'
        return (prefix, float("inf"))

    if time_group == 'Before':
        index = -1
    elif "Time" in time_group:
        index = int(time_group.replace("Time", ""))
    elif time_group == "nan":
        return (prefix, float("inf"))
    else:
        raise ValueError(f"Unexpected time group: {time_group}")

    return (prefix, index)


def create_time_intervals(df1, df2):
    df2 = df2.rename(columns={'INF_START_DATETIME': 'INF_START'})
    merged_df = pd.merge(df1, df2, on=['NOPHO_NR', 'INFNO'], how='left')

    merged_df['TIME_DIFF'] = (merged_df['PL_DATETIME'] - merged_df['INF_START']).dt.total_seconds() / 3600

    intervals = [
        (-7, 0, 'Before'),
        (0, 18, 'Time0'),
        (18, 30, 'Time23')
    ]

    for t in range(3, 13):
        intervals.append((6*t + 18, 6*t + 24, f'Time{6*t + 18}'))

    for t in range(13, 30):
        intervals.append((12*t - 54, 12*t - 42, f'Time{12*t - 54}'))

    conditions = [(merged_df['TIME_DIFF'] > start) & (merged_df['TIME_DIFF'] <= end) for start, end, _ in intervals]
    choices = [label for _, _, label in intervals]

    merged_df['TIME_GROUP'] = np.select(conditions, choices, default=np.nan)

    # Pivot the TIME_GROUP column with PL_PC and PL_MTX as values
    pivoted_pc = pd.pivot_table(merged_df, index=['NOPHO_NR', 'INFNO'], columns='TIME_GROUP', values='PL_PC', aggfunc='first')
    pivoted_mtx = pd.pivot_table(merged_df, index=['NOPHO_NR', 'INFNO'], columns='TIME_GROUP', values='PL_MTX', aggfunc='first')

    # Reset the column names
    pivoted_pc.columns = [f'PL_PC_{col}' for col in pivoted_pc.columns]
    pivoted_mtx.columns = [f'PL_MTX_{col}' for col in pivoted_mtx.columns]

    pivoted_pc.columns.name = None
    pivoted_mtx.columns.name = None

    # Combine the pivoted dataframes
    result_df = pd.concat([pivoted_pc, pivoted_mtx], axis=1)

    result_df = result_df.reset_index()  # Add this line to reset the index of the resulting dataframe

    # Sort columns using the custom_sort function
    result_df = result_df.sort_index(axis=1, key=lambda x: x.map(custom_sort))

    return result_df


def load_blood_samples_by_treatment(
    hdm_plasma: pd.DataFrame, infusion_times: pd.DataFrame):
    # Merge blood samples to treatments
    clean_infusion_times = infusion_times
    # clean_infusion_times = remove_patients_with_duplicate_treatments(infusion_times)
    samples_with_treatment_no = merge_blood_samples_to_treatment(
        hdm_plasma, clean_infusion_times
    )
    return samples_with_treatment_no

def compute_streaks_of_detection(
    df: pd.DataFrame,
    column_variable: str,
    column_patient_id: str,
    column_date: datetime,
):
    """Compute streaks of column_variable column"""
    df = df.sort_values([column_patient_id, column_date])
    df["shifted_detection"] = df.groupby(column_patient_id)[column_variable].shift(1)
    df["start_of_streak"] = df[column_variable].ne(df[f"shifted_{column_variable}"])
    df["streak_id"] = df.groupby(column_patient_id)["start_of_streak"].cumsum()
    return df["streak_id"]


def is_streak_longer_than_duration(
    df: pd.DataFrame,
    column_variable: str,
    column_patient_id: str,
    column_date: str,
    longer_than_n_hours: int,
):
    """Get duration of each streak of successive values in column_variable column,
    and return True if duration exceeds longer_than_n_hours, False otherwise, in a new column.

    column_variable is generally the detection variable so we see if long streak of positive diagnostic
    """
    data = df.copy()
    data["streak_id"] = compute_streaks_of_detection(
        data, column_variable, column_patient_id, column_date
    )

    # Group elements in the same streak together to compute duration of streak and if it's a positive or negative one
    group_streaks_by_patient = (
        data.groupby([column_patient_id, "streak_id"])[[column_date, column_variable]]
        .agg(
            min_date=(column_date, "min"),
            max_date=(column_date, "max"),
            is_streak_positive=(column_variable, "first"),
        )
        .reset_index()
    )
    group_streaks_by_patient["streak_duration"] = (
        group_streaks_by_patient["max_date"] - group_streaks_by_patient["min_date"]
    ) / np.timedelta64(1, "h")

    group_streaks_by_patient[column_variable] = (
        group_streaks_by_patient["streak_duration"] > longer_than_n_hours
    ) & group_streaks_by_patient["is_streak_positive"]

    # Left join the data with the table of long positive streaks
    res = (
        data.reset_index()
        .merge(
            group_streaks_by_patient, on=[column_patient_id, "streak_id"], how="left"
        )
        .set_index("index")
    )

    return res[f"{column_variable}_x"] & res[f"{column_variable}_y"]
