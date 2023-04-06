import base64
from datetime import datetime
from io import StringIO

import pandas as pd
import streamlit as st

from joblib import dump

from src.constants import *


def manipulate_hdm_plasma(df):
    """Manipulate hdm_plasma_table DataFrame"""
    # drop rows with no PL_DATE or PL_TIME
    df = df.dropna(subset=['PL_DATE', 'PL_TIME'])
    # combine PL_DATE and PL_TIME columns into a single datetime column
    df['PL_DATE'] = (
        df['PL_DATE'].apply(lambda x: x.strftime('%m/%d/%Y'))
        + " "
        + df['PL_TIME'].apply(lambda x: x.strftime('%H:%M:%S'))
    )

    df.insert(9, 'PL_DATETIME', pd.to_datetime(df['PL_DATE'], format="%m/%d/%Y %H:%M:%S"))
    df = df.drop(['PL_TIME'], axis=1)
    df = df.drop(['PL_DATE'], axis=1)
    df[INFUSION_NO] = df[INFUSION_NO].astype(str)

    return df

def melt_manipulate_hdm_plasma(df):
    df_melted = df.melt(id_vars=['NOPHO_NR', 'PATNUM', 'GUID', 'PATNUMMTX', 'CPR', 'INFNO', 'HOSPITAL', 'LNAME', 'FNAME', 'PL_DATETIME', 'P_ENTERED', 'P_CHANGE'], var_name='PL_Type', value_name='PL_Value')
    df_melted['IUPAC_CODE'] = df_melted['PL_Type'].map({'PL_PC': 'NPU18016', 'PL_MTX': 'NPU02739'})
    return df_melted


# df_plasma = load_hdm_plasma('data/not_used/HDM_Torben/HDM_Plasma.xlsx')


def manipulate_hdm_mtx(df):
    """Load file with infusion times"""
    # drop rows with no PL_DATE or PL_TIME
    df = df.dropna(subset=[INF_STARTDATE, INF_STARTHOUR])

    # combine PL_DATE and PL_TIME columns into a single datetime column
    df[INF_STARTDATE] = (
        df[INF_STARTDATE].apply(lambda x: x.strftime('%m/%d/%Y'))
        + " "
        + df[INF_STARTHOUR].apply(lambda x: x.strftime('%H:%M:%S'))
    )    

    df.insert(9, 'INF_START_DATETIME', pd.to_datetime(df[INF_STARTDATE], format="%m/%d/%Y %H:%M:%S"))
    df = df.drop([INF_STARTHOUR], axis=1)
    df = df.drop([INF_STARTDATE], axis=1)
    df[INFUSION_NO] = df[INFUSION_NO].astype(str)

    return df

def load_infusion_times(df) -> pd.DataFrame:
    """Load file with infusion times"""
    # drop rows with no PL_DATE or PL_TIME
    df = df.dropna(subset=[INF_STARTDATE, INF_STARTHOUR])
    columns=[PATIENT_ID, INFUSION_NO, SEX, MP6_STOP, INF_STARTDATE, INF_STARTHOUR]
    df = df[columns]

    # combine PL_DATE and PL_TIME columns into a single datetime column
    df[INF_STARTDATE] = (
        df[INF_STARTDATE].apply(lambda x: x.strftime('%m/%d/%Y'))
        + " "
        + df[INF_STARTHOUR].apply(lambda x: x.strftime('%H:%M:%S'))
    )    

    df.insert(3, 'INF_START_DATETIME', pd.to_datetime(df[INF_STARTDATE], format="%m/%d/%Y %H:%M:%S"))
    df = df.drop([INF_STARTHOUR], axis=1)
    df = df.drop([INF_STARTDATE], axis=1)
    df[INFUSION_NO] = df[INFUSION_NO].astype(str)

    return df

def remove_patients_with_duplicate_treatments(
    infusion_times: pd.DataFrame,
) -> pd.DataFrame:
    """Some patients have repeated INFNO treatment numbers, let's remove those patients for now"""
    df = infusion_times.copy()
    count_treatment_per_id = (
        df[PATIENT_ID].astype(str) + "_" + df[INFUSION_NO]
    ).value_counts()
    ids_with_duplicate_treatments = {
        s.split("_")[0]
        for s in count_treatment_per_id[count_treatment_per_id > 1].index.values
    }
    if len(ids_with_duplicate_treatments) != 0:
       st.warning(
           f"Patients have duplicate number treatments in infusion times "
           f"and were removed: {ids_with_duplicate_treatments}"
       )
    return df[~df[PATIENT_ID].isin(ids_with_duplicate_treatments)]

# df_mtx = load_hdm_mtx('data/not_used/HDM_Torben/HDM_MTX.xlsx')


def merge_blood_samples_to_treatment(samples_df, infusion_times_df):
    # Now that infusion times have no duplicates
    infusion_times_df = infusion_times_df.drop_duplicates(subset=[PATIENT_ID, INFUSION_NO, INF_START_DATETIME])

    # We can pivot the infusion times dataset to make it easier to join to samples
    # Numeric columns are now INFNO: number of treatment
    pivot_infusion_times = infusion_times_df.pivot(
        index=PATIENT_ID, columns=INFUSION_NO, values=INF_START_DATETIME
    ).reset_index()

    samples_with_infusion_times = samples_df.merge(
        pivot_infusion_times, on=PATIENT_ID, how="left", indicator=True
    )
    samples_with_infusion_times["_merge"] = samples_with_infusion_times[
        "_merge"
    ].astype(str)

    def date_to_treatment_no(s: pd.Series):
        if s["_merge"] == "left_only":
            return [None, None, None]
        elif not pd.isnull(s["8"]) and s[PLASMA_SAMPLE_DATETIME] >= s["8"]:
            return [8, s["8"], s[PLASMA_SAMPLE_DATETIME] - s["8"]]
        elif not pd.isnull(s["7"]) and s[PLASMA_SAMPLE_DATETIME] >= s["7"]:
            return [7, s["7"], s[PLASMA_SAMPLE_DATETIME] - s["7"]]
        elif not pd.isnull(s["6"]) and s[PLASMA_SAMPLE_DATETIME] >= s["6"]:
            return [6, s["6"], s[PLASMA_SAMPLE_DATETIME] - s["6"]]
        elif not pd.isnull(s["5"]) and s[PLASMA_SAMPLE_DATETIME] >= s["5"]:
            return [5, s["5"], s[PLASMA_SAMPLE_DATETIME] - s["5"]]
        elif not pd.isnull(s["4"]) and s[PLASMA_SAMPLE_DATETIME] >= s["4"]:
            return [4, s["4"], s[PLASMA_SAMPLE_DATETIME] - s["4"]]
        elif not pd.isnull(s["3"]) and s[PLASMA_SAMPLE_DATETIME] >= s["3"]:
            return [3, s["3"], s[PLASMA_SAMPLE_DATETIME] - s["3"]]
        elif not pd.isnull(s["2"]) and s[PLASMA_SAMPLE_DATETIME] >= s["2"]:
            return [2, s["2"], s[PLASMA_SAMPLE_DATETIME] - s["2"]]
        elif not pd.isnull(s["1"]) and s[PLASMA_SAMPLE_DATETIME] >= s["1"]:
            return [1, s["1"], s[PLASMA_SAMPLE_DATETIME] - s["1"]]
        elif not pd.isnull(s["1"]) and s[PLASMA_SAMPLE_DATETIME] < s["1"]:
            return [0, s["1"], s[PLASMA_SAMPLE_DATETIME] - s["1"]]
        else:
            return [None, None, None]

    # TODO: this apply method is probably the slowest of the app, can we vectorize this ?
    samples_with_infusion_times[
        [INFUSION_NO, INF_STARTDATE, DIFFERENCE_SAMPLETIME_TO_INF_STARTDATE]
    ] = samples_with_infusion_times.apply(
        date_to_treatment_no, axis=1, result_type="expand"
    )
    samples_with_infusion_times[
        DIFFERENCE_SAMPLETIME_TO_INF_STARTDATE
    ] = samples_with_infusion_times[DIFFERENCE_SAMPLETIME_TO_INF_STARTDATE].astype(
        "timedelta64[h]"
    )

    samples_with_infusion_times = samples_with_infusion_times.drop(columns=["_merge"])

    # integrate MP6_stop and SEX separately so we can filter inaccurate data
    sex_per_patient = infusion_times_df.loc[
        infusion_times_df[SEX] != 0, [PATIENT_ID, SEX]
    ].drop_duplicates()
    samples_with_infusion_times = samples_with_infusion_times.merge(
        sex_per_patient, on=PATIENT_ID, how="left"
    )

    MP6_per_patient_treatment = infusion_times_df[
        [PATIENT_ID, INFUSION_NO, MP6_STOP]
    ].drop_duplicates()
    MP6_per_patient_treatment[INFUSION_NO] = MP6_per_patient_treatment[
        INFUSION_NO
    ].astype(float)
    samples_with_infusion_times = samples_with_infusion_times.merge(
        MP6_per_patient_treatment, on=[PATIENT_ID, INFUSION_NO], how="left"
    )

    return samples_with_infusion_times


def save_model(classifier, output_path: str):
    dump(classifier, output_path)


def save_png_figure(figure, output_path: str):
    figure.savefig(
        output_path,
        format="png",
        dpi=150,
        bbox_inches="tight",
    )