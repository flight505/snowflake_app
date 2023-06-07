import json
import pandas as pd
from datetime import datetime
from pprint import pprint

from requests import head


# pd.set_option('display.max_rows', None)

# ALL08_PatientData = pd.read_excel('/Users/jesper/Projects/MTX/MTX data 2023/src/data/ALL08_PatientData_2021-01-26.xlsx', header=0, skiprows=[1], sheet_name=0)
# print(ALL08_PatientData.columns)
# # Remove empty columns
# ALL08_PatientData =  ALL08_PatientData.dropna(axis=1, how='all')

# print( ALL08_PatientData.shape)

# # Remove columns that are 99.998% empty
# thresh = len( ALL08_PatientData) * 0.001
# ALL08_PatientData =  ALL08_PatientData.dropna(axis=1, thresh=thresh)

# print( ALL08_PatientData.shape)

# # Remove empty rows
# ALL08_PatientData =  ALL08_PatientData.dropna(subset=['nophonr'])

# # Convert mixed type columns to string
# mixed_type_columns = [col for col in  ALL08_PatientData.columns if  ALL08_PatientData[col].dtype == 'O' and  ALL08_PatientData[col].apply(type).nunique() > 1]
# ALL08_PatientData[mixed_type_columns] =  ALL08_PatientData[mixed_type_columns].astype(str)

# # Fill NaN values with an empty string
# object_columns =  ALL08_PatientData.select_dtypes(include='object').columns.tolist()
# ALL08_PatientData[object_columns] =  ALL08_PatientData[object_columns].fillna('')

# # Remove leading and trailing spaces from string columns
# ALL08_PatientData[object_columns] =  ALL08_PatientData[object_columns].apply(lambda x: x.str.strip())

# # create a dictionary to specify the correct data types
# dtype_dict = {
#     'sp1filledby': str,
#     'exclusioncomment': str,
#     'exclusioncomment_central': str,
#     'andrasjd': str,
#     'cnscomment': str,
#     'therapyindcomm': str,
#     'karyotyp': str,
#     'geneticcomm': str,
#     'f1t1p1filledby': str,
#     'f1t1p2filledby': str,
#     'f1t1p3filledby': str,
#     'karyrevi': str,
#     'compkaryctrlrev_comment': str,
#     'therapyd15comm': str,
#     'extracy': str,
#     'therapyd29comm': str,
#     'therapyd79comm': str,
#     'blastdg1': str,
#     'f2t1p1filledby': str,
#     'f2t1p2filledby': str,
#     'indtherapymod': str,
#     'cencomm': str,
#     'f2t1p3filledby': str,
#     'mancomm': str,
#     'mancommby': str,
#     'monby': str,
#     'saeabdominalr': str,
#     'saeacceptedby': str,
#     'saeanaphylaxisr': str,
#     'saecnsbleedingr': str,
#     'saedialysisr': str,
#     'saefilledby': str,
#     'saehyperglycemiar': str,
#     'saeintcarer': str,
#     'saeliverdysfr': str,
#     'saeotherr': str,
#     'saeotherynr': str,
#     'saepancretitisr': str,
#     'saeparalysisr': str,
#     'saeseizuresr': str,
#     'saesepticshockr': str,
#     'saethrombosisr': str,
#     'saevodr': str,
#     'rndbmperfd79comm': str,
#     'rndbmperfd92comm': str,
#     'rndcompl6mpcomm': str,
#     'rndcomplPEGaspcomm': str,
#     'rndconteaspcomm': str,
#     'rndcontpegaspcomm': str,
#     'rndincr6mpd50comm': str,
#     'rndincr6mpd71comm': str,
#     'rndpacricomm': str,
#     'rndrand1exclcomm': str,
#     'rndrand1filledby': str,
#     'rndrand2exclcomm': str,
#     'rndrand2filledby': str,
#     'rndthboduringpegaspcomm': str,
#     'rndrand3exclcomm': str,
#     'rnditmedcomm': str,
#     'rnd3newneurosymptcomm': str,
#     'rnd3extsteroidcomm': str,
#     'rnd3neuroexmcomm': str,
#     'rnd3psychdeficitcomm': str,
#     'rnd3motornormalcomm': str,
#     'rnd3filledby': str,
#     'trtreatmentcomm': str,
#     'trunk_peg_asp_comm': str,
#     'trfilledby': str,
#     'admincomm': str,
#     'deadreas': str,
#     'eventhistory': str,
#     'f3t1p1filledby': str,
#     'hospcomm': str,
#     'lostatup': str,
#     'consentwcomment': str,
#     'f4t1p1filledby': str,
#     'rdcomm': str,
#     'rtcomm': str,
#     'comment_relapse': str,
#     'recomm': str,
#     'reemcom': str,
#     'remrdcom': str,
#     'rdeadcomm': str,
#     'routcomm': str,
#     'fret1p1filledby': str

# }
#  ALL08_PatientData =  ALL08_PatientData.astype(dtype_dict)

# Print data types after conversion
# print( ALL08_PatientData.dtypes)

# Save the cleaned data to a CSV file
#  ALL08_PatientData.to_csv('ALL_2008_data_cleaned.csv', index=False)

# # Load the Excel file into a pandas dataframe
# df = pd.read_excel("/Users/jesper/Projects/MTX/MTX data 2023/src/data/ALL08_PatientData_2021-01-26.xlsx")

# # Get the column headers from the CSV file as a list
# df_csv = pd.read_csv("/Users/jesper/Projects/MTX/MTX data 2023/ALL_2008_data_cleaned.csv")
# valid_column_names = list(df_csv.columns)

# # Create a dictionary mapping column names to descriptions
# column_dict = dict(zip(df.columns, df.iloc[0]))

# # Create a new dictionary that only contains valid column names
# column_descriptions = {k: {"description": v} for k, v in column_dict.items() if k in valid_column_names}


# # Write the dictionary to a text file
# try:
#     with open('column_descriptions.txt', 'w') as f:
#         f.write(json.dumps(column_descriptions, indent=4, default=str))
#         print('The file column_descriptions.json was created successfully.')
# except Exception as e:
#     print('An error occurred while writing the file:', e)


# Read Excel files
def load_hdm_mtx(df):
    df = pd.read_excel(df)
    print(f"Initial hdm_mtx {df.shape}")
    df = df.dropna(subset=['NOPHO_NR', 'MTX_INFDATE', 'MTX_INF_START']).drop(columns="Unnamed: 0")
    df.columns = df.columns.str.lower().str.replace('\xa0', ' ')
    print(f"Cleaned hdm_mtx {df.shape}")

    return df

hdm_mtx_df = load_hdm_mtx("/Users/jesper/Projects/MTX/MTX data 2023/src/data/HDM_MTX_NOPHO_2023.xlsx")

def load_hdm_plasma(df):
    df = pd.read_excel(df)
    print(f"Initial hdm_plasma {df.shape}")
    df = df.dropna(subset=['NOPHO_NR', 'PL_DATE', 'PL_TIME']).drop(columns="Unnamed: 0")
    df.columns = df.columns.str.lower().str.replace('\xa0', ' ')

    print(f"Cleaned hdm_plasma {df.shape}")
    return df

hdm_plasma_df = load_hdm_plasma("/Users/jesper/Projects/MTX/MTX data 2023/src/data/hdm_plasma_clean.xlsx")


def load_HDM_descript(file_path, sheet_name):
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    df.columns = df.columns.str.lower().str.replace('\xa0', ' ')
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Filter only columns starting with "value"
    value_cols = [col for col in df.columns if col.startswith("value")]

    # Concatenate the values from these columns using commas
    df["detail_values"] = df[value_cols].apply(lambda x: ",".join(x.dropna().astype(str)), axis=1)
    # drop all columns beginning with "value + an integer"
    df = df.drop(columns=[col for col in df.columns if col.startswith("value")])
    return df

hdm_descript = load_HDM_descript("/Users/jesper/Projects/MTX/MTX data 2023/src/data/data_oversigt_HDM.xlsx", "Sheet7")

print(hdm_descript.head())

# # create a dictionary mapping value from column_dict_hdm to df_hdm_mtx column names
# # Get the number of columns in hdm_mtx_df
# num_cols = len(hdm_mtx_df.columns)

# # Get the number of rows in hdm_descript where Variable matches with column names in hdm_mtx_df
# num_rows = len(hdm_descript[hdm_descript["variable"].isin(hdm_mtx_df.columns)])

# # Print the results
# print("Number of columns in hdm_mtx_df:", num_cols)
# print("Number of rows in hdm_descript matching with column names in hdm_mtx_df:", num_rows)


# # def create_column_dict(df, description_df):
# #     # Create a dictionary mapping column names to descriptions from description_df
# #     column_dict = dict(zip(description_df["variable"], description_df["question"]))


# #     # If variable is not in df, remove it from column_dict
# #     column_dict = {k: v for k, v in column_dict.items() if k in df.columns}

# #     # If column names in df are not in column_dict, add them to column_dict and set question to columns name
# #     column_dict.update({k: k for k in df.columns if k not in column_dict})

# #     # if values in column_dict are nan, add the column name to the value instead of nan. add description to column name
# #     column_dict = {k: {"description": k} if pd.isna(v) else {"description": v} for k, v in column_dict.items()}


# #     return column_dict

def create_column_dict_mtx(df, description_df):
    # Create a dictionary mapping column names to descriptions from description_df
    column_dict = {k: {"description": v} if pd.notnull(v) else {"description": k} for k, v in dict(zip(description_df["variable"], description_df["question"])).items()}

    # If variable is not in df, remove it from column_dict
    column_dict = {k: v if k in df.columns else {"description": k} for k, v in column_dict.items()}

    # Add details to column_dict
    for i, row in description_df.iterrows():
        var = row["variable"]
        val = row["detail_values"].strip() if isinstance(row["detail_values"], str) and row["detail_values"].strip() else None
        q = row["question"]
        if var in column_dict and pd.notnull(val):
            details = {}
            for j in range(i, len(description_df)):
                if description_df.loc[j, "variable"] != var:
                    break
                if pd.notnull(description_df.loc[j, "detail_values"]):
                    if pd.isnull(description_df.loc[j, "question"]) and len(description_df.loc[j, "detail_values"].strip()) > 0:
                        details[str(len(details)+1)] = description_df.loc[j, "detail_values"]
                    elif len(description_df.loc[j, "question"].strip()) > 0:
                        details[description_df.loc[j, "detail_values"]] = description_df.loc[j, "question"]
            if len(details) > 0:
                column_dict[var]["details"] = details
            column_dict[var]["details"] = {k: v for k, v in column_dict[var]["details"].items() if k and k.strip()}

    return column_dict


column_dict_hdm_mtx = create_column_dict_mtx(hdm_mtx_df, hdm_descript)
# print(column_dict_hdm_mtx)


def create_column_dict(df, description_df, table_name, output_file):
    # Create a dictionary mapping column names to descriptions from description_df
    column_dict = {}
    for col in df.columns:
        if col in description_df["variable"].values:
            row = description_df.loc[description_df["variable"] == col].iloc[0]
            description = row["question"]
            details = {row["detail_values"]: row["question"]} if pd.notnull(row["detail_values"]) else {}
            for i in range(description_df.index.get_loc(row.name) + 1, len(description_df)):
                next_row = description_df.iloc[i]
                if next_row["variable"] != col:
                    break
                if pd.notnull(next_row["detail_values"]):
                    if pd.isnull(next_row["question"]):
                        details[next_row["detail_values"]] = next_row["detail_values"]
                    else:
                        details[next_row["detail_values"]] = next_row["question"]
            if pd.notnull(description):
                column_dict[col] = {"description": description.strip()}
                if len(details) > 0:
                    column_dict[col]["details"] = details
                else:
                    column_dict[col]["details"] = {}
            else:
                column_dict[col] = {"description": col}
        else:
            column_dict[col] = {"description": col}

    try:
        with open(output_file, 'w') as f:
            f.write(json.dumps({table_name: column_dict}, indent=4, default=str))
            print(f'The file {output_file} was created successfully.')
    except Exception as e:
        print(f'An error occurred while writing the file {output_file}:', e)
    return column_dict


column_dict_hdm_mtx = create_column_dict(hdm_mtx_df, hdm_descript, "hdm_mtx_table", "column_descriptions_hdm_mtx_table.txt")

column_dict_hdm_plasma = create_column_dict(hdm_plasma_df, hdm_descript, "hdm_plasma_table", "column_descriptions_hdm_plasma_table.txt")

# conbine the two dictionaries
column_dict_hdm = {**column_dict_hdm_plasma, **column_dict_hdm_mtx}

# Write the dictionary to a text file
try:
    with open('column_descriptions.txt', 'w') as f:
        f.write(json.dumps(column_dict_hdm, indent=4, default=str))
        print('The file column_descriptions.json was created successfully.')
except Exception as e:
    print('An error occurred while writing the file:', e)


# Write the dictionary to a text file
# try:
#     with open('column_descriptions_hdm_mtx_table.txt', 'w') as f:
#         f.write(json.dumps(column_dict_hdm_mtx, indent=4, default=str))
#         print('The file column_descriptions.json was created successfully.')
# except Exception as e:
#     print('An error occurred while writing the file:', e)


# def create_excel_with_col_names(df, file_path):
#     # Extract column names and store in a dataframe
#     col_names_df = pd.DataFrame(df.columns).T
    
#     # Write the dataframe to an excel file
#     with pd.ExcelWriter(file_path) as writer:
#         col_names_df.to_excel(writer, sheet_name='Sheet1', index=False)
# # convert dictionary to dataframe
# column_dict_hdm = pd.DataFrame(column_dict_hdm).transpose()
# column_dict_hdm.to_excel("database_column_descriptions.xlsx", index_label="column_name")
