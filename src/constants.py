"""All constants, mostly name of columns from initial dataframe used in the computation of diagnostics

    from src.constants import *
    print(DETECTION)
"""
from enum import Enum
from venv import create

########################################################################
# HDM_Plasma file 
########################################################################

PLASMA_SAMPLE_DATETIME = 'PL_DATETIME'
CREA_SAMPLE_CONC = "PL_PC"
MTX_SAMPLE_CONC = "PL_MTX"
VALUE = "PL_Value"
P_CODE = "IUPAC_CODE"
SAMPLE_TIME = "PL_DATETIME"
########################################################################
# HDM_MTX file / Infusion times file
########################################################################
INFUSION_NO = "INFNO"
INF_STARTDATE = "MTX_INFDATE"
INF_STARTHOUR = "MTX_INF_START"
INF_START_DATETIME = "INF_START_DATETIME"

SEX = "SEX"
MP6_STOP = "MP6_POST_STOP"
PATIENT_ID = "NOPHO_NR"
########################################################################
# Computed in app
########################################################################
DETECTION = "detection"
DIFFERENCE_SAMPLETIME_TO_INF_STARTDATE = "HOUR_DIFF_SAMPLE_INF"

########################################################################
# Blood Samples file
########################################################################
# SAMPLE_TIME = "REALSAMPLECOLLECTIONTIME"
# P_CODE = "OL_INVER_IUPAC_CDE"
PATIENT_ID = "NOPHO_NR"
# VALUE = "INTERNAL_REPLY_NUM"
REF_PATIENT = "REFTEXT"


# I have one dataframe where the columns are: 

# NOPHO_NR              int64
# GUID                 object
# PATNUMMTX             int64
# CPR                  object
# INFNO                 int64
# HOSPITAL              int64
# LNAME                object
# FNAME               float64
# SEX                   int64
# PL_DATETIME      datetime64[ns]
# PL_PC               float64
# PL_MTX              float64
# P_ENTERED    datetime64[ns]
# P_CHANGE     datetime64[ns]

# And another dataframe where the columns are:
# NOPHO_NR, INFNO, SEX, INF_START_DATETIME

# PL_DATETIME is the time when the sample is taken
# INF_START_DATETIME is the time when infusion begins, and INFNO is the treatment number. 
# using both dataframes write code to modify the dataframe such that the first dataframe is has a new columns begining with PL_PC_TIME x, where x is the interval 0, 23, 36 ,...
# and columns called PL_MTX_TIME x, where x is the interval 0, 23, 36 ,... 
# TIME x which between the time when the sample is taken and the time when the infusion begins.
# You have to use the INFNO to match the correct infusion time.
# you can assume that Infusion times outside 7 hours before and 304 hours after are considered having
# a date error. Hence they are set to missing.
# The table has to be pivoted so that the columns are the intervals, and the rows are the patients.
# You have to take the values in PL_PC and PL_MTX and calculate their place in the intervals columns using the time difference between the sample time (PL_DATETIME) and the INF_START_DATETIME and acounting for the infusion number (INFNO).

# here is some provious code I have written to create the intervals:


# > . /**/egen timegroup = cut(infusiontime), at(-7 0 18 30 39(6)87 100(12)304) label icodes
# > (524 missing values generated)

# . label define timegroup 0 "Before" 1 "Time0" 2 "Time23", replace

# . forvalues t = 3/12 {
#   2. label define timegroup `t' `"Time `=6*`t'+18'"', add
#   3. }

# . forvalues t = 13/29 {
#   2. label define timegroup `t' `"Time `=12*`t'-54'"', add
#   3. }

# > time23         (18 - 30)
# > time36.        (30 - 39)
# > time42.        (39 - 45)
# > time48.        (45 - 51)
# > time54.        (51 - 57)
# > time60.        Xxx
# > time66.        Xxx
# > time72.        Xxx
# > time78.        Xxx
# > time84         xxx
# > time90.        (87 - 100)
# > time102.      100 - 112
# > time114.      ect
# > into intervaller of 12 timer
# > */*

# encapsulate into a function 