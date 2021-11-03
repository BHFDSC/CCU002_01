# ******************************************************************************
# Script:       4-1_CCU002_analysis_retrieve_data.R
# SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK
#               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
# About:        Load CCU002-01 data

# Author:       Hoda Abbasizanjani
#               Health Data Research UK, Swansea University, 2021
# ******************************************************************************
rm(list = ls())

# Setup DB2 connection ---------------------------------------------------------
library(RODBC)
source("login_box.r");
login = getLogin();
sql = odbcConnect('PR_SAIL',login[1],login[2]);
login = 0

# Load cohort ------------------------------------------------------------------
df <- sqlQuery(sql,"SELECT ALF_E,
                           DEATH_DATE,
                           EXP_CONFIRMED_COVID19_DATE,
                           EXP_CONFIRMED_COVID_PHENOTYPE,
						   COV_SEX,
                           COV_AGE,
                           COV_ETHNICITY,
                           OUT_ICVT_PREGNANCY,
                           OUT_ARTERY_DISSECT,
                           OUT_ANGINA,
                           OUT_OTHER_DVT,
                           OUT_DVT_ICVT,
                           OUT_DVT_PREGNANCY,
                           OUT_DVT_DVT,
                           OUT_FRACTURE,
                           OUT_THROMBOCYTOPENIA,
                           OUT_LIFE_ARRHYTHMIA,
                           OUT_PERICARDITIS,
                           OUT_TTP,
                           OUT_MESENTERIC_THROMBUS,
                           OUT_DIC,
                           OUT_MYOCARDITIS,
                           OUT_STROKE_TIA,
                           OUT_STROKE_ISCH,
                           OUT_OTHER_ARTERIAL_EMBOLISM,
                           OUT_UNSTABLE_ANGINA,
                           OUT_PE,
                           OUT_AMI,
                           OUT_HF,
                           OUT_PORTAL_VEIN_THROMBOSIS,
                           OUT_CARDIOMYOPATHY,
                           OUT_STROKE_SAH_HS,
                           OUT_ARTERIAL_EVENT,
                           OUT_VENOUS_EVENT,
                           OUT_HAEMATOLOGICAL_EVENT,
                           OUT_DVT_EVENT,
                           OUT_ICVT_EVENT,
                           COV_SMOKING_STATUS,
                           COV_EVER_AMI,
                           COV_EVER_PE_VT,
                           COV_EVER_ICVT,
                           COV_EVER_ALL_STROKE,
                           COV_EVER_THROMBOPHILIA,
                           COV_EVER_TCP,
                           COV_EVER_DEMENTIA,
                           COV_EVER_LIVER_DISEASE,
                           COV_EVER_CKD,
                           COV_EVER_CANCER,
                           COV_SURGERY_LASTYR,
                           COV_EVER_HYPERTENSION,
                           COV_EVER_DIABETES,
                           COV_EVER_OBESITY,
                           COV_EVER_DEPRESSION,
                           COV_EVER_COPD,
                           COV_DEPRIVATION,
                           COV_REGION,
                           COV_ANTIPLATELET_MEDS,
                           COV_LIPID_MEDS,
                           COV_ANTICOAGULATION_MEDS,
                           COV_COCP_MEDS,
                           COV_HRT_MEDS,
                           COV_N_DISORDER,
                           COV_EVER_OTHER_ARTERIAL_EMBOLISM,
                           COV_EVER_DIC,
                           COV_EVER_MESENTERIC_THROMBUS,
                           COV_EVER_ARTERY_DISSECT,
                           COV_EVER_LIFE_ARRHYTHMIA,
                           COV_EVER_CARDIOMYOPATHY,
                           COV_EVER_HF,
                           COV_EVER_PERICARDITIS,
                           COV_EVER_MYOCARDITIS,
                           COV_UNIQUE_BNF_CHAPS
                    FROM SAILWWMCCV.CCU002_01_COHORT_FULL_20210914")

data.table::fwrite(df,"CCU002_DataPreparation/raw/CCU002_01_COHORT_FULL_20210914.csv")
