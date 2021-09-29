# Databricks notebook source
# MAGIC %md %md # CCU002_01-D10-cohort
# MAGIC 
# MAGIC **Description** This notebook makes the analysis dataset.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker, Sam Ip, Spencer Keene

# COMMAND ----------

%run Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/wrang000_functions

# COMMAND ----------

 # MAGIC %md
 ## Define exposures

# COMMAND ----------

 %sql
 --All patients exposed to COVID
 create or replace global temporary view ccu002_01_covid19_confirmed as
 select *
 from dars_nic_391419_j3w9t_collab.ccu002_01_covid_trajectory
 where covid_status = 'confirmed'
 AND (covid_phenotype = '01_Covid_positive_test' OR covid_phenotype = '01_GP_covid_diagnosis' OR covid_phenotype = '02_Covid_admission_any_position' OR covid_phenotype = '02_Covid_admission_primary_position')
 AND (source = 'SGSS' OR source = 'HES APC' OR source = 'SUS' OR source = 'GDPPR')
 AND (date >= '2020-01-01' and date < '2020-12-08' )

# COMMAND ----------

 %sql
 --Days between first COVID event and first admission with COVID in primary position
 create or replace global temporary view ccu002_01_days_to_covid_admission as
 with min_date_covid as(
 select person_id_deid, min(date) as first_covid_event
 from global_temp.ccu002_01_covid19_confirmed
 group by person_id_deid
 ),
 
 min_date_admission as (
 select person_id_deid, min(date) as first_admission_date
 from global_temp.ccu002_01_covid19_confirmed
 where covid_phenotype = '02_Covid_admission_primary_position'
 group by person_id_deid
 ),
 
 min_date_covid_admission as (
 select t1.person_id_deid, t2.first_covid_event, t1.first_admission_date
 from min_date_admission t1
 left join min_date_covid t2
 on t1.person_id_deid = t2.person_id_deid)
 
 select *, datediff(first_admission_date,first_covid_event) as days_between
 from min_date_covid_admission

# COMMAND ----------

 %sql
 --Only want patients who were admitted within 28 days
 create or replace global temp view ccu002_01_covid_admission_primary as 
 select distinct person_id_deid, 'hospitalised' as covid_hospitalisation
 from global_temp.ccu002_01_days_to_covid_admission
 where days_between <= '28'

# COMMAND ----------

 %sql
 -- Labelling as hospitalised and non-hospitalsied
 create or replace global temp view ccu002_01_covid_hospitalised as 
 with cte_hospitalised as (
 select t1.*, t2.covid_hospitalisation
 from global_temp.ccu002_01_covid19_confirmed t1
 left join global_temp.ccu002_01_covid_admission_primary t2
 on t1.person_id_deid = t2.person_id_deid)
 
 select *,
 case when covid_hospitalisation IS NULL THEN 'non_hospitalised' ELSE covid_hospitalisation END AS covid_hospitalisation_phenotype
 from cte_hospitalised

# COMMAND ----------

 %sql
 -- Finding first COVID event date
 create or replace global temp view ccu002_01_covid_cohort as
 select person_id_deid, min(date) AS covid19_confirmed_date, covid_hospitalisation_phenotype
 from global_temp.ccu002_01_covid_hospitalised
 group by person_id_deid, covid_hospitalisation_phenotype

# COMMAND ----------

 # MAGIC %md ## Create temporary views for each outcome

# COMMAND ----------

 # MAGIC %md
 #### First position individual events

# COMMAND ----------

for outcome in ["ICVT_pregnancy","artery_dissect","angina","other_DVT","DVT_ICVT","DVT_pregnancy","DVT_DVT","fracture","thrombocytopenia","life_arrhythmia","pericarditis","TTP","mesenteric_thrombus","DIC","myocarditis","stroke_TIA","stroke_isch","other_arterial_embolism","unstable_angina","PE","AMI","HF","portal_vein_thrombosis","cardiomyopathy","stroke_SAH_HS"]:
  
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW " + outcome + " AS SELECT NHS_NUMBER_DEID, record_date AS " + outcome + "_date FROM dars_nic_391419_j3w9t_collab.ccu002_01_cvdoutcomes_infections_first_v2 WHERE name = '" + outcome + "'")

# COMMAND ----------

 # MAGIC %md
 #### First position summary events

# COMMAND ----------

 %sql
 --Group together arterial, venous and haematological events and select the earliest event date in each summary group
 create or replace global temp view  ccu002_01_arterial_venous_haematological AS
 
 with cte_arterial_venous_haematological as (
 SELECT NHS_NUMBER_DEID, record_date, name, term, SOURCE, terminology, code,
 (case when name = 'AMI' OR name = 'stroke_isch' /*OR name = 'stroke_SAH_HS' OR name = 'stroke_NOS' OR name = 'stroke_SAH' OR name = 'retinal_infarction' */ OR name = 'other_arterial_embolism' THEN 1 Else 0 End) as Arterial_event,
 (case when name like 'PE%' OR name like '%DVT%' OR name like '%ICVT%' OR name like 'portal%' THEN 1 Else 0 End) as Venous_event,
 (case when name like 'DIC' OR name = 'TTP' OR name = 'thrombocytopenia' THEN 1 Else 0 End) as Haematological_event
 from dars_nic_391419_j3w9t_collab.ccu002_01_cvdoutcomes_infections_first_v2),
 
 cte_first_arterial as (
 select NHS_NUMBER_DEID, 'Arterial_event' as composite_event_type, min(record_date) as min_date
 from cte_arterial_venous_haematological
 where Arterial_event = '1'
 group by nhs_number_deid
 ),
 
 cte_first_venous as (
 select NHS_NUMBER_DEID, 'Venous_event' as composite_event_type, min(record_date) as min_date
 from cte_arterial_venous_haematological
 where Venous_event = '1'
 group by nhs_number_deid
 ),
 
 cte_first_haematological as (
 select NHS_NUMBER_DEID, 'Haematological_event' as composite_event_type, min(record_date) as min_date
 from cte_arterial_venous_haematological
 where Haematological_event = '1'
 group by nhs_number_deid
 )
 
 select *
 from cte_first_arterial
 union all
 select *
 from cte_first_venous
 union all
 select *
 from cte_first_haematological

# COMMAND ----------

# Generate temporary views for each outcome
for outcome in ["Arterial_event","Venous_event","Haematological_event"]:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW " + outcome + " AS SELECT NHS_NUMBER_DEID, min_date AS " + outcome + "_date FROM global_temp.ccu002_01_arterial_venous_haematological WHERE composite_event_type = '" + outcome + "'")

# COMMAND ----------

 %sql
 --Group together DVT and ICVT events and select the earliest event date in each summary group
 create or replace global temp view ccu002_01_dvt_icvt as
 
 with cte_dvt_icvt as (
 SELECT NHS_NUMBER_DEID, record_date, name, term, SOURCE, terminology, code,
 (case when name = 'DVT_DVT' OR name = 'DVT_pregnancy' THEN 1 Else 0 End) as DVT_event,
 (case when name like 'DVT_ICVT' OR name like 'ICVT_pregnancy' THEN 1 Else 0 End) as ICVT_event 
 from dars_nic_391419_j3w9t_collab.ccu002_01_cvdoutcomes_infections_first_v2),
 
 cte_first_dvt as(
 select NHS_NUMBER_DEID, 'DVT_event' as composite_event_type, min(record_date) as min_date
 from cte_dvt_icvt
 where DVT_event = '1'
 group by nhs_number_deid
 ),
 
 cte_first_icvt as(
 select NHS_NUMBER_DEID, 'ICVT_event' as composite_event_type, min(record_date) as min_date
 from cte_dvt_icvt
 where ICVT_event = '1'
 group by nhs_number_deid
 )
 
 select *
 from cte_first_dvt
 union all
 select *
 from cte_first_icvt

# COMMAND ----------

# Generate temporary views for each outcome
for outcome in ["DVT_event","ICVT_event"]:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW " + outcome + " AS SELECT NHS_NUMBER_DEID, min_date AS " + outcome + "_date FROM global_temp.ccu002_01_dvt_icvt WHERE composite_event_type = '" + outcome + "'")

# COMMAND ----------

 # MAGIC %md
 ## Create cohort table

# COMMAND ----------

 %sql
 -- Create cohort with exposures (prefix 'exp_'), outcomes (prefix 'out_'), and covariates (prefix 'cov_')
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_cohort_full AS
 SELECT FLOOR(RAND()*8)+1 AS CHUNK, -- CHUNK divides the data into parts for import into R
        cohort.NHS_NUMBER_DEID,
        cohort.DATE_OF_DEATH AS death_date,
        cohort.SEX AS cov_sex,
        cohort.AGE_AT_COHORT_START AS cov_age, 
        CASE WHEN cohort.CATEGORISED_ETHNICITY IS NULL THEN 'Missing' ELSE cohort.CATEGORISED_ETHNICITY END AS cov_ethncity,
        covid19_confirmed.covid19_confirmed_date AS exp_confirmed_covid19_date,
        covid19_confirmed.covid_hospitalisation_phenotype AS exp_confirmed_covid_phenotype,
        ICVT_pregnancy.ICVT_pregnancy_date AS out_ICVT_pregnancy,
        artery_dissect.artery_dissect_date AS out_artery_dissect,
        angina.angina_date AS out_angina,
        other_DVT.other_DVT_date AS out_other_DVT,
        DVT_ICVT.DVT_ICVT_date AS out_DVT_ICVT,
        DVT_pregnancy.DVT_pregnancy_date AS out_DVT_pregnancy,
        DVT_DVT.DVT_DVT_date AS out_DVT_DVT,
        fracture.fracture_date AS out_fracture,
        thrombocytopenia.thrombocytopenia_date AS out_thrombocytopenia,
        life_arrhythmia.life_arrhythmia_date AS out_life_arrhythmia,
        pericarditis.pericarditis_date AS out_pericarditis,
        TTP.TTP_date AS out_TTP,
        mesenteric_thrombus.mesenteric_thrombus_date AS out_mesenteric_thrombus,
        DIC.DIC_date AS out_DIC,
        myocarditis.myocarditis_date AS out_myocarditis,
        stroke_TIA.stroke_TIA_date AS out_stroke_TIA,
        stroke_isch.stroke_isch_date AS out_stroke_isch,
        other_arterial_embolism.other_arterial_embolism_date AS out_other_arterial_embolism,
        unstable_angina.unstable_angina_date AS out_unstable_angina,
        PE.PE_date AS out_PE,
        AMI.AMI_date AS out_AMI,
        HF.HF_date AS out_HF,
        portal_vein_thrombosis.portal_vein_thrombosis_date AS out_portal_vein_thrombosis,
        cardiomyopathy.cardiomyopathy_date AS out_cardiomyopathy,
        stroke_SAH_HS.stroke_SAH_HS_date AS out_stroke_SAH_HS,
        Arterial_event.Arterial_event_date AS out_Arterial_event,
        Venous_event.Venous_event_date AS out_Venous_event,
        Haematological_event.Haematological_event_date AS out_Haematological_event,
        DVT_event.DVT_event_date AS out_DVT_event,
        ICVT_event.ICVT_event_date AS out_ICVT_event,
        CASE WHEN covar.smoking_status IS NULL THEN 'Missing' ELSE covar.smoking_status END AS cov_smoking_status,
        CASE WHEN covar.AMI=1 THEN 1 ELSE 0 END AS cov_ever_ami,
        CASE WHEN covar.PE = 1 OR covar.VT = 1 THEN 1
             WHEN covar.PE = 0 AND covar.VT = 0 THEN 0 ELSE 0 END as cov_ever_pe_vt,
        CASE WHEN covar.DVT_ICVT=1 THEN 1 ELSE 0 END AS cov_ever_icvt,
        CASE WHEN covar.stroke_SAH_HS = 1 OR covar.stroke_TIA = 1 THEN 1 
             WHEN covar.stroke_SAH_HS = 0 AND covar.stroke_TIA = 0 THEN 0 Else 0 End as cov_ever_all_stroke,
        CASE WHEN covar.thrombophilia=1 THEN 1 ELSE 0 END AS cov_ever_thrombophilia,
        CASE WHEN covar.TCP=1 THEN 1 ELSE 0 END AS cov_ever_tcp,
        CASE WHEN covar.dementia=1 THEN 1 ELSE 0 END AS cov_ever_dementia,
        CASE WHEN covar.liver_disease=1 THEN 1 ELSE 0 END AS cov_ever_liver_disease,
        CASE WHEN covar.CKD=1 THEN 1 ELSE 0 END AS cov_ever_ckd,
        CASE WHEN covar.cancer=1 THEN 1 ELSE 0 END AS cov_ever_cancer,
        CASE WHEN covar.SURGERY_LASTYR_HES IS NULL THEN 0 ELSE covar.SURGERY_LASTYR_HES END AS cov_surgery_lastyr,
        CASE WHEN covar.hypertension=1 THEN 1 ELSE 0 END AS cov_ever_hypertension,
        CASE WHEN covar.diabetes=1 THEN 1 ELSE 0 END AS cov_ever_diabetes,
        CASE WHEN covar.OBESE_BMI=1 or covar.BMI_obesity=1 THEN 1 
             WHEN covar.OBESE_BMI=0 AND covar.BMI_obesity=0 THEN 0 ELSE 0 END AS cov_ever_obesity,
        CASE WHEN covar.depression=1 THEN 1 ELSE 0 END AS cov_ever_depression,
        CASE WHEN covar.COPD=1 THEN 1 ELSE 0 END AS cov_ever_copd,
        CASE WHEN covar.DECI_IMD IS NULL THEN 'Missing' 
             WHEN (covar.DECI_IMD=1 OR covar.DECI_IMD=2) THEN 'Deciles_1_2'
             WHEN (covar.DECI_IMD=3 OR covar.DECI_IMD=4) THEN 'Deciles_3_4'
             WHEN (covar.DECI_IMD=5 OR covar.DECI_IMD=6) THEN 'Deciles_5_6'
             WHEN (covar.DECI_IMD=7 OR covar.DECI_IMD=8) THEN 'Deciles_7_8'
             WHEN (covar.DECI_IMD=9 OR covar.DECI_IMD=10) THEN 'Deciles_9_10' END AS cov_deprivation,
        CASE WHEN covar.region_name IS NULL THEN 'Missing' ELSE covar.region_name END AS cov_region,
        CASE WHEN covar.antiplatelet=1 THEN 1 ELSE 0 END AS cov_antiplatelet_meds,
        --CASE WHEN covar.BP_LOWER_MEDS=1 THEN 1 ELSE 0 END AS cov_hypertension_meds, -- removed as hypertension includes hypertension meds
        CASE WHEN covar.lipid_lowering=1 THEN 1 ELSE 0 END AS cov_lipid_meds,
        CASE WHEN covar.anticoagulant=1 THEN 1 ELSE 0 END AS cov_anticoagulation_meds,
        CASE WHEN covar.cocp=1 AND cohort.AGE_AT_COHORT_START<70 THEN 1 ELSE 0 END AS cov_cocp_meds,
        CASE WHEN covar.hrt=1 THEN 1 ELSE 0 END AS cov_hrt_meds,
        CASE WHEN covar.N_DISORDER IS NULL THEN 0 ELSE covar.N_DISORDER END AS cov_n_disorder,
        CASE WHEN covar.other_arterial_embolism=1 THEN 1 ELSE 0 END AS cov_ever_other_arterial_embolism,
        CASE WHEN covar.DIC=1 THEN 1 ELSE 0 END AS cov_ever_dic,
        CASE WHEN covar.mesenteric_thrombus=1 THEN 1 ELSE 0 END AS cov_ever_mesenteric_thrombus,
        CASE WHEN covar.artery_dissect=1 THEN 1 ELSE 0 END AS cov_ever_artery_dissect,
        CASE WHEN covar.life_arrhythmia=1 THEN 1 ELSE 0 END AS cov_ever_life_arrhythmia,
        CASE WHEN covar.cardiomyopathy=1 THEN 1 ELSE 0 END AS cov_ever_cardiomyopathy,
        CASE WHEN covar.HF=1 THEN 1 ELSE 0 END AS cov_ever_hf,
        CASE WHEN covar.pericarditis=1 THEN 1 ELSE 0 END AS cov_ever_pericarditis,
        CASE WHEN covar.myocarditis = 1 THEN 1 ELSE 0 END AS cov_ever_myocarditis,
        CASE WHEN covar.unique_bnf_chapters IS NULL THEN 0 ELSE covar.unique_bnf_chapters END AS cov_unique_bnf_chaps,
        CASE WHEN covar.AMI=1 OR covar.other_arterial_embolism=1 OR covar.stroke_isch=1 THEN 1
             WHEN covar.AMI=0 AND covar.other_arterial_embolism=0 AND covar.stroke_isch=0 THEN 0 ELSE 0 END AS cov_ever_Arterial_event, 
        CASE WHEN covar.PE = 1 OR covar.VT=1 OR covar.DVT_ICVT=1 THEN 1
             WHEN covar.PE = 0 AND covar.VT=0 AND covar.DVT_ICVT=0 THEN 0 ELSE 0 END AS cov_ever_Venous_event
        
        
 FROM dars_nic_391419_j3w9t_collab.ccu002_01_inf_included_patients_rk_210702 AS cohort
 LEFT JOIN global_temp.ccu002_01_covid_cohort AS covid19_confirmed on cohort.NHS_NUMBER_DEID = covid19_confirmed.person_id_deid
 LEFT JOIN global_temp.ICVT_pregnancy AS ICVT_pregnancy on cohort.NHS_NUMBER_DEID = ICVT_pregnancy.NHS_NUMBER_DEID
 LEFT JOIN global_temp.artery_dissect AS artery_dissect on cohort.NHS_NUMBER_DEID = artery_dissect.NHS_NUMBER_DEID
 LEFT JOIN global_temp.angina AS angina on cohort.NHS_NUMBER_DEID = angina.NHS_NUMBER_DEID
 LEFT JOIN global_temp.other_DVT AS other_DVT on cohort.NHS_NUMBER_DEID = other_DVT.NHS_NUMBER_DEID
 LEFT JOIN global_temp.DVT_ICVT AS DVT_ICVT on cohort.NHS_NUMBER_DEID = DVT_ICVT.NHS_NUMBER_DEID
 LEFT JOIN global_temp.DVT_pregnancy AS DVT_pregnancy on cohort.NHS_NUMBER_DEID = DVT_pregnancy.NHS_NUMBER_DEID
 LEFT JOIN global_temp.DVT_DVT AS DVT_DVT on cohort.NHS_NUMBER_DEID = DVT_DVT.NHS_NUMBER_DEID
 LEFT JOIN global_temp.fracture AS fracture on cohort.NHS_NUMBER_DEID = fracture.NHS_NUMBER_DEID
 LEFT JOIN global_temp.thrombocytopenia AS thrombocytopenia on cohort.NHS_NUMBER_DEID = thrombocytopenia.NHS_NUMBER_DEID
 LEFT JOIN global_temp.life_arrhythmia AS life_arrhythmia on cohort.NHS_NUMBER_DEID = life_arrhythmia.NHS_NUMBER_DEID
 LEFT JOIN global_temp.pericarditis AS pericarditis on cohort.NHS_NUMBER_DEID = pericarditis.NHS_NUMBER_DEID
 LEFT JOIN global_temp.TTP AS TTP on cohort.NHS_NUMBER_DEID = TTP.NHS_NUMBER_DEID
 LEFT JOIN global_temp.mesenteric_thrombus AS mesenteric_thrombus on cohort.NHS_NUMBER_DEID = mesenteric_thrombus.NHS_NUMBER_DEID
 LEFT JOIN global_temp.DIC AS DIC on cohort.NHS_NUMBER_DEID = DIC.NHS_NUMBER_DEID
 LEFT JOIN global_temp.myocarditis AS myocarditis on cohort.NHS_NUMBER_DEID = myocarditis.NHS_NUMBER_DEID
 LEFT JOIN global_temp.stroke_TIA AS stroke_TIA on cohort.NHS_NUMBER_DEID = stroke_TIA.NHS_NUMBER_DEID
 LEFT JOIN global_temp.stroke_isch AS stroke_isch on cohort.NHS_NUMBER_DEID = stroke_isch.NHS_NUMBER_DEID
 LEFT JOIN global_temp.other_arterial_embolism AS other_arterial_embolism on cohort.NHS_NUMBER_DEID = other_arterial_embolism.NHS_NUMBER_DEID
 LEFT JOIN global_temp.unstable_angina AS unstable_angina on cohort.NHS_NUMBER_DEID = unstable_angina.NHS_NUMBER_DEID
 LEFT JOIN global_temp.PE AS PE on cohort.NHS_NUMBER_DEID = PE.NHS_NUMBER_DEID
 LEFT JOIN global_temp.AMI AS AMI on cohort.NHS_NUMBER_DEID = AMI.NHS_NUMBER_DEID
 LEFT JOIN global_temp.HF AS HF on cohort.NHS_NUMBER_DEID = HF.NHS_NUMBER_DEID
 LEFT JOIN global_temp.portal_vein_thrombosis AS portal_vein_thrombosis on cohort.NHS_NUMBER_DEID = portal_vein_thrombosis.NHS_NUMBER_DEID
 LEFT JOIN global_temp.cardiomyopathy AS cardiomyopathy on cohort.NHS_NUMBER_DEID = cardiomyopathy.NHS_NUMBER_DEID
 LEFT JOIN global_temp.stroke_SAH_HS AS stroke_SAH_HS on cohort.NHS_NUMBER_DEID = stroke_SAH_HS.NHS_NUMBER_DEID
 LEFT JOIN global_temp.DVT_event AS DVT_event on cohort.NHS_NUMBER_DEID = DVT_event.NHS_NUMBER_DEID
 LEFT JOIN global_temp.ICVT_event AS ICVT_event on cohort.NHS_NUMBER_DEID = ICVT_event.NHS_NUMBER_DEID
 LEFT JOIN global_temp.Arterial_event AS Arterial_event on cohort.NHS_NUMBER_DEID = Arterial_event.NHS_NUMBER_DEID
 LEFT JOIN global_temp.Venous_event AS Venous_event on cohort.NHS_NUMBER_DEID = Venous_event.NHS_NUMBER_DEID
 LEFT JOIN global_temp.Haematological_event AS Haematological_event on cohort.NHS_NUMBER_DEID = Haematological_event.NHS_NUMBER_DEID
 LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_01_included_patients_allcovariates AS covar on cohort.NHS_NUMBER_DEID = covar.NHS_NUMBER_DEID

# COMMAND ----------

 # MAGIC %md
 ### Apply exclusions

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_cohort AS
 SELECT *
 FROM global_temp.ccu002_01_cohort_full
 WHERE (cov_age<111) -- Remove people older than 110
 AND ((cov_sex=2) OR (cov_sex=1 AND cov_cocp_meds=0 AND cov_hrt_meds=0)) -- Remove men indicated for COCP or HRT as these drugs are primarily indicated for women
 AND ((exp_confirmed_covid19_date IS NULL) OR (death_date IS NULL) OR (exp_confirmed_covid19_date < death_date)) -- Remove people who die before exposure
 
