# Databricks notebook source
 #MAGIC %md # CCU002_01_codelists
 
 **Description** This notebook creates global temporary views for the drug codelists needed for the CCU002_01 infection project.
 
 **Author(s)** 
 
 **Project(s)** CCU002_01
 
 **Reviewer(s)** 
 
 **Date last updated**  
 **Date last reviewed**
 
 **Data last run** 
 
 **Data input** 
 `dars_nic_391419_j3w9t_collab.ccu002_01_primary_care_meds_dars_nic_391419_j3w9t`
 
 **Data output** 
 
 `global_temp.ccu002_01_drug_codelists`
 
 `global_temp.ccu002_01_infection_covariates`
 
 `global_temp.ccu002_01_smokingstatus_SNOMED`
 
 `global_temp.ccu002_01_pregnancy_birth_sno`
 
 `global_temp.cccu002_01_prostate_cancer_sno`
 
 `global_temp.ccu002_01_liver_disease`
 
 `global_temp.ccu002_01_depression`
 
 **Software and versions** SQL, Python
 
 **Packages and versions** Not applicable

# COMMAND ----------

%run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/wrang000_functions"

# COMMAND ----------

#Data sets parameters
project_prefix = 'ccu002_01_'
collab_database_name = 'dars_nic_391419_j3w9t_collab'
meds_data = 'primary_care_meds_dars_nic_391419_j3w9t'


#Final table parameters
codelist_final_name = 'codelist'


# COMMAND ----------

 #MAGIC %md ## Define drug codelists

# COMMAND ----------

#Create global temporary view containing all codelists
spark.sql(f"""
CREATE
OR REPLACE GLOBAL TEMPORARY VIEW ccu002_01_drug_codelists AS
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'antiplatelet' AS codelist
FROM
 {collab_database_name}.{project_prefix}{meds_data}{frozen_new_initials_date}
WHERE
  left(PrescribedBNFCode, 4) = '0209'
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'bp_lowering' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  left(PrescribedBNFCode, 9) = '0205053A0' -- aliskiren
  OR left(PrescribedBNFCode, 6) = '020504' -- alpha blockers
  OR (
    left(PrescribedBNFCode, 4) = '0204' -- beta blockers
    AND NOT (
      left(PrescribedBNFCode, 9) = '0204000R0' -- exclude propranolol
      OR left(PrescribedBNFCode, 9) = '0204000Q0' -- exclude propranolol
    )
  )
  OR left(PrescribedBNFCode, 6) = '020602' -- calcium channel blockers
  OR (
    left(PrescribedBNFCode, 6) = '020502' -- centrally acting antihypertensives
    AND NOT (
      left(PrescribedBNFCode, 8) = '0205020G' -- guanfacine because it is only used for ADHD
      OR left(PrescribedBNFCode, 9) = '0205052AE' -- drugs for heart failure, not for hypertension
    )
  )
  OR left(PrescribedBNFCode, 6) = '020203' -- potassium sparing diuretics
  OR left(PrescribedBNFCode, 6) = '020201' -- thiazide diuretics
  OR left(PrescribedBNFCode, 6) = '020501' -- vasodilator antihypertensives
  OR left(PrescribedBNFCode, 7) = '0205051' -- angiotensin-converting enzyme inhibitors
  OR left(PrescribedBNFCode, 7) = '0205052' -- angiotensin-II receptor antagonists
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'lipid_lowering' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  left(PrescribedBNFCode, 4) = '0212'
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'anticoagulant' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  (
    left(PrescribedBNFCode, 6) = '020802'
    AND NOT (
      left(PrescribedBNFCode, 8) = '0208020I'
      OR left(PrescribedBNFCode, 8) = '0208020W'
    )
  )
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'cocp' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  left(PrescribedBNFCode, 6) = '070301'
UNION ALL
SELECT
  DISTINCT PrescribedBNFCode AS code,
  PrescribedBNFName AS term,
  'BNF' AS system,
  'hrt' AS codelist
FROM
  {collab_database_name}.{project_prefix}{meds_data}
WHERE
  left(PrescribedBNFCode, 7) = '0604011' """)

# COMMAND ----------

 #MAGIC %md ## Define smoking status codelists 

# COMMAND ----------

# MAGIC %sql
 
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_smokingstatus_SNOMED  AS
 
 SELECT *
 FROM VALUES
 
 ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
 ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
 ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
 ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
 ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
 ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
 ("230058003","Pipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
 ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
 ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
 ("230065006","Chain smoker (finding)","Current-smoker","Heavy"),
 ("266918002","Tobacco smoking consumption (observable entity)","Current-smoker","Unknown"),
 ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
 ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
 ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
 ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
 ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
 ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
 ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
 ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
 ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
 ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
 ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
 ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
 ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
 ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
 ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
 ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
 ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
 ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
 ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
 ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
 ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
 ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
 ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
 ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
 ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
 ("77176002","Smoker (finding)","Current-smoker","Unknown"),
 ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
 ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
 ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
 ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
 ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
 ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
 ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
 ("160625004","Date ceased smoking (observable entity)","Current-smoker","Unknown"),
 ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
 ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
 ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
 ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
 ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
 ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
 ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
 ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
 ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
 ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
 ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
 ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
 ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
 ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
 ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
 ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
 ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
 ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
 ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
 ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
 ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
 ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
 ("401201003","Cigarette pack-years (observable entity)","Current-smoker","Unknown"),
 ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
 ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
 ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
 ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
 ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
 ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
 ("77176002","Smoker (finding)","Current-smoker","Unknown"),
 ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
 ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
 ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
 ("1092041000000100","Ex-very heavy smoker (40+/day) (finding)","Ex-smoker","Unknown"),
 ("1092091000000100","Ex-moderate smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
 ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
 ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
 ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
 ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
 ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
 ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
 ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
 ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
 ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
 ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
 ("1092031000000100","Ex-smoker amount unknown (finding)","Ex-smoker","Unknown"),
 ("1092071000000100","Ex-heavy smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
 ("1092111000000100","Ex-light smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
 ("1092131000000100","Ex-trivial smoker (<1/day) (finding)","Ex-smoker","Unknown"),
 ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
 ("160625004","Date ceased smoking (observable entity)","Ex-smoker","Unknown"),
 ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
 ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
 ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
 ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
 ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
 ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
 ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
 ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
 ("1092041000000100","Ex-very heavy smoker (40+/day) (finding)","Ex-smoker","Unknown"),
 ("1092071000000100","Ex-heavy smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
 ("1092111000000100","Ex-light smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
 ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
 ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
 ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
 ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
 ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
 ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
 ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
 ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
 ("1092031000000100","Ex-smoker amount unknown (finding)","Ex-smoker","Unknown"),
 ("1092091000000100","Ex-moderate smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
 ("1092131000000100","Ex-trivial smoker (<1/day) (finding)","Ex-smoker","Unknown"),
 ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
 ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
 ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
 ("230058003","Pipe tobacco consumption (observable entity)","Ex-smoker","Unknown"),
 ("230065006","Chain smoker (finding)","Ex-smoker","Unknown"),
 ("266918002","Tobacco smoking consumption (observable entity)","Ex-smoker","Unknown"),
 ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
 ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
 ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
 ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
 ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
 ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
 ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
 ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
 ("266919005","Never smoked tobacco (finding)","Never-smoker","NA"),
 ("221000119102","Never smoked any substance (finding)","Never-smoker","NA"),
 ("266919005","Never smoked tobacco (finding)","Never-smoker","NA")
 
 AS tab(conceptID, description, smoking_status, severity);

# COMMAND ----------

#MAGIC #MAGIC %md ## Pregnancy & birth codelist

# COMMAND ----------

 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ccu002_01_pregnancy_birth_sno AS
 SELECT *
 FROM VALUES
 ("171057006","Pregnancy alcohol education (procedure)"),
 ("72301000119103","Asthma in pregnancy (disorder)"),
 ("10742121000119104","Asthma in mother complicating childbirth (disorder)"),
 ("10745291000119103","Malignant neoplastic disease in mother complicating childbirth (disorder)"),
 ("10749871000119100","Malignant neoplastic disease in pregnancy (disorder)"),
 ("20753005","Hypertensive heart disease complicating AND/OR reason for care during pregnancy (disorder)"),
 ("237227006","Congenital heart disease in pregnancy (disorder)"),
 ("169501005","Pregnant, diaphragm failure (finding)"),
 ("169560008","Pregnant - urine test confirms (finding)"),
 ("169561007","Pregnant - blood test confirms (finding)"),
 ("169562000","Pregnant - vaginal examination confirms (finding)"),
 ("169565003","Pregnant - planned (finding)"),
 ("169566002","Pregnant - unplanned - wanted (finding)"),
 ("413567003","Aplastic anemia associated with pregnancy (disorder)"),
 ("91948008","Asymptomatic human immunodeficiency virus infection in pregnancy (disorder)"),
 ("169488004","Contraceptive intrauterine device failure - pregnant (finding)"),
 ("169508004","Pregnant, sheath failure (finding)"),
 ("169564004","Pregnant - on abdominal palpation (finding)"),
 ("77386006","Pregnant (finding)"),
 ("10746341000119109","Acquired immune deficiency syndrome complicating childbirth (disorder)"),
 ("10759351000119103","Sickle cell anemia in mother complicating childbirth (disorder)"),
 ("10757401000119104","Pre-existing hypertensive heart and chronic kidney disease in mother complicating childbirth (disorder)"),
 ("10757481000119107","Pre-existing hypertensive heart and chronic kidney disease in mother complicating pregnancy (disorder)"),
 ("10757441000119102","Pre-existing hypertensive heart disease in mother complicating childbirth (disorder)"),
 ("10759031000119106","Pre-existing hypertensive heart disease in mother complicating pregnancy (disorder)"),
 ("1474004","Hypertensive heart AND renal disease complicating AND/OR reason for care during childbirth (disorder)"),
 ("199006004","Pre-existing hypertensive heart disease complicating pregnancy, childbirth and the puerperium (disorder)"),
 ("199007008","Pre-existing hypertensive heart and renal disease complicating pregnancy, childbirth and the puerperium (disorder)"),
 ("22966008","Hypertensive heart AND renal disease complicating AND/OR reason for care during pregnancy (disorder)"),
 ("59733002","Hypertensive heart disease complicating AND/OR reason for care during childbirth (disorder)"),
 ("171054004","Pregnancy diet education (procedure)"),
 ("106281000119103","Pre-existing diabetes mellitus in mother complicating childbirth (disorder)"),
 ("10754881000119104","Diabetes mellitus in mother complicating childbirth (disorder)"),
 ("199225007","Diabetes mellitus during pregnancy - baby delivered (disorder)"),
 ("237627000","Pregnancy and type 2 diabetes mellitus (disorder)"),
 ("609563008","Pre-existing diabetes mellitus in pregnancy (disorder)"),
 ("609566000","Pregnancy and type 1 diabetes mellitus (disorder)"),
 ("609567009","Pre-existing type 2 diabetes mellitus in pregnancy (disorder)"),
 ("199223000","Diabetes mellitus during pregnancy, childbirth and the puerperium (disorder)"),
 ("199227004","Diabetes mellitus during pregnancy - baby not yet delivered (disorder)"),
 ("609564002","Pre-existing type 1 diabetes mellitus in pregnancy (disorder)"),
 ("76751001","Diabetes mellitus in mother complicating pregnancy, childbirth AND/OR puerperium (disorder)"),
 ("526961000000105","Pregnancy advice for patients with epilepsy (procedure)"),
 ("527041000000108","Pregnancy advice for patients with epilepsy not indicated (situation)"),
 ("527131000000100","Pregnancy advice for patients with epilepsy declined (situation)"),
 ("10753491000119101","Gestational diabetes mellitus in childbirth (disorder)"),
 ("40801000119106","Gestational diabetes mellitus complicating pregnancy (disorder)"),
 ("10562009","Malignant hypertension complicating AND/OR reason for care during childbirth (disorder)"),
 ("198944004","Benign essential hypertension complicating pregnancy, childbirth and the puerperium - delivered (disorder)"),
 ("198945003","Benign essential hypertension complicating pregnancy, childbirth and the puerperium - delivered with postnatal complication (disorder)"),
 ("198946002","Benign essential hypertension complicating pregnancy, childbirth and the puerperium - not delivered (disorder)"),
 ("198949009","Renal hypertension complicating pregnancy, childbirth and the puerperium (disorder)"),
 ("198951008","Renal hypertension complicating pregnancy, childbirth and the puerperium - delivered (disorder)"),
 ("198954000","Renal hypertension complicating pregnancy, childbirth and the puerperium with postnatal complication (disorder)"),
 ("199005000","Pre-existing hypertension complicating pregnancy, childbirth and puerperium (disorder)"),
 ("23717007","Benign essential hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
 ("26078007","Hypertension secondary to renal disease complicating AND/OR reason for care during childbirth (disorder)"),
 ("29259002","Malignant hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
 ("65402008","Pre-existing hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
 ("8218002","Chronic hypertension complicating AND/OR reason for care during childbirth (disorder)"),
 ("10752641000119102","Eclampsia with pre-existing hypertension in childbirth (disorder)"),
 ("118781000119108","Pre-existing hypertensive chronic kidney disease in mother complicating pregnancy (disorder)"),
 ("18416000","Essential hypertension complicating AND/OR reason for care during childbirth (disorder)"),
 ("198942000","Benign essential hypertension complicating pregnancy, childbirth and the puerperium (disorder)"),
 ("198947006","Benign essential hypertension complicating pregnancy, childbirth and the puerperium with postnatal complication (disorder)"),
 ("198952001","Renal hypertension complicating pregnancy, childbirth and the puerperium - delivered with postnatal complication (disorder)"),
 ("198953006","Renal hypertension complicating pregnancy, childbirth and the puerperium - not delivered (disorder)"),
 ("199008003","Pre-existing secondary hypertension complicating pregnancy, childbirth and puerperium (disorder)"),
 ("34694006","Pre-existing hypertension complicating AND/OR reason for care during childbirth (disorder)"),
 ("37618003","Chronic hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
 ("48552006","Hypertension secondary to renal disease complicating AND/OR reason for care during pregnancy (disorder)"),
 ("71874008","Benign essential hypertension complicating AND/OR reason for care during childbirth (disorder)"),
 ("78808002","Essential hypertension complicating AND/OR reason for care during pregnancy (disorder)"),
 ("91923005","Acquired immunodeficiency syndrome virus infection associated with pregnancy (disorder)"),
 ("10755671000119100","Human immunodeficiency virus in mother complicating childbirth (disorder)"),
 ("721166000","Human immunodeficiency virus complicating pregnancy childbirth and the puerperium (disorder)"),
 ("449369001","Stopped smoking before pregnancy (finding)"),
 ("449345000","Smoked before confirmation of pregnancy (finding)"),
 ("449368009","Stopped smoking during pregnancy (finding)"),
 ("88144003","Removal of ectopic interstitial uterine pregnancy requiring total hysterectomy (procedure)"),
 ("240154002","Idiopathic osteoporosis in pregnancy (disorder)"),
 ("956951000000104","Pertussis vaccination in pregnancy (procedure)"),
 ("866641000000105","Pertussis vaccination in pregnancy declined (situation)"),
 ("956971000000108","Pertussis vaccination in pregnancy given by other healthcare provider (finding)"),
 ("169563005","Pregnant - on history (finding)"),
 ("10231000132102","In-vitro fertilization pregnancy (finding)"),
 ("134781000119106","High risk pregnancy due to recurrent miscarriage (finding)"),
 ("16356006","Multiple pregnancy (disorder)"),
 ("237239003","Low risk pregnancy (finding)"),
 ("276367008","Wanted pregnancy (finding)"),
 ("314204000","Early stage of pregnancy (finding)"),
 ("439311009","Intends to continue pregnancy (finding)"),
 ("713575004","Dizygotic twin pregnancy (disorder)"),
 ("80997009","Quintuplet pregnancy (disorder)"),
 ("1109951000000101","Pregnancy insufficiently advanced for reliable antenatal screening (finding)"),
 ("1109971000000105","Pregnancy too advanced for reliable antenatal screening (finding)"),
 ("237238006","Pregnancy with uncertain dates (finding)"),
 ("444661007","High risk pregnancy due to history of preterm labor (finding)"),
 ("459166009","Dichorionic diamniotic twin pregnancy (disorder)"),
 ("459167000","Monochorionic twin pregnancy (disorder)"),
 ("459168005","Monochorionic diamniotic twin pregnancy (disorder)"),
 ("459171002","Monochorionic monoamniotic twin pregnancy (disorder)"),
 ("47200007","High risk pregnancy (finding)"),
 ("60810003","Quadruplet pregnancy (disorder)"),
 ("64254006","Triplet pregnancy (disorder)"),
 ("65147003","Twin pregnancy (disorder)"),
 ("713576003","Monozygotic twin pregnancy (disorder)"),
 ("171055003","Pregnancy smoking education (procedure)"),
 ("10809101000119109","Hypothyroidism in childbirth (disorder)"),
 ("428165003","Hypothyroidism in pregnancy (disorder)")
 
 AS tab(code, term)

# COMMAND ----------

#MAGIC #MAGIC %md ## Prostate cancer codelist

# COMMAND ----------

 %sql
 
 CREATE OR REPLACE GLOBAL TEMPORARY VIEW ccu002_01_prostate_cancer_sno AS
 SELECT *
 FROM VALUES
 ("126906006","Neoplasm of prostate"),
 ("81232004","Radical cystoprostatectomy"),
 ("176106009","Radical cystoprostatourethrectomy"),
 ("176261008","Radical prostatectomy without pelvic node excision"),
 ("176262001","Radical prostatectomy with pelvic node sampling"),
 ("176263006","Radical prostatectomy with pelvic lymphadenectomy"),
 ("369775001","Gleason Score 2-4: Well differentiated"),
 ("369777009","Gleason Score 8-10: Poorly differentiated"),
 ("385377005","Gleason grade finding for prostatic cancer (finding)"),
 ("394932008","Gleason prostate grade 5-7 (medium) (finding)"),
 ("399068003","Malignant tumor of prostate (disorder)"),
 ("428262008","History of malignant neoplasm of prostate (situation)")
 
 AS tab(code, term)

# COMMAND ----------

 #MAGIC %md
 ## Depression codelist

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_depression as 
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like 'depression'
 and 
 (terminology like 'SNOMED'
 or terminology like 'ICD10')

# COMMAND ----------

 #MAGIC %md
 ## Obesity codelist

# COMMAND ----------

 %sql
 create or replace global temporary view ccu002_01_BMI_obesity as
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like '%BMI_obesity'
 and 
 (terminology like 'SNOMED'
 or terminology like 'ICD10')
 union all
 SELECT *
 FROM VALUES
 ('BMI_obesity','ICD10','E66','Diagnosis of obesity','','')
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ## COPD

# COMMAND ----------

 %sql
 create or replace global temporary view ccu002_01_COPD as
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like '%COPD%'
 and 
 (terminology like 'SNOMED'
 or terminology like 'ICD10')

# COMMAND ----------

 #MAGIC %md
 ## Hypertension

# COMMAND ----------

 %sql
 create or replace global temporary view ccu002_01_hypertension as
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like '%hypertension%'
 and 
 (terminology like 'SNOMED'
 or terminology like 'ICD10'
 or terminology like 'DMD')
 union all
 select *
 from values
 ('hypertension','ICD10','I10','Essential (primary) hypertension','',''),
 ('hypertension','ICD10','I11','Hypertensive heart disease','',''),
 ('hypertension','ICD10','I12','Hypertensive renal disease','',''),
 ('hypertension','ICD10','I13','Hypertensive heart and renal disease','',''),
 ('hypertension','ICD10','I15','Secondary hypertension','','')
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ## Diabetes

# COMMAND ----------

 %sql
 create or replace global temporary view ccu002_01_diabetes as
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like '%diabetes%'
 and 
 (terminology like 'SNOMED'
 or terminology like 'ICD10'
 or terminology like 'DMD')
 union all
 select *
 from values
 ('diabetes','ICD10','E10','Insulin-dependent diabetes mellitus','',''),
 ('diabetes','ICD10','E11','Non-insulin-dependent diabetes mellitus','',''),
 ('diabetes','ICD10','E12','Malnutrition-related diabetes mellitus','',''),
 ('diabetes','ICD10','O242','Diabetes mellitus in pregnancy: Pre-existing malnutrition-related diabetes mellitus','',''),
 ('diabetes','ICD10','E13','Other specified diabetes mellitus','',''),
 ('diabetes','ICD10','E14','Unspecified diabetes mellitus','',''),
 ('diabetes','ICD10','G590','Diabetic mononeuropathy','',''),
 ('diabetes','ICD10','G632','Diabetic polyneuropathy','',''),
 ('diabetes','ICD10','H280','Diabetic cataract','',''),
 ('diabetes','ICD10','H360','Diabetic retinopathy','',''),
 ('diabetes','ICD10','M142','Diabetic arthropathy','',''),
 ('diabetes','ICD10','N083','Glomerular disorders in diabetes mellitus','',''),
 ('diabetes','ICD10','O240','Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, insulin-dependent','',''),
 ('diabetes','ICD10','O241','Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, non-insulin-dependent','',''),
 ('diabetes','ICD10','O243','Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, unspecified','','')
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Cancer

# COMMAND ----------

 %sql
 create or replace global temporary view ccu002_01_cancer as
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like '%cancer%'
 and 
 (terminology like 'SNOMED'
 or terminology like 'ICD10')

# COMMAND ----------

 #MAGIC %md
 ### Liver disease

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_liver AS
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like '%liver%'
 and 
 terminology like 'ICD10'
 union all
 SELECT
 	pheno.name, 
     CONCAT(pheno.terminology, '_SNOMEDmapped') AS terminology,
 	pheno.term AS term, 
 	mapfile.SCT_CONCEPTID AS code,
     "" AS code_type,
   "" AS RecordDate
 FROM
 	bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127 AS pheno,
 	dss_corporate.read_codes_map_ctv3_to_snomed AS mapfile 
 WHERE pheno.name LIKE  "%liver%" 
 AND pheno.terminology ="CTV3"
 AND pheno.code = mapfile.CTV3_CONCEPTID 
 AND mapfile.IS_ASSURED = 1;

# COMMAND ----------

 #MAGIC %md
 ### Dementia

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_dementia AS
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like 'dementia%'
 and 
 (terminology like 'ICD10'
 or terminology like 'SNOMED')

# COMMAND ----------

 #MAGIC %md
 ### CKD

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_ckd AS
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like 'CKD%'
 and 
 (terminology like 'ICD10'
 or terminology like 'SNOMED')

# COMMAND ----------

 #MAGIC %md
 ### Myocardial infarction

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_ami AS
 select terminology, code, term, code_type, recorddate,
 case 
 when code like 'I25.2' or code like 'I24.1' then 'AMI_covariate_only'
 else
 'AMI'
 end as new_name
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like 'AMI%'
 and 
 (terminology like 'ICD10'
 or terminology like 'SNOMED')

# COMMAND ----------

 #MAGIC %md
 ### VT

# COMMAND ----------

 %sql
 
 create or replace global temp view ccu002_01_vt as
 
 select * 
 from values
 ("DVT_DVT","ICD10","I80","Phlebitis and thrombophlebitis","1","20210127"),
 ("other_DVT","ICD10","I82.8","Other vein thrombosis","1","20210127"),
 ("other_DVT","ICD10","I82.9","Other vein thrombosis","1","20210127"),
 ("other_DVT","ICD10","I82.0","Other vein thrombosis","1","20210127"),
 ("other_DVT","ICD10","I82.3","Other vein thrombosis","1","20210127"),
 ("other_DVT","ICD10","I82.2","Other vein thrombosis","1","20210127"),
 ("DVT_pregnancy","ICD10","O22.3","Thrombosis during pregnancy and puerperium","1","20210127"),
 ("DVT_pregnancy","ICD10","O87.1","Thrombosis during pregnancy and puerperium","1","20210127"),
 ("DVT_pregnancy","ICD10","O87.9","Thrombosis during pregnancy and puerperium","1","20210127"),
 ("DVT_pregnancy","ICD10","O88.2","Thrombosis during pregnancy and puerperium","1","20210127"),
 ("ICVT_pregnancy","ICD10","O22.5","ntracranial venous thrombosis in pregnancy and puerperium","1","20210127"),
 ("ICVT_pregnancy","ICD10","O87.3","ntracranial venous thrombosis in pregnancy and puerperium","1","20210127"),
 ("portal_vein_thrombosis","ICD10","I81","Portal vein thrombosis","1","20210127"),
 ("VT_covariate_only","ICD10","O08.2","Embolism following abortion and ectopic and molar pregnancy","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

 #MAGIC %md
 ### DVT_ICVT

# COMMAND ----------

 %sql
 
 create or replace global temp view ccu002_01_DVT_ICVT as
 
 select * 
 from values
 ("DVT_ICVT","ICD10","G08","Intracranial venous thrombosis","1","20210127"),
 ("DVT_ICVT","ICD10","I67.6","Intracranial venous thrombosis","1","20210127"),
 ("DVT_ICVT","ICD10","I63.6","Intracranial venous thrombosis","1","20210127"),
 ("DVT_ICVT_covariate_only","SNOMED","195230003","Cerebral infarction due to cerebral venous thrombosis,  nonpyogenic","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

 #MAGIC %md
 ### PE

# COMMAND ----------

 %sql
 
 create or replace global temp view ccu002_01_PE as
 
 select * 
 from values
 
 ("PE","ICD10","I26.0","Pulmonary embolism","1","20210127"),
 ("PE","ICD10","I26.9","Pulmonary embolism","1","20210127"),
 ("PE_covariate_only","SNOMED","438773007","Pulmonary embolism with mention of acute cor pulmonale","1","20210127"),
 ("PE_covariate_only","SNOMED","133971000119108","Pulmonary embolism with mention of acute cor pulmonale","1","20210127")
 
 
 as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

 #MAGIC %md
 ### Ischemic stroke 

# COMMAND ----------

 %sql
 
 create or replace global temp view ccu002_01_stroke_IS as
 
 with cte as
 (select terminology, code, term, code_type, RecordDate,
 case 
 when terminology like 'SNOMED' then 'stroke_isch'
 end as new_name
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like 'stroke_IS'
 AND terminology ='SNOMED'
 AND code_type='1' 
 AND RecordDate='20210127'
 )
 
 select * 
 from values
 ("stroke_isch","ICD10","I63.0","Cerebral infarction","1","20210127"),
 ("stroke_isch","ICD10","I63.1","Cerebral infarction","1","20210127"),
 ("stroke_isch","ICD10","I63.2","Cerebral infarction","1","20210127"),
 ("stroke_isch","ICD10","I63.3","Cerebral infarction","1","20210127"),
 ("stroke_isch","ICD10","I63.4","Cerebral infarction","1","20210127"),
 ("stroke_isch","ICD10","I63.5","Cerebral infarction","1","20210127"),
 ("stroke_isch","ICD10","I63.8","Cerebral infarction","1","20210127"),
 ("stroke_isch","ICD10","I63.9","Cerebral infarction","1","20210127")
 as tab(name, terminology, code, term, code_type, RecordDate)
 union all
 select new_name as name, terminology, code, term, code_type, RecordDate
 from cte

# COMMAND ----------

 #MAGIC %md
 ### Stroke, NOS 

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_stroke_NOS AS
 
 select *,
 case 
 when name like 'stroke_NOS%' then 'stroke_isch'
 end as new_name
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like 'stroke_NOS%'
 and 
 (terminology like 'ICD10'
 or terminology like 'SNOMED')
 and term != 'Stroke in the puerperium (disorder)'
 and term != 'Cerebrovascular accident (disorder)'
 and term != 'Right sided cerebral hemisphere cerebrovascular accident (disorder)'
 and term != 'Left sided cerebral hemisphere cerebrovascular accident (disorder)'
 and term != 'Brainstem stroke syndrome (disorder)'
 AND code_type='1' 
 AND RecordDate='20210127'

# COMMAND ----------

 #MAGIC %md
 ### stroke_SAH

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_stroke_SAH AS
 select terminology, code, term, code_type, recorddate,
 case 
 when terminology like 'ICD10' then 'stroke_SAH_HS'
 end as new_name
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like 'stroke_SAH%'
 and 
 terminology like 'ICD10'

# COMMAND ----------

 #MAGIC %md
 ### stroke_HS

# COMMAND ----------

 %sql
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_stroke_HS AS
 select terminology, code, term, code_type, recorddate,
 case 
 when terminology like 'SNOMED' then 'stroke_SAH_HS_covariate_only'
 else 'stroke_SAH_HS'
 end as new_name
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like 'stroke_HS%'
 and 
 (terminology like 'ICD10'
 or terminology like 'SNOMED')

# COMMAND ----------

 #MAGIC %md
 ### Thrombophilia

# COMMAND ----------

 %sql
 
 create or replace global temp view ccu002_01_thrombophilia as
 
 select * 
 from values
 ("thrombophilia","ICD10","D68.5","Primary thrombophilia","1","20210127"),
 ("thrombophilia","ICD10","D68.6","Other thrombophilia","1","20210127"),
 ("thrombophilia","SNOMED","439001009","Acquired thrombophilia","1","20210127"),
 ("thrombophilia","SNOMED","441882000","History of thrombophilia","1","20210127"),
 ("thrombophilia","SNOMED","439698008","Primary thrombophilia","1","20210127"),
 ("thrombophilia","SNOMED","234467004","Thrombophilia","1","20210127"),
 ("thrombophilia","SNOMED","441697004","Thrombophilia associated with pregnancy","1","20210127"),
 ("thrombophilia","SNOMED","442760001","Thrombophilia caused by antineoplastic agent therapy","1","20210127"),
 ("thrombophilia","SNOMED","442197003","Thrombophilia caused by drug therapy","1","20210127"),
 ("thrombophilia","SNOMED","442654007","Thrombophilia caused by hormone therapy","1","20210127"),
 ("thrombophilia","SNOMED","442363001","Thrombophilia caused by vascular device","1","20210127"),
 ("thrombophilia","SNOMED","439126002","Thrombophilia due to acquired antithrombin III deficiency","1","20210127"),
 ("thrombophilia","SNOMED","439002002","Thrombophilia due to acquired protein C deficiency","1","20210127"),
 ("thrombophilia","SNOMED","439125003","Thrombophilia due to acquired protein S deficiency","1","20210127"),
 ("thrombophilia","SNOMED","441079006","Thrombophilia due to antiphospholipid antibody","1","20210127"),
 ("thrombophilia","SNOMED","441762006","Thrombophilia due to immobilisation","1","20210127"),
 ("thrombophilia","SNOMED","442078001","Thrombophilia due to malignant neoplasm","1","20210127"),
 ("thrombophilia","SNOMED","441946009","Thrombophilia due to myeloproliferative disorder","1","20210127"),
 ("thrombophilia","SNOMED","441990004","Thrombophilia due to paroxysmal nocturnal haemoglobinuria","1","20210127"),
 ("thrombophilia","SNOMED","441945008","Thrombophilia due to trauma","1","20210127"),
 ("thrombophilia","SNOMED","442121006","Thrombophilia due to vascular anomaly","1","20210127"),
 ("thrombophilia","SNOMED","439698008","Hereditary thrombophilia","1","20210127"),
 ("thrombophilia","SNOMED","783250007","Hereditary thrombophilia due to congenital histidine-rich (poly-L) glycoprotein deficiency","1",	"20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

 #MAGIC %md
 ### Thrombocytopenia

# COMMAND ----------

 %sql
 
 create or replace global temp view ccu002_01_TCP as
 
 select * 
 from values
 ("thrombocytopenia","ICD10","D69.3","Thrombocytopenia","1","20210127"),
 ("thrombocytopenia","ICD10","D69.4","Thrombocytopenia","1","20210127"),
 ("thrombocytopenia","ICD10","D69.5","Thrombocytopenia","1","20210127"),
 ("thrombocytopenia","ICD10","D69.6","Thrombocytopenia","1","20210127"),
 ("TTP","ICD10","M31.1","Thrombotic microangiopathy","1","20210127"),
 ("TCP_covariate_only","SNOMED","74576004","Acquired thrombocytopenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","439007008","Acquired thrombotic thrombocytopenic purpura","1","20210127"),
 ("TCP_covariate_only","SNOMED","28505005","Acute idiopathic thrombocytopenic purpura","1","20210127"),
 ("TCP_covariate_only","SNOMED","128091003","Autoimmune thrombocytopenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","13172003","Autoimmune thrombocytopenic purpura","1","20210127"),
 ("TCP_covariate_only","SNOMED","438476003","Autoimmune thrombotic thrombocytopenic purpura","1","20210127"),
 ("TCP_covariate_only","SNOMED","111588002","Heparin associated thrombotic thrombocytopenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","73397007","Heparin induced thrombocytopaenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","438492008","Hereditary thrombocytopenic disorder","1","20210127"),
 ("TCP_covariate_only","SNOMED","441511006","History of immune thrombocytopenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","49341000119108","History of thrombocytopaenia",	"1","20210127"),
 ("TCP_covariate_only","SNOMED","726769004","HIT (Heparin induced thrombocytopenia) antibody","1","20210127"),
 ("TCP_covariate_only","SNOMED","371106008","Idiopathic maternal thrombocytopenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","32273002","Idiopathic thrombocytopenic purpura","1","20210127"),
 ("TCP_covariate_only","SNOMED","2897005","Immune thrombocytopenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","32273002","Immune thrombocytopenic purpura","1","20210127"),
 ("TCP_covariate_only","SNOMED","36070007","Immunodeficiency with thrombocytopenia AND eczema","1","20210127"),
 ("TCP_covariate_only","SNOMED","33183004","Post infectious thrombocytopenic purpura","1","20210127"),
 ("TCP_covariate_only","SNOMED","267534000","Primary thrombocytopenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","154826009","Secondary thrombocytopenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","866152006","Thrombocytopenia due to 2019 novel coronavirus","1","20210127"),
 ("TCP_covariate_only","SNOMED","82190001","Thrombocytopenia due to defective platelet production","1","20210127"),
 ("TCP_covariate_only","SNOMED","78345002","Thrombocytopenia due to diminished platelet production","1","20210127"),
 ("TCP_covariate_only","SNOMED","191323001","Thrombocytopenia due to extracorporeal circulation of blood","1","20210127"),
 ("TCP_covariate_only","SNOMED","87902006",	"Thrombocytopenia due to non-immune destruction","1","20210127"),
 ("TCP_covariate_only","SNOMED","302873008","Thrombocytopenic purpura",	"1","20210127"),
 ("TCP_covariate_only","SNOMED","417626001","Thrombocytopenic purpura associated with metabolic disorder","1","20210127"),
 ("TCP_covariate_only","SNOMED","402653004","Thrombocytopenic purpura due to defective platelet production","1","20210127"),
 ("TCP_covariate_only","SNOMED","402654005","Thrombocytopenic purpura due to platelet consumption","1","20210127"),
 ("TCP_covariate_only","SNOMED","78129009","Thrombotic thrombocytopenic purpura","1","20210127"),
 ("TCP_covariate_only","SNOMED","441322009","Drug induced thrombotic thrombocytopenic purpura","1","20210127"),
 ("TCP_covariate_only","SNOMED","19307009","Drug-induced immune thrombocytopenia","1","20210127"),
 ("TCP_covariate_only","SNOMED","783251006","Hereditary thrombocytopenia with normal platelets","1","20210127"),
 ("TCP_covariate_only","SNOMED","191322006","Thrombocytopenia caused by drugs","1","20210127")
 
 
 as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

 #MAGIC %md
 ### Retinal Infarction

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_retinal_infarction as
 select *
 from values
 ("stroke_isch","ICD10","H34","Retinal vascular occlusions","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Other arterial embolism

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_arterial_embolism as
 select *
 from values
 ("other_arterial_embolism","ICD10","I74","arterial embolism and thrombosis","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Disseminated intravascular coagulation (DIC)

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_DIC as
 select *
 from values
 ("DIC","ICD10","D65","Disseminated intravascular coagulation","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Mesenteric thrombus

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_mesenteric_thrombus as
 select *
 from values
 ("mesenteric_thrombus","ICD10","K55.9","Acute vascular disorders of intestine","1","20210127"),
 ("mesenteric_thrombus","ICD10","K55.0","Acute vascular disorders of intestine","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Spinal stroke

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_spinal_stroke as
 select *
 from values
 ("stroke_isch","ICD10","G95.1","Avascular myelopathies (arterial or venous)","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_fracture as
 select *
 from values
 
 ("fracture","ICD10","S720","Upper leg fracture","",""),
 ("fracture","ICD10","S721","Upper leg fracture","",""),
 ("fracture","ICD10","S723","Upper leg fracture","",""),
 ("fracture","ICD10","S724","Upper leg fracture","",""),
 ("fracture","ICD10","S727","Upper leg fracture","",""),
 ("fracture","ICD10","S728","Upper leg fracture","",""),
 ("fracture","ICD10","S729","Upper leg fracture","",""),
 ("fracture","ICD10","S820","Lower leg fracture","",""),
 ("fracture","ICD10","S821","Lower leg fracture","",""),
 ("fracture","ICD10","S822","Lower leg fracture","",""),
 ("fracture","ICD10","S823","Lower leg fracture","",""),
 ("fracture","ICD10","S824","Lower leg fracture","",""),
 ("fracture","ICD10","S825","Lower leg fracture","",""),
 ("fracture","ICD10","S826","Lower leg fracture","",""),
 ("fracture","ICD10","S827","Lower leg fracture","",""),
 ("fracture","ICD10","S828","Lower leg fracture","",""),
 ("fracture","ICD10","S829","Lower leg fracture","",""),
 ("fracture","ICD10","S920","Foot fracture","",""),
 ("fracture","ICD10","S921","Foot fracture","",""),
 ("fracture","ICD10","S922","Foot fracture","",""),
 ("fracture","ICD10","S923","Foot fracture","",""),
 ("fracture","ICD10","S927","Foot fracture","",""),
 ("fracture","ICD10","S929","Foot fracture","",""),
 ("fracture","ICD10","T12","Fracture of lower limb","",""),
 ("fracture","ICD10","T025","Fractures involving multiple regions of both lower limbs","",""),
 ("fracture","ICD10","T023","Fractures involving multiple regions of both lower limbs","","")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Arterial dissection

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_artery_dissect as
 select *
 from values
 
 ("artery_dissect","ICD10","I71.0","Dissection of aorta [any part]","",""),
 ("artery_dissect","ICD10","I72.0","Aneurysm and dissection of carotid artery","",""),
 ("artery_dissect","ICD10","I72.1","Aneurysm and dissection of artery of upper extremity","",""),
 ("artery_dissect","ICD10","I72.6","Dissection of vertebral artery","","")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Life threatening arrhythmias

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_life_arrhythmias as
 select *
 from values
 
 ("life_arrhythmia","ICD10","I46.0","Cardiac arrest with successful resuscitation","",""),
 ("life_arrhythmia","ICD10","I46.1 ","Sudden cardiac death, so described","",""),
 ("life_arrhythmia","ICD10","I46.9","Cardiac arrest, unspecified","",""),
 ("life_arrhythmia","ICD10","I47.0","Re-entry ventricular arrhythmia","",""),
 ("life_arrhythmia","ICD10","I47.2","Ventricular tachycardia","",""),
 ("life_arrhythmia","ICD10","I49.0","Ventricular fibrillation and flutter","","")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Cardiomyopathy

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_cardiomyopathy as
 select *
 from values
 
 ("cardiomyopathy","ICD10","I42.0","Dilated cardiomyopathy","1","20210127"),
 ("cardiomyopathy","ICD10","I42.3","Endomyocardial (eosinophilic) disease","1","20210127"),
 ("cardiomyopathy","ICD10","I42.5","Other restrictive cardiomyopathy","1","20210127"),
 ("cardiomyopathy","ICD10","I42.7","Cardiomyopathy due to drugs and other external agents","1","20210127"),
 ("cardiomyopathy","ICD10","I42.8","Other cardiomyopathies","1","20210127"),
 ("cardiomyopathy","ICD10","I42.9","Cardiomyopathy, unspecified","1","20210127"),
 ("cardiomyopathy","ICD10","I43","Cardiomyopathy in diseases classified elsewhere","1","20210127"),
 ("cardiomyopathy","ICD10","I25.5 ","Ischaemic cardiomyopathy","1","20210127"),
 ("cardiomyopathy","ICD10","O90.3 ","Cardiomyopathy in the puerperium","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate)
 union all
 select *
 from bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
 where name like 'cardiomyopathy'
 and terminology like 'SNOMED'

# COMMAND ----------

 #MAGIC %md
 ### Heart failure

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_HF as
 select *
 from values
 
 ("HF","ICD10","I50","Heart failure","",""),
 ("HF","ICD10","I11.0","Hypertensive heart disease with (congestive) heart failure","",""),
 ("HF","ICD10","I13.0","Hypertensive heart and renal disease with (congestive) heart failure","",""),
 ("HF","ICD10","I13.2","Hypertensive heart and renal disease with both (congestive) heart failure and renal failure","",""),
 ("HF","SNOMED","10335000","Chronic right-sided heart failure (disorder)","1","20210127"),
 ("HF","SNOMED","10633002","Acute congestive heart failure","1","20210127"),
 ("HF","SNOMED","42343007","Congestive heart failure","1","20210127"),
 ("HF","SNOMED","43736008","Rheumatic left ventricular failure","1","20210127"),
 ("HF","SNOMED","48447003","Chronic heart failure (disorder)","1","20210127"),
 ("HF","SNOMED","56675007","Acute heart failure","1","20210127"),
 ("HF","SNOMED","71892000","Cardiac asthma","1","20210127"),
 ("HF","SNOMED","79955004","Chronic cor pulmonale","1","20210127"),
 ("HF","SNOMED","83105008","Malignant hypertensive heart disease with congestive heart failure","1","20210127"),
 ("HF","SNOMED","84114007","Heart failure","1","20210127"),
 ("HF","SNOMED","85232009","Left heart failure","1","20210127"),
 ("HF","SNOMED","87837008","Chronic pulmonary heart disease","1","20210127"),
 ("HF","SNOMED","88805009","Chronic congestive heart failure","1","20210127"),
 ("HF","SNOMED","92506005","Biventricular congestive heart failure","1","20210127"),
 ("HF","SNOMED","128404006","Right heart failure","1","20210127"),
 ("HF","SNOMED","134401001","Left ventricular systolic dysfunction","1","20210127"),
 ("HF","SNOMED","134440006","Referral to heart failure clinic","1","20210127"),
 ("HF","SNOMED","194767001","Benign hypertensive heart disease with congestive cardiac failure","1","20210127"),
 ("HF","SNOMED","194779001","Hypertensive heart and renal disease with (congestive) heart failure","1","20210127"),
 ("HF","SNOMED","194781004","Hypertensive heart and renal disease with both (congestive) heart failure and renal failure","1","20210127"),
 ("HF","SNOMED","195111005","Decompensated cardiac failure","1","20210127"),
 ("HF","SNOMED","195112003","Compensated cardiac failure","1","20210127"),
 ("HF","SNOMED","195114002","Acute left ventricular failure","1","20210127"),
 ("HF","SNOMED","206586007","Congenital cardiac failure","1","20210127"),
 ("HF","SNOMED","233924009","Heart failure as a complication of care (disorder)","1","20210127"),
 ("HF","SNOMED","275514001","Impaired left ventricular function","1","20210127"),
 ("HF","SNOMED","314206003","Refractory heart failure (disorder)","1","20210127"),
 ("HF","SNOMED","367363000","Right ventricular failure","1","20210127"),
 ("HF","SNOMED","407596008","Echocardiogram shows left ventricular systolic dysfunction (finding)","1","20210127"),
 ("HF","SNOMED","420300004","New York Heart Association Classification - Class I (finding)","1","20210127"),
 ("HF","SNOMED","420913000","New York Heart Association Classification - Class III (finding)","1","20210127"),
 ("HF","SNOMED","421704003","New York Heart Association Classification - Class II (finding)","1","20210127"),
 ("HF","SNOMED","422293003","New York Heart Association Classification - Class IV (finding)","1","20210127"),
 ("HF","SNOMED","426263006","Congestive heart failure due to left ventricular systolic dysfunction (disorder)","1","20210127"),
 ("HF","SNOMED","426611007","Congestive heart failure due to valvular disease (disorder)","1","20210127"),
 ("HF","SNOMED","430396006","Chronic systolic dysfunction of left ventricle (disorder)","1","20210127"),
 ("HF","SNOMED","446221000","Heart failure with normal ejection fraction (disorder)","1","20210127"),
 ("HF","SNOMED","698592004","Asymptomatic left ventricular systolic dysfunction (disorder)","1","20210127"),
 ("HF","SNOMED","703272007","Heart failure with reduced ejection fraction (disorder)","1","20210127"),
 ("HF","SNOMED","717491000000102","Excepted from heart failure quality indicators - informed dissent (finding)","1","20210127"),
 ("HF","SNOMED","760361000000100","Fast track heart failure referral for transthoracic two dimensional echocardiogram","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Angina & Unstable angina

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_angina as
 select *
 from values
 
 ("angina","ICD10","I20","Angina","",""),
 ("angina","SNOMED","10971000087107","Myocardial ischemia during surgery (disorder)","1","20210127"),
 ("angina","SNOMED","15960061000119102","Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)","1","20210127"),
 ("angina","SNOMED","15960581000119102","Angina co-occurrent and due to arteriosclerosis of autologous vein coronary artery bypass graft (disorder)","1","20210127"),
 ("angina","SNOMED","15960661000119107","Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)","1","20210127"),
 ("angina","SNOMED","19057007","Status anginosus (disorder)","1","20210127"),
 ("angina","SNOMED","194828000","Angina (disorder)","1","20210127"),
 ("angina","SNOMED","21470009","Syncope anginosa (disorder)","1","20210127"),
 ("angina","SNOMED","233821000","New onset angina (disorder)","1","20210127"),
 ("angina","SNOMED","25106000","Impending infarction (disorder)","1","20210127"),
 ("angina","SNOMED","314116003","Post infarct angina (disorder)","1","20210127"),
 ("angina","SNOMED","371806006","Progressive angina (disorder)","1","20210127"),
 ("angina","SNOMED","371809004","Recurrent angina status post coronary stent placement (disorder)","1","20210127"),
 ("angina","SNOMED","371812001","Recurrent angina status post directional coronary atherectomy (disorder)","1","20210127"),
 ("angina","SNOMED","41334000","Angina, class II (disorder)","1","20210127"),
 ("angina","SNOMED","413439005","Acute ischemic heart disease (disorder)","1","20210127"),
 ("angina","SNOMED","413444003","Acute myocardial ischemia (disorder)","1","20210127"),
 ("angina","SNOMED","413838009","Chronic ischemic heart disease (disorder)","1","20210127"),
 ("angina","SNOMED","429559004","Typical angina (disorder)","1","20210127"),
 ("angina","SNOMED","4557003","Preinfarction syndrome (disorder)","1","20210127"),
 ("angina","SNOMED","59021001","Angina decubitus (disorder)","1","20210127"),
 ("angina","SNOMED","61490001","Angina, class I (disorder)","1","20210127"),
 ("angina","SNOMED","712866001","Resting ischemia co-occurrent and due to ischemic heart disease (disorder)","1","20210127"),
 ("angina","SNOMED","791000119109","Angina associated with type II diabetes mellitus (disorder)","1","20210127"),
 ("angina","SNOMED","85284003","Angina, class III (disorder)","1","20210127"),
 ("angina","SNOMED","89323001","Angina, class IV (disorder)","1","20210127"),
 ("angina","SNOMED","15960141000119102","Angina co-occurrent and due to coronary arteriosclerosis (disorder)","1","20210127"),
 ("angina","SNOMED","15960381000119109","Angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)","1","20210127"),
 ("angina","SNOMED","194823009","Acute coronary insufficiency (disorder)","1","20210127"),
 ("angina","SNOMED","225566008","Ischemic chest pain (finding)","1","20210127"),
 ("angina","SNOMED","233819005","Stable angina (disorder)","1","20210127"),
 ("angina","SNOMED","233823002","Silent myocardial ischemia (disorder)","1","20210127"),
 ("angina","SNOMED","300995000","Exercise-induced angina (disorder)","1","20210127"),
 ("angina","SNOMED","315025001","Refractory angina (disorder)","1","20210127"),
 ("angina","SNOMED","35928006","Nocturnal angina (disorder)","1","20210127"),
 ("angina","SNOMED","371807002","Atypical angina (disorder)","1","20210127"),
 ("angina","SNOMED","371808007","Recurrent angina status post percutaneous transluminal coronary angioplasty (disorder)","1","20210127"),
 ("angina","SNOMED","371810009","Recurrent angina status post coronary artery bypass graft (disorder)","1","20210127"),
 ("angina","SNOMED","371811008","Recurrent angina status post rotational atherectomy (disorder)","1","20210127"),
 ("angina","SNOMED","394659003","Acute coronary syndrome (disorder)","1","20210127"),
 ("angina","SNOMED","413844008","Chronic myocardial ischemia (disorder)","1","20210127"),
 ("angina","SNOMED","414545008","Ischemic heart disease (disorder)","1","20210127"),
 ("angina","SNOMED","414795007","Myocardial ischemia (disorder)","1","20210127"),
 ("angina","SNOMED","46109009","Subendocardial ischemia (disorder)","1","20210127"),
 ("angina","SNOMED","697976003","Microvascular ischemia of myocardium (disorder)","1","20210127"),
 ("angina","SNOMED","703214003","Silent coronary vasospastic disease (disorder)","1","20210127"),
 ("angina","SNOMED","713405002","Subacute ischemic heart disease (disorder)","1","20210127"),
 ("angina","SNOMED","87343002","Prinzmetal angina (disorder)","1","20210127"),
 ("unstable_angina","ICD10","I20.1","Unstable angina","",""),
 ("unstable_angina","SNOMED","15960061000119102","Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)","1","20210127"),
 ("unstable_angina","SNOMED","15960661000119107","Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)","1","20210127"),
 ("unstable_angina","SNOMED","19057007","Status anginosus (disorder)","1","20210127"),
 ("unstable_angina","SNOMED","25106000","Impending infarction (disorder)","1","20210127"),
 ("unstable_angina","SNOMED","4557003","Preinfarction syndrome (disorder)","1","20210127"),
 ("unstable_angina","SNOMED","712866001","Resting ischemia co-occurrent and due to ischemic heart disease (disorder)","1","20210127"),
 ("unstable_angina","SNOMED","315025001","Refractory angina (disorder)","1","20210127"),
 ("unstable_angina","SNOMED","35928006","Nocturnal angina (disorder)","1","20210127"),
 ("unstable_angina","SNOMED","371807002","Atypical angina (disorder)","1","20210127"),
 ("unstable_angina","SNOMED","394659003","Acute coronary syndrome (disorder)","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Pericarditis & Myocarditis

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_pericarditis_myocarditis as
 select *
 from values
 
 ("pericarditis","ICD10","I30","Acute pericarditis","",""),
 ("myocarditis","ICD10","I51.4","Myocarditis, unspecified","",""),
 ("myocarditis","ICD10","I40","Acute myocarditis","",""),
 ("myocarditis","ICD10","I41","Myocarditis in diseases classified elsewhere","","")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### Stroke_TIA

# COMMAND ----------

 %sql
 create or replace global temp view ccu002_01_stroke_TIA as
 select *
 from values
 
 ("stroke_TIA","ICD10","G45","Transient cerebral ischaemic attacks and related syndromes","",""),
 ("stroke_TIA","SNOMED","195206000","Intermittent cerebral ischaemia","1","20210127"),
 ("stroke_TIA","SNOMED","230716006","Anterior circulation transient ischaemic attack","1","20210127"),
 ("stroke_TIA","SNOMED","266257000","TIA","1","20210127"),
 ("stroke_TIA","SNOMED","230717002","Vertebrobasilar territory transient ischemic attack (disorder)","1","20210127"),
 ("stroke_TIA","SNOMED","230716006","Anterior circulation transient ischaemic attack","1","20210127"),
 ("stroke_TIA","SNOMED","230717002","Vertebrobasilar territory transient ischaemic attack","1","20210127")
 
 as tab(name, terminology, code, term, code_type, RecordDate)

# COMMAND ----------

 #MAGIC %md
 ### COVID codes

# COMMAND ----------

 %sql
 -- SNOMED COVID-19 codes
 -- Create Temporary View with of COVID-19 codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
 -- Covid-1 Status groups:
 -- - Lab confirmed incidence
 -- - Lab confirmed historic
 -- - Clinically confirmed
 CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_01_snomed_codes_covid19 AS
 SELECT *
 FROM VALUES
 ("1008541000000105","Coronavirus ribonucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
 ("1029481000000103","Coronavirus nucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
 ("120814005","Coronavirus antibody (substance)","0","1","Lab confirmed historic"),
 ("121973000","Measurement of coronavirus antibody (procedure)","0","1","Lab confirmed historic"),
 ("1240381000000105","Severe acute respiratory syndrome coronavirus 2 (organism)","0","1","Clinically confirmed"),
 ("1240391000000107","Antigen of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
 ("1240401000000105","Antibody to severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed historic"),
 ("1240411000000107","Ribonucleic acid of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
 ("1240421000000101","Serotype severe acute respiratory syndrome coronavirus 2 (qualifier value)","0","1","Lab confirmed historic"),
 ("1240511000000106","Detection of severe acute respiratory syndrome coronavirus 2 using polymerase chain reaction technique (procedure)","0","1","Lab confirmed incidence"),
 ("1240521000000100","Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
 ("1240531000000103","Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
 ("1240541000000107","Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
 ("1240551000000105","Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
 ("1240561000000108","Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
 ("1240571000000101","Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
 ("1240581000000104","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)","0","1","Lab confirmed incidence"),
 ("1240741000000103","Severe acute respiratory syndrome coronavirus 2 serology (observable entity)","0","1","Lab confirmed historic"),
 ("1240751000000100","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
 ("1300631000000101","Coronavirus disease 19 severity score (observable entity)","0","1","Clinically confirmed"),
 ("1300671000000104","Coronavirus disease 19 severity scale (assessment scale)","0","1","Clinically confirmed"),
 ("1300681000000102","Assessment using coronavirus disease 19 severity scale (procedure)","0","1","Clinically confirmed"),
 ("1300721000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed by laboratory test (situation)","0","1","Lab confirmed historic"),
 ("1300731000000106","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed using clinical diagnostic criteria (situation)","0","1","Clinically confirmed"),
 ("1321181000000108","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 record extraction simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
 ("1321191000000105","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 procedures simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
 ("1321201000000107","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 health issues simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
 ("1321211000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 presenting complaints simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
 ("1321241000000105","Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
 ("1321301000000101","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid qualitative existence in specimen (observable entity)","0","1","Lab confirmed incidence"),
 ("1321311000000104","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
 ("1321321000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
 ("1321331000000107","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 total immunoglobulin in serum (observable entity)","0","1","Lab confirmed historic"),
 ("1321341000000103","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin G in serum (observable entity)","0","1","Lab confirmed historic"),
 ("1321351000000100","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin M in serum (observable entity)","0","1","Lab confirmed historic"),
 ("1321541000000108","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G detected (finding)","0","1","Lab confirmed historic"),
 ("1321551000000106","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M detected (finding)","0","1","Lab confirmed historic"),
 ("1321761000000103","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A detected (finding)","0","1","Lab confirmed historic"),
 ("1321801000000108","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin A in serum (observable entity)","0","1","Lab confirmed historic"),
 ("1321811000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
 ("1322781000000102","Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)","0","1","Lab confirmed incidence"),
 ("1322871000000109","Severe acute respiratory syndrome coronavirus 2 antibody detection result positive (finding)","0","1","Lab confirmed historic"),
 ("186747009","Coronavirus infection (disorder)","0","1","Clinically confirmed")
 
 AS tab(clinical_code, description, sensitive_status, include_binary, covid_status);

# COMMAND ----------

 %sql 
 CREATE OR REPLACE GLOBAL TEMP VIEW inf_codelist AS
 SELECT
   codelist AS name,
   system AS terminology,
   code,
   term,
   "" AS code_type,
   "" AS RecordDate
 FROM
   global_temp.ccu002_01_drug_codelists
 UNION ALL
 SELECT
   smoking_status AS name,
   'SNOMED' AS terminology,
   conceptID AS code,
   description AS term,
   "" AS code_type,
   "" AS RecordDate
 FROM
   global_temp.ccu002_01_smokingstatus_SNOMED
 UNION ALL
 SELECT
 'pregnancy_and_birth' AS name,
 'SNOMED' AS terminology,
 code,
 term,
 "" AS code_type,
  "" AS RecordDate
 FROM
 global_temp.ccu002_01_pregnancy_birth_sno
 UNION ALL
 SELECT
 'prostate_cancer' AS name,
 'SNOMED' AS terminology,
 code,
 term,
 "" AS code_type,
  "" AS RecordDate
 FROM
 global_temp.ccu002_01_prostate_cancer_sno
 UNION ALL
 SELECT
 'depression' AS name,
 terminology,
 code,
 term,
 code_type,
 RecordDate
 FROM
 global_temp.ccu002_01_depression
 union all
 select *
 from global_temp.ccu002_01_BMI_obesity
 union all
 select *
 from global_temp.ccu002_01_COPD
 union all
 select *
 from global_temp.ccu002_01_hypertension
 union all
 select *
 from global_temp.ccu002_01_diabetes
 union all 
 select *
 from global_temp.ccu002_01_cancer
 union all 
 select *
 from global_temp.ccu002_01_liver
 union all 
 select *
 from global_temp.ccu002_01_dementia
 union all 
 select *
 from global_temp.ccu002_01_ckd
 union all
 select 
 new_name as name,
 terminology,
 code,
 term,
 code_type,
 RecordDate
 from global_temp.ccu002_01_ami
 union all
 select *
 from global_temp.ccu002_01_vt
 union all
 select * 
 from global_temp.ccu002_01_DVT_ICVT
 union all
 select * 
 from global_temp.ccu002_01_PE
 union all
 select * 
 from global_temp.ccu002_01_stroke_IS
 union all
 select 
 new_name as name,
 terminology,
 code,
 term,
 code_type,
 RecordDate
 from global_temp.ccu002_01_stroke_NOS
 union all
 select 
 new_name as name,
 terminology,
 code,
 term,
 code_type,
 RecordDate
 from global_temp.ccu002_01_stroke_SAH
 union all
 select 
 new_name as name,
 terminology,
 code,
 term,
 code_type,
 RecordDate
 from global_temp.ccu002_01_stroke_HS
 union all
 select * 
 from global_temp.ccu002_01_thrombophilia
 union all
 select * 
 from global_temp.ccu002_01_TCP
 union all
 select * 
 from global_temp.ccu002_01_retinal_infarction
 union all
 select * 
 from global_temp.ccu002_01_arterial_embolism
 union all
 select * 
 from global_temp.ccu002_01_DIC
 union all
 select * 
 from global_temp.ccu002_01_mesenteric_thrombus
 union all
 select * 
 from global_temp.ccu002_01_spinal_stroke
 union all
 select * 
 from global_temp.ccu002_01_fracture
 union all
 select * 
 from global_temp.ccu002_01_artery_dissect
 union all
 select * 
 from global_temp.ccu002_01_life_arrhythmias
 union all
 select * 
 from global_temp.ccu002_01_cardiomyopathy
 union all
 select * 
 from global_temp.ccu002_01_HF
 union all
 select * 
 from global_temp.ccu002_01_angina
 union all
 select * 
 from global_temp.ccu002_01_pericarditis_myocarditis
 union all
 select * 
 from global_temp.ccu002_01_stroke_TIA
 union all
 SELECT
   'GDPPR_confirmed_COVID' AS name,
   'SNOMED' AS terminology,
   clinical_code AS code,
   description AS term,
   "" AS code_type,
   "" AS RecordDate
 from global_temp.ccu002_01_snomed_codes_covid19

