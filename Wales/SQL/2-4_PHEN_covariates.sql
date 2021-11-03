--************************************************************************************************
-- Script:       2-4_PHEN_covariates.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- About:        Create PHEN level covariates tables

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Create a table for PEDW covariates
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_PEDW_COVARIATES (
    alf_e                        bigint,
    gndr_cd                      char(1),
    admis_dt                     date,
    e_diag_cd_1234               char(4),
    d_diag_cd_123                char(3),
    d_diag_cd_1234               char(4),
    diabetes                     smallint,
    depression                   smallint,
    obesity                      smallint,
    cancer                       smallint,
    copd                         smallint,
    ckd                          smallint,
    liver_disease                smallint,
    hypertension                 smallint,
    dementia                     smallint,
    ami                          smallint,
    vt                           smallint,
    dvt_icvt                     smallint,
    pe                           smallint,
    stroke_isch                  smallint,
    stroke_sah_hs                smallint,
    thrombophilia                smallint,
    tcp                          smallint,
    angina                       smallint,
    other_arterial_embolism      smallint,
    dic                          smallint,
    mesenteric_thrombus          smallint,
    artery_dissect               smallint,
    life_arrhythmia              smallint,
    cardiomyopathy               smallint,
    hf                           smallint,
    pericarditis                 smallint,
    myocarditis                  smallint,
    stroke_tia                   smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_PEDW_COVARIATES;
TRUNCATE TABLE SAILWWMCCV.PHEN_PEDW_COVARIATES IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_PEDW_COVARIATES (alf_e, gndr_cd, admis_dt, e_diag_cd_1234,
                                             d_diag_cd_123, d_diag_cd_1234)
    SELECT DISTINCT s.alf_e,
    s.gndr_cd,
    s.admis_dt,
    LEFT(e.diag_cd_1234,4),
    d.diag_cd_123,
    LEFT(d.diag_cd,4)    
    FROM SAILWWMCCV.WMCC_PEDW_SPELL s
    LEFT JOIN SAILWWMCCV.WMCC_PEDW_EPISODE e
    ON s.prov_unit_cd = e.prov_unit_cd
    AND s.spell_num_e = e.spell_num_e
    LEFT JOIN SAILWWMCCV.WMCC_PEDW_DIAG d
    ON e.prov_unit_cd = d.prov_unit_cd
    AND e.spell_num_e = d.spell_num_e
    AND e.epi_num = d.epi_num
    JOIN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HYPERTENSION
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIABETES
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_CANCER
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIVER_DISEASE
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEMENTIA
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_CKD
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_OBESITY
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_COPD
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEPRESSION
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_TCP
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_ANGINA
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIC
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT code FROM SAILWWMCCV.PHEN_ICD10_HF
          WHERE is_latest = 1
          )
    ON (d.diag_cd_1234 = code OR d.diag_cd = code OR e.diag_cd_1234 = code OR e.diag_cd_123 = code)
    WHERE alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20)
    ORDER BY alf_e;

SELECT count(*) FROM SAILWWMCCV.PHEN_PEDW_COVARIATES;
-------------------------------------------------------------------------------------------------
-- Hypertension
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.hypertension = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HYPERTENSION)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HYPERTENSION)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HYPERTENSION)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HYPERTENSION);
--------------------------------------------------------------------------------------------------
-- diabete
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.diabetes = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIABETES)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIABETES)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIABETES);
--------------------------------------------------------------------------------------------------
-- cancer
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.cancer = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CANCER)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CANCER)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CANCER);
--------------------------------------------------------------------------------------------------
-- liver_disease
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.liver_disease = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIVER_DISEASE)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIVER_DISEASE)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIVER_DISEASE);
--------------------------------------------------------------------------------------------------
-- dementia
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.dementia = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEMENTIA)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEMENTIA)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEMENTIA);
--------------------------------------------------------------------------------------------------
-- ckd
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.ckd = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CKD)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CKD)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CKD);
--------------------------------------------------------------------------------------------------
-- obesity
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.obesity = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_OBESITY)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_OBESITY)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_OBESITY);
--------------------------------------------------------------------------------------------------
-- COPD
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.copd = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_COPD)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_COPD)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_COPD);
--------------------------------------------------------------------------------------------------
-- Depression
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.depression = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEPRESSION)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEPRESSION)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEPRESSION);
--------------------------------------------------------------------------------------------------
-- AMI
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.ami = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002);
--------------------------------------------------------------------------------------------------
-- VT
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.vt = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002);
--------------------------------------------------------------------------------------------------
-- DVT_ICVT
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.dvt_icvt = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT);
--------------------------------------------------------------------------------------------------
-- PE
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.pe = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002);
--------------------------------------------------------------------------------------------------
-- Stroke_ISCH
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_isch = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002);
--------------------------------------------------------------------------------------------------
-- Stroke_SAH_HS
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_sah_hs = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002);
--------------------------------------------------------------------------------------------------
-- thrombophilia
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.thrombophilia = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA);
--------------------------------------------------------------------------------------------------
-- TCP
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.tcp = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_TCP)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_TCP)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_TCP)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_TCP);
--------------------------------------------------------------------------------------------------
-- Angina
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.angina = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ANGINA)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ANGINA)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ANGINA)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ANGINA);
--------------------------------------------------------------------------------------------------
-- other_arterial_embolism
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.other_arterial_embolism = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR);
--------------------------------------------------------------------------------------------------
-- DIC
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.dic = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIC)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIC)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIC)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIC);
--------------------------------------------------------------------------------------------------
-- mesenteric_thrombus
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.mesenteric_thrombus= 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS);
--------------------------------------------------------------------------------------------------
-- artery_dissect
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.artery_dissect = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002);
--------------------------------------------------------------------------------------------------
-- life_arrhythmia
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.life_arrhythmia = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002);
--------------------------------------------------------------------------------------------------
-- cardiomyopathy
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.cardiomyopathy = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002);
--------------------------------------------------------------------------------------------------
-- HF
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.hf = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HF)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HF)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HF)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HF);
--------------------------------------------------------------------------------------------------
-- pericarditis
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.pericarditis = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS);
--------------------------------------------------------------------------------------------------
-- myocarditis
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.myocarditis = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS);
--------------------------------------------------------------------------------------------------
-- stroke_TIA
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_tia = 1
WHERE e_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002)
OR LEFT(e_diag_cd_1234,3) IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002)
OR d_diag_cd_123 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002)
OR d_diag_cd_1234 IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002);

-- ***********************************************************************************************
-- Create a table for WLGP covariates
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_WLGP_COVARIATES (
    alf_e                        bigint,
    gndr_cd                      char(1),
    wob                          date,
    event_cd                     char(5),
    event_val                    decimal(31,8),
    event_dt                     date,
    hypertension                 smallint,
    bmi                          smallint,
    pregnancy                    smallint,
    systolic_blood_pressure      smallint,
    heart_failure                smallint, 
    diabetes_diag                smallint,
    diabetes_type2               smallint,
    depression                   smallint,
    obesity                      smallint,
    cancer                       smallint,
    copd                         smallint,
    ckd                          smallint,
    ckd_stage                    smallint,
    egfr                         smallint,
    liver_disease                smallint,
    dementia                     smallint,
    alcohol                      smallint,
    alcohol_status               smallint,
    smoking                      smallint,
    smoking_category             char(2),
    ami                          smallint,
    dvt_icvt                     smallint,
    pe                           smallint,
    stroke_isch                  smallint,
    stroke_sah_hs                smallint,
    thrombophilia                smallint,
    tcp                          smallint,
    angina                       smallint,
    artery_dissect               smallint,
    cardiomyopathy               smallint,
    stroke_tia                   smallint
    )
DISTRIBUTE BY HASH(alf_e);


--DROP TABLE SAILWWMCCV.PHEN_WLGP_COVARIATES;
TRUNCATE TABLE SAILWWMCCV.PHEN_WLGP_COVARIATES IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_WLGP_COVARIATES (alf_e, gndr_cd, wob, event_cd, event_val, event_dt)
    SELECT DISTINCT alf_e,
    gndr_cd,
    wob,
    event_cd,
    event_val,
    event_dt
    FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
    WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_HYPERTENSION
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_BMI
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_PREGNANCY
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_BP
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_ALCOHOL
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_ALCOHOL_STATUS
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_SMOKING
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_DIABETES_TYPE2
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_DIABETES
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_DEPRESSION
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_OBESITY
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_CANCER
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_COPD
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_CKD
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_CKD_STAGE
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_EGFR
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_LIVER_DISEASE
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_DEMENTIA
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_HF
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_AMI
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_DVT_ICVT
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_PE
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_ISCH_CCU002
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_SAH_HS_CCU002
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_THROMBOPHILIA
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_TCP
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_ANGINA
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_ARTERY_DISSECT
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_CARDIOMYOPATHY
                       WHERE is_latest = 1
                       UNION ALL
                       SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_TIA
                       WHERE is_latest = 1
                       )
    AND alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20)
    AND YEAR(event_dt) =< 2021
    ORDER BY alf_e;

-------------------------------------------------------------------------------------------------
-- Hypertension
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.hypertension = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_HYPERTENSION);
-------------------------------------------------------------------------------------------------
-- BMI
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.bmi = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_BMI);
-------------------------------------------------------------------------------------------------
-- Pregnancy
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.pregnancy = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_PREGNANCY);
-------------------------------------------------------------------------------------------------
-- Systolic blood pressure (average of two most recent systolic blood pressure)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.systolic_blood_pressure = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_BP);
-------------------------------------------------------------------------------------------------
-- Heart failure
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.heart_failure = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_HF);
-------------------------------------------------------------------------------------------------
-- Alcohol consumption & alcohol category (most recent record)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.alcohol = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_ALCOHOL);
-------------------------------------------------------------------------------------------------
-- Alcohol status
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.alcohol_status = 1
WHERE event_cd IN (SELECT code
                   FROM SAILWWMCCV.PHEN_READ_ALCOHOL_STATUS
                   WHERE category NOT IN ('Non drinker', 'Drinker status not specified'));
-------------------------------------------------------------------------------------------------
-- Smoking status & smoking category
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.smoking = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_SMOKING);

UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.smoking_category = src.category
FROM SAILWWMCCV.PHEN_READ_SMOKING src
WHERE tgt.smoking = 1
AND tgt.event_cd = src.code;
-------------------------------------------------------------------------------------------------
-- Diabetes,type 2
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.diabetes_type2 = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DIABETES_TYPE2);
-------------------------------------------------------------------------------------------------
-- Diabetes diag
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.diabetes_diag = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DIABETES);
-------------------------------------------------------------------------------------------------
-- Cancer
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.cancer = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CANCER);
-------------------------------------------------------------------------------------------------
-- Liver disease
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.liver_disease = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_LIVER_DISEASE);
-------------------------------------------------------------------------------------------------
-- Dementia
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.dementia = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DEMENTIA);
-------------------------------------------------------------------------------------------------
-- CKD
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.ckd = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CKD);
-------------------------------------------------------------------------------------------------
-- CKD stage
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.ckd_stage = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CKD_STAGE);
-------------------------------------------------------------------------------------------------
-- EGFR
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.egfr = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_EGFR);
-------------------------------------------------------------------------------------------------
-- Obesity
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.obesity = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_OBESITY);
-------------------------------------------------------------------------------------------------
-- COPD
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.copd = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_COPD);
-------------------------------------------------------------------------------------------------
-- Depression
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.depression = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DEPRESSION);
-------------------------------------------------------------------------------------------------
-- AMI
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.ami = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_AMI);
-------------------------------------------------------------------------------------------------
-- DVT_ICVT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.dvt_icvt = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DVT_ICVT);
-------------------------------------------------------------------------------------------------
-- PE
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.pe = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_PE);
-------------------------------------------------------------------------------------------------
-- STROKE_ISCH
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.stroke_isch = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_ISCH_CCU002);
-------------------------------------------------------------------------------------------------
-- STROKE_SAH_HS
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.stroke_sah_hs = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_SAH_HS_CCU002);
-------------------------------------------------------------------------------------------------
-- Thrombophilia
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.thrombophilia = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_THROMBOPHILIA);
-------------------------------------------------------------------------------------------------
-- TCP
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.tcp = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_TCP);
-------------------------------------------------------------------------------------------------
-- Angina
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.angina = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_ANGINA);
-------------------------------------------------------------------------------------------------
-- artery_dissect
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.artery_dissect = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_ARTERY_DISSECT);
-------------------------------------------------------------------------------------------------
-- cardiomyopathy
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.cardiomyopathy = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CARDIOMYOPATHY);
-------------------------------------------------------------------------------------------------
-- stroke_tia
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.stroke_tia = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_TIA);
-------------------------------------------------------------------------------------------------