--************************************************************************************************
-- Script:      3-4_CCU002_covariates.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
-- About:        Determine covariates for CCU002-01 cohort

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.CCU002_START_DATE DATE DEFAULT '2020-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.CCU002_END_DATE DATE DEFAULT '2020-12-07';

-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_COVARIATES_20210914 (
    alf_e                                      bigint,
    gndr_cd                                    char(1),
    wob                                        date,
    dod                                        date,
    gp_coverage_end_date                       date,
    consultation_rate                          int,
    unique_medications                         int,
    unique_bnf_chapters                        int,
    antiplatelet                               smallint,
    bp_lowering                                smallint,
    lipid_lowering                             smallint,
    anticoagulant                              smallint,
    cocp                                       smallint,
    hrt                                        smallint, 
    diabetes_medication                        smallint,
    diabetes_diag                              smallint,
    hypertension_medication                    smallint,
    hypertension_diag                          smallint,
    pregnancy                                  smallint,
    depression                                 smallint,
    cancer                                     smallint,
    copd                                       smallint,
    ckd                                        smallint,
    liver_disease                              smallint,
    dementia                                   smallint,
    smoking                                    smallint,
    smoking_status                             char(20),
    surgery                                    smallint,
    bmi_obesity                                smallint,
    bmi                                        decimal(31,8),
    obese_bmi                                  smallint,
    obesity                                    smallint, -- bmi_obesity + obese_bmi
    ami                                        smallint,
    vt                                         smallint,
    dvt_icvt                                   smallint,
    pe                                         smallint,
    stroke_isch                                smallint,
    stroke_sah_hs                              smallint,
    thrombophilia                              smallint,
    tcp                                        smallint,
    angina                                     smallint,
    other_arterial_embolism                    smallint,
    dic                                        smallint,
    mesenteric_thrombus                        smallint,
    artery_dissect                             smallint,
    life_arrhythmia                            smallint,
    cardiomyopathy                             smallint,
    hf                                         smallint,
    pericarditis                               smallint,
    myocarditis                                smallint,
    stroke_tia                                 smallint,
    all_stroke                                 smallint,
    pe_vt                                      smallint,
    n_disorder                                 int
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.CCU002_COVARIATES_20210914;
TRUNCATE TABLE SAILWWMCCV.CCU002_COVARIATES_20210914 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_COVARIATES_20210914 (alf_e, gndr_cd, wob, dod, gp_coverage_end_date)
    SELECT DISTINCT alf_e,
    gndr_cd,
    wob,
    dod,
    gp_coverage_end_date
    FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914
    ORDER BY alf_e;
-------------------------------------------------------------------------------------------------
-- Number of unique drugs prescribed over previous 3 months (not used in the new version)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.unique_medications = src.unique_medications
FROM (SELECT alf_e,
             count(medication) AS unique_medications
      FROM (SELECT alf_e,
                   bnf_combined AS medication,
                   count(bnf_combined)
            FROM (SELECT DISTINCT alf_e,
                         bnf_combined
                  FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
                  WHERE bnf_combined IS NOT NULL
                  AND dt_prescribed >= '2019-10-01'
                  AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
                  ORDER BY alf_e, bnf_combined
                  )
            GROUP BY alf_e, bnf_combined
            ORDER BY alf_e
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914
SET unique_medications = 0
WHERE unique_medications IS NULL
AND alf_e IN (SELECT DISTINCT alf_e
              FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
              WHERE event_dt >= '2019-01-01'
              AND event_dt < SAILWWMCCV.CCU002_START_DATE);

SELECT count(alf_e) FROM SAILWWMCCV.CCU002_COVARIATES_20210914
WHERE unique_medications IS NULL;
-------------------------------------------------------------------------------------------------
-- Number of unique BNF chapters over past 3 months
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.unique_bnf_chapters = src.unique_bnf_chapters
FROM (SELECT alf_e,
             count(bnf_chapter) AS unique_bnf_chapters
      FROM (SELECT alf_e,
                   bnf_chapter,
                   count(bnf_chapter)
            FROM (SELECT DISTINCT alf_e,
                         LEFT(bnf_combined,2) AS bnf_chapter
                  FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
                  WHERE bnf_combined IS NOT NULL
                  AND dt_prescribed >= '2019-10-01'
                  AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
                  ORDER BY alf_e
                  )
            GROUP BY alf_e, bnf_chapter
            ORDER BY alf_e
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914
SET unique_bnf_chapters = 0
WHERE unique_bnf_chapters IS NULL
AND alf_e IN (SELECT DISTINCT alf_e
              FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
              WHERE event_dt >= '2019-01-01'
              AND event_dt < SAILWWMCCV.CCU002_START_DATE);
-------------------------------------------------------------------------------------------------
-- Antiplatelet drugs
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.antiplatelet = 1
FROM (SELECT DISTINCT alf_e
      FROM (SELECT DISTINCT alf_e,
                   bnf_combined,
                   dt_prescribed
            FROM SAILWWMCCV.PHEN_WDDS_COVARIATES
            WHERE antiplatelet_agents = 1
            AND dt_prescribed >= '2019-10-01'
            AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
            )
     ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
-- BP lowering
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.bp_lowering = 1
FROM (SELECT DISTINCT alf_e
      FROM (SELECT DISTINCT alf_e,
                   bnf_combined,
                   dt_prescribed
            FROM SAILWWMCCV.PHEN_WDDS_COVARIATES
            WHERE bp_lowering = 1
            AND dt_prescribed >= '2019-10-01'
            AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
            )
     ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
-- Lipid lowering
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.lipid_lowering = 1
FROM (SELECT DISTINCT alf_e
      FROM (SELECT DISTINCT alf_e,
                   bnf_combined,
                   dt_prescribed
            FROM SAILWWMCCV.PHEN_WDDS_COVARIATES
            WHERE lipid_regulating_drugs = 1
            AND dt_prescribed >= '2019-10-01'
            AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
            )
     ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
-- Anticoagulant
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.anticoagulant = 1
FROM (SELECT DISTINCT alf_e
      FROM (SELECT DISTINCT alf_e,
                   bnf_combined,
                   dt_prescribed
            FROM SAILWWMCCV.PHEN_WDDS_COVARIATES
            WHERE anticoagulant = 1
            AND dt_prescribed >= '2019-10-01'
            AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
            )
     ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
-- COCP
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.cocp = 1
FROM (SELECT DISTINCT alf_e
      FROM (SELECT DISTINCT alf_e,
                   bnf_combined,
                   dt_prescribed
            FROM SAILWWMCCV.PHEN_WDDS_COVARIATES
            WHERE cocp = 1
            AND dt_prescribed >= '2019-10-01'
            AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
            )
     ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
-- HRT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.hrt = 1
FROM (SELECT DISTINCT alf_e
      FROM (SELECT DISTINCT alf_e,
                   bnf_combined,
                   dt_prescribed
            FROM SAILWWMCCV.PHEN_WDDS_COVARIATES
            WHERE hrt = 1
            AND dt_prescribed >= '2019-10-01'
            AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
            )
     ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
-- Diabetes medication (DMD code)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.diabetes_medication = 1
FROM (SELECT DISTINCT alf_e
      FROM (SELECT DISTINCT alf_e,
                   bnf_combined,
                   dmdcode_prescribed,
                   dt_prescribed
            FROM SAILWWMCCV.PHEN_WDDS_COVARIATES
            WHERE diabetes_medication = 1
            AND dt_prescribed > '2019-09-30'
            AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
            )
     ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
-- Hypertension medication (DMD code)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.hypertension_medication = 1
FROM (SELECT DISTINCT alf_e
      FROM (SELECT DISTINCT alf_e,
                   bnf_combined,
                   dmdcode_prescribed,
                   dt_prescribed
            FROM SAILWWMCCV.PHEN_WDDS_COVARIATES
            WHERE hypertension_medication = 1
            AND dt_prescribed > '2019-09-30'
            AND dt_prescribed < SAILWWMCCV.CCU002_START_DATE
            )
     ) src
WHERE tgt.alf_e = src.alf_e;

--***********************************************************************************************
--------------------------------------------------------------------------------------------------
-- Consultation rate
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.consultation_rate = src.consultation_rate
FROM (SELECT alf_e,
             count(DISTINCT event_dt) AS consultation_rate
      FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
      WHERE event_dt >= '2019-01-01'
      AND event_dt < SAILWWMCCV.CCU002_START_DATE
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.consultation_rate = 0
WHERE consultation_rate IS NULL AND gp_coverage_end_date IS NOT NULL;
-------------------------------------------------------------------------------------------------
-- Pregnancy (not used anymore)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.pregnancy = 1
WHERE tgt.alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE event_dt  < SAILWWMCCV.CCU002_START_DATE
                    AND event_dt >= '2019-01-01'
                    AND pregnancy = 1);
-------------------------------------------------------------------------------------------------
-- Hypertension
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.hypertension_diag = 1
WHERE tgt.alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE hypertension = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.hypertension_diag = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE hypertension = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.hypertension_diag IS NULL;
--------------------------------------------------------------------------------------------------
-- diabetes_diag
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.diabetes_diag = 1
WHERE tgt.alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE diabetes_diag = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.diabetes_diag = 1
WHERE tgt.alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE diabetes = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.diabetes_diag IS NULL;
--------------------------------------------------------------------------------------------------
-- cancer
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.cancer = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE cancer = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.cancer = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE cancer = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.cancer IS NULL;
--------------------------------------------------------------------------------------------------
-- liver_disease
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.liver_disease = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE liver_disease = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.liver_disease = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE liver_disease = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.liver_disease IS NULL;
--------------------------------------------------------------------------------------------------
-- dementia
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.dementia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE dementia = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.dementia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE dementia = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.dementia IS NULL;
--------------------------------------------------------------------------------------------------
-- ckd
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.ckd = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE ckd = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.ckd = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE ckd = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.ckd IS NULL;
--------------------------------------------------------------------------------------------------
-- COPD
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.copd = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE copd = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.copd = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE copd = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.copd IS NULL;
--------------------------------------------------------------------------------------------------
-- Depression
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.depression = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE depression = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.depression = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE depression = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.depression IS NULL;
-------------------------------------------------------------------------------------------------
-- Smoking status
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.smoking = 1,
    tgt.smoking_status = CASE WHEN src.smoking_category = 'S' THEN 'Current-smoker'
                              WHEN src.smoking_category = 'E' THEN 'Ex-smoker'
                              WHEN src.smoking_category = 'N' THEN 'Never-smoker'
                         END
FROM (SELECT alf_e,
             event_dt,
             smoking_category,
             ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt DESC) AS row_num
      FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
      WHERE smoking = 1
      AND event_dt < SAILWWMCCV.CCU002_START_DATE) src
WHERE tgt.alf_e = src.alf_e
AND row_num = 1;
-------------------------------------------------------------------------------------------------
-- bmi_obesity
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.bmi_obesity = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE obesity = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE
                    AND event_cd IN (SELECT code
                                     FROM SAILWWMCCV.PHEN_READ_OBESITY
                                     WHERE category = 'Diagnosis of Obesity'
                                     )
                   );

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.bmi_obesity = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE obesity = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.bmi_obesity IS NULL;

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914
SET bmi_obesity = 0
WHERE bmi_obesity IS NULL;
--------------------------------------------------------------------------------------------------
-- BMI & OBESE_BMI
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.bmi = src.event_val,
    tgt.obese_bmi = CASE WHEN src.event_val >= 30 THEN 1
                         ELSE 0 END
FROM (SELECT alf_e,
             event_dt,
             event_cd,
             event_val,
             ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt DESC) AS row_num
      FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
      WHERE bmi = 1
      AND event_dt < SAILWWMCCV.CCU002_START_DATE
      AND YEAR(event_dt) - YEAR(wob) >= 18
      AND event_val IS NOT NULL
      AND event_val >= 12
      AND event_val <= 100
      ) src
WHERE tgt.alf_e = src.alf_e
AND row_num = 1;

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914
SET obese_bmi = 0
WHERE obese_bmi IS NULL;
-------------------------------------------------------------------------------------------------
-- Obesity
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.obesity = CASE WHEN obese_bmi = 1 OR bmi_obesity = 1 THEN 1
                       WHEN obese_bmi = 0 AND bmi_obesity = 0 THEN 0
                       ELSE 0
                  END
WHERE alf_e IS NOT NULL;
-------------------------------------------------------------------------------------------------
-- AMI
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.ami = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE ami = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.ami = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE ami = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.ami IS NULL;
-------------------------------------------------------------------------------------------------
-- VT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.vt = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE vt = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914
SET vt = 0
WHERE vt IS NULL;
-------------------------------------------------------------------------------------------------
-- DVT_ICVT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.dvt_icvt = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE dvt_icvt = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.dvt_icvt = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE dvt_icvt = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.dvt_icvt IS NULL;
-------------------------------------------------------------------------------------------------
-- PE
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.pe = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE pe = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.pe = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE pe = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.pe IS NULL;

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914
SET pe = 0
WHERE pe IS NULL;
-------------------------------------------------------------------------------------------------
-- Stroke ISCH
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.stroke_isch = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE stroke_isch = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.stroke_isch = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_isch = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.stroke_isch IS NULL;
-------------------------------------------------------------------------------------------------
-- Stroke SAH_HS
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.stroke_sah_hs = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE stroke_sah_hs = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.stroke_sah_hs = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_sah_hs = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.stroke_sah_hs IS NULL;
-------------------------------------------------------------------------------------------------
-- Thrombophilia
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.thrombophilia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE thrombophilia = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.thrombophilia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE thrombophilia = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.thrombophilia IS NULL;
-------------------------------------------------------------------------------------------------
-- TCP
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.tcp = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE tcp = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.tcp = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE tcp = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.tcp IS NULL;
-------------------------------------------------------------------------------------------------
-- Angina
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.angina = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE angina = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.angina = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE angina = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.angina IS NULL;
-------------------------------------------------------------------------------------------------
-- other_arterial_embolism
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.other_arterial_embolism = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE other_arterial_embolism = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE);
-------------------------------------------------------------------------------------------------
-- DIC
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.dic = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE dic = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE);
-------------------------------------------------------------------------------------------------
-- mesenteric_thrombus
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.mesenteric_thrombus = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE mesenteric_thrombus = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE);
-------------------------------------------------------------------------------------------------
-- Artery dissect
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.artery_dissect = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE artery_dissect = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.artery_dissect = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE artery_dissect = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.artery_dissect IS NULL;
-------------------------------------------------------------------------------------------------
-- Life arrhythmia
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.life_arrhythmia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE life_arrhythmia = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE);
-------------------------------------------------------------------------------------------------
-- Cardiomyopathy
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.cardiomyopathy = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE cardiomyopathy = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.cardiomyopathy = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE cardiomyopathy = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.cardiomyopathy IS NULL;
-------------------------------------------------------------------------------------------------
-- HF
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.hf = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE heart_failure = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.hf = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE hf = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.hf IS NULL;
-------------------------------------------------------------------------------------------------
-- Pericarditis
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.pericarditis = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE pericarditis = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE);
-------------------------------------------------------------------------------------------------
-- Myocarditis
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.myocarditis = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE myocarditis = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE);
-------------------------------------------------------------------------------------------------
-- Stroke TIA
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.stroke_tia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE stroke_tia = 1
                    AND event_dt < SAILWWMCCV.CCU002_START_DATE);

UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.stroke_tia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_tia = 1
                    AND admis_dt < SAILWWMCCV.CCU002_START_DATE)
AND tgt.stroke_tia IS NULL;
-------------------------------------------------------------------------------------------------
-- All stroke
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914
SET all_stroke = CASE WHEN stroke_tia = 1 OR stroke_sah_hs = 1 OR stroke_isch = 1 THEN 1
                      WHEN stroke_tia = 0 AND stroke_sah_hs = 0 AND stroke_isch = 0 THEN 0
                      ELSE 0
                 END
WHERE alf_e IS NOT NULL;
-------------------------------------------------------------------------------------------------
-- PE + VT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914
SET pe_vt = CASE WHEN pe =1 OR vt =1 THEN 1
                 WHEN pe = 0 AND vt = 0 THEN 0
                 ELSE 0
            END
WHERE alf_e IS NOT NULL;
--***********************************************************************************************
--------------------------------------------------------------------------------------------------
-- surgery
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.surgery = 1
WHERE tgt.alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.WMCC_PEDW_SPELL s
                    LEFT JOIN SAILWWMCCV.WMCC_PEDW_EPISODE e
                    ON s.prov_unit_cd = e.prov_unit_cd
                    AND s.spell_num_e = e.spell_num_e
                    LEFT JOIN SAILWWMCCV.WMCC_PEDW_OPER d
                    ON e.prov_unit_cd = d.prov_unit_cd
                    AND e.spell_num_e = d.spell_num_e
                    AND e.epi_num = d.epi_num 
                    WHERE d.oper_cd IS NOT NULL
                    AND oper_dt < SAILWWMCCV.CCU002_START_DATE
                    AND oper_dt>= '2019-01-01');
--***********************************************************************************************
-------------------------------------------------------------------------------------------------
-- Disorders (using Charlson codelist)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.n_disorder = src.n_disorder
FROM (SELECT alf_e,
             count(DISTINCT event_cd) AS n_disorder
      FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
      WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CHARLSON)
      AND event_dt >= '2019-01-01'
      AND event_dt < SAILWWMCCV.CCU002_START_DATE
      GROUP BY alf_e) src
WHERE tgt.alf_e = src.alf_e


UPDATE SAILWWMCCV.CCU002_COVARIATES_20210914 tgt
SET tgt.n_disorder = 0
WHERE n_disorder IS NULL AND gp_coverage_end_date IS NOT NULL;

SELECT max(n_disorder) FROM SAILWWMCCV.CCU002_COVARIATES_20210914;
