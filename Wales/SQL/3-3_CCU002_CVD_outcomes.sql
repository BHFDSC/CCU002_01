--************************************************************************************************
-- Script:       3-3_CCU002_CVD_outcomes.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
-- About:        Create tables for CCU002-01 project which contain all CVD outcomes (primary care, hospital and death data)

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.CCU002_START_DATE DATE DEFAULT '2020-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.CCU002_END_DATE DATE DEFAULT '2020-12-07';

-- ***********************************************************************************************
-- Create a table containing all CVD outcomes for CCU002
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914 (
    alf_e  		           bigint,
    wob                    date,
    dod                    date,
    gndr_cd                char(1),
    record_date            date,
    code                   char(5),
    name                   char(50),    -- CVD category
    description            char(255),
    terminology            char(9),     -- ICD10, Read code
    arterial_event         smallint,
    venous_event           smallint,
    haematological_event   smallint,
    source                 char(5),     -- WLGP, Death, PEDW, OPDW
    category               char(40)
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914;
TRUNCATE TABLE SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914
    SELECT alf_e,
           wob,
           dod,
           gndr_cd,
           dod AS record_date,
           outcome_icd10 AS code,
           --outcome_name AS name,
           CASE WHEN outcome_name = 'AMI_CCU002' AND outcome_category = 'AMI' THEN 'AMI'
                WHEN outcome_name = 'PE_CCU002' THEN 'PE'
                WHEN outcome_name = 'STROKE_TIA_CCU002' THEN 'STROKE_TIA'
                WHEN outcome_name = 'ARTERY_DISSECT_CCU002' THEN 'ARTERY_DISSECT'
                WHEN outcome_name = 'CARDIOMYOPATHY_CCU002' THEN 'CARDIOMYOPATHY'
                WHEN outcome_name = 'STROKE_ISCH_CCU002' THEN 'STROKE_ISCH'
                WHEN outcome_name = 'LIFE_ARRHYTHM_CCU002' THEN 'LIFE_ARRHYTHM'
                WHEN outcome_name = 'STROKE_SAH_HS_CCU002' THEN 'STROKE_SAH_HS'
                WHEN outcome_name = 'ANGINA' AND outcome_category = 'Angina' THEN 'ANGINA'
                WHEN outcome_name = 'ANGINA' AND outcome_category = 'Unstable angina' THEN 'UNSTABLE_ANGINA'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'DVT_DVT' THEN 'DVT_DVT'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'other_DVT' THEN 'OTHER_DVT'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'DVT_pregnancy' THEN 'DVT_PREGNANCY'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'ICVT_pregnancy' THEN 'ICVT_PREGNANCY'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'portal_vein_thrombosis' THEN 'PORTAL_VEIN_THROMBOSIS'
                WHEN outcome_name = 'TCP' AND outcome_category = 'TTP' THEN 'TTP'
                WHEN outcome_name = 'TCP' AND outcome_category = 'thrombocytopenia' THEN 'THROMBOCYTOPENIA'
                ELSE outcome_name  -- 'FRACTURE', 'PERICARDITIS','MYOCARDITIS','MESENTERIC_THROMBUS','DVT_ICVT','HF',
                                   -- 'DIC','ARTERIAL_EMBOLISM_OTHR','THROMBOPHILIA',
           END AS name,
           outcome_term AS description,
           'ICD10' AS terminology,
           CASE WHEN outcome_name IN ('AMI_CCU002', 'STROKE_ISCH_CCU002', 'ARTERIAL_EMBOLISM_OTHR') THEN 1 ELSE 0 END AS arterial_event,
           CASE WHEN outcome_name IN ('VT_CCU002', 'DVT_ICVT', 'PE_CCU002') THEN 1 ELSE 0 END AS venous_event,
           CASE WHEN outcome_name IN ('DIC', 'TCP') THEN 1 ELSE 0 END AS haematological_event,
           'Death' AS source,
           outcome_category AS category
    FROM SAILWWMCCV.PHEN_DEATH_CVD
    WHERE dod >= SAILWWMCCV.CCU002_START_DATE
    AND dod <= SAILWWMCCV.CCU002_END_DATE
    AND alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914)
    AND outcome_icd10 NOT IN ('O082', 'I252', 'I241') -- covariate_only
    AND outcome_name IN ('AMI_CCU002', 'STROKE_ISCH_CCU002', 'ARTERIAL_EMBOLISM_OTHR','ANGINA','THROMBOPHILIA',
                         'VT_CCU002', 'DVT_ICVT', 'PE_CCU002', 'DIC', 'TCP', 'HF', 'STROKE_TIA_CCU002',
                         'ARTERY_DISSECT_CCU002', 'CARDIOMYOPATHY_CCU002', 'FRACTURE', 'LIFE_ARRHYTHM_CCU002',
                         'MYOCARDITIS', 'PERICARDITIS', 'STROKE_SAH_HS_CCU002', 'MESENTERIC_THROMBUS')
    UNION ALL
    SELECT alf_e,
           NULL AS wob,
           NULL AS dod,
           gndr_cd,
           admis_dt AS record_date,
           outcome_icd10 AS code,
           --outcome_name AS name,
           CASE WHEN outcome_name = 'AMI_CCU002' AND outcome_category = 'AMI' THEN 'AMI'
                WHEN outcome_name = 'PE_CCU002' THEN 'PE'
                WHEN outcome_name = 'STROKE_TIA_CCU002' THEN 'STROKE_TIA'
                WHEN outcome_name = 'ARTERY_DISSECT_CCU002' THEN 'ARTERY_DISSECT'
                WHEN outcome_name = 'CARDIOMYOPATHY_CCU002' THEN 'CARDIOMYOPATHY'
                WHEN outcome_name = 'STROKE_ISCH_CCU002' THEN 'STROKE_ISCH'
                WHEN outcome_name = 'LIFE_ARRHYTHM_CCU002' THEN 'LIFE_ARRHYTHM'
                WHEN outcome_name = 'STROKE_SAH_HS_CCU002' THEN 'STROKE_SAH_HS'
                WHEN outcome_name = 'ANGINA' AND outcome_category = 'Angina' THEN 'ANGINA'
                WHEN outcome_name = 'ANGINA' AND outcome_category = 'Unstable angina' THEN 'UNSTABLE_ANGINA'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'DVT_DVT' THEN 'DVT_DVT'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'other_DVT' THEN 'OTHER_DVT'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'DVT_pregnancy' THEN 'DVT_PREGNANCY'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'ICVT_pregnancy' THEN 'ICVT_PREGNANCY'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'portal_vein_thrombosis' THEN 'PORTAL_VEIN_THROMBOSIS'
                WHEN outcome_name = 'TCP' AND outcome_category = 'TTP' THEN 'TTP'
                WHEN outcome_name = 'TCP' AND outcome_category = 'thrombocytopenia' THEN 'THROMBOCYTOPENIA'
                ELSE outcome_name  -- 'FRACTURE', 'PERICARDITIS','MYOCARDITIS','MESENTERIC_THROMBUS','DVT_ICVT','HF',
                                   -- 'DIC','ARTERIAL_EMBOLISM_OTHR','THROMBOPHILIA',
           END AS name,
           outcome_term AS description,
           'ICD10' AS terminology,
           CASE WHEN outcome_name IN ('AMI_CCU002', 'STROKE_ISCH_CCU002', 'ARTERIAL_EMBOLISM_OTHR') THEN 1 ELSE 0 END AS arterial_event,
           CASE WHEN outcome_name IN ('VT_CCU002', 'DVT_ICVT', 'PE_CCU002') THEN 1 ELSE 0 END AS venous_event,
           CASE WHEN outcome_name IN ('DIC', 'TCP') THEN 1 ELSE 0 END AS haematological_event,
           'PEDW' AS SOURCE,
           outcome_category AS category
    FROM SAILWWMCCV.PHEN_PEDW_CVD
    WHERE admis_dt >= SAILWWMCCV.CCU002_START_DATE -- '2020-01-01'
    AND admis_dt <= SAILWWMCCV.CCU002_END_DATE     --'2020-12-07'
    AND alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914)
    AND outcome_icd10 NOT IN ('O082', 'I252', 'I241') -- covariate_only
    AND outcome_name IN ('AMI_CCU002', 'STROKE_ISCH_CCU002', 'ARTERIAL_EMBOLISM_OTHR','ANGINA','THROMBOPHILIA',
                         'VT_CCU002', 'DVT_ICVT', 'PE_CCU002', 'DIC', 'TCP', 'HF', 'STROKE_TIA_CCU002',
                         'ARTERY_DISSECT_CCU002', 'CARDIOMYOPATHY_CCU002', 'FRACTURE', 'LIFE_ARRHYTHM_CCU002',
                         'MYOCARDITIS', 'PERICARDITIS', 'STROKE_SAH_HS_CCU002', 'MESENTERIC_THROMBUS')
    UNION ALL
    SELECT alf_e,
           wob,
           NULL AS dod,
           gndr_cd,
           event_dt AS record_date,
           outcome_readcode AS code,
           --outcome_name AS name,
           CASE WHEN outcome_name = 'STROKE_ISCH_CCU002' THEN 'STROKE_ISCH'
                WHEN outcome_name = 'STROKE_SAH_HS_CCU002' THEN 'STROKE_SAH_HS'
                WHEN outcome_name = 'ANGINA' AND outcome_category = 'Stable angina' THEN 'ANGINA'
                WHEN outcome_name ='ANGINA' AND outcome_category = 'Unstable Angina' THEN 'UNSTABLE_ANGINA'
                ELSE outcome_name -- 'AMI', 'CARDIOMYOPATHY', 'PE', 'HF', 'DVT_ICVT', 'THROMBOPHILIA', 'STROKE_TIA'
           END AS name,
           outcome_term AS description,
           'Read code' AS terminology,
           CASE WHEN outcome_name IN ('AMI', 'STROKE_ISCH_CCU002') THEN 1 ELSE 0 END AS arterial_event,
           CASE WHEN outcome_name IN ('DVT_ICVT', 'PE') THEN 1 ELSE 0 END AS venous_event,
           CASE WHEN outcome_name IN ('TCP') THEN 1 ELSE 0 END AS haematological_event, -- codes only for covariates
           'WLGP' AS SOURCE,
           outcome_category AS category
    FROM SAILWWMCCV.PHEN_WLGP_CVD
    WHERE event_dt >= SAILWWMCCV.CCU002_START_DATE
    AND event_dt <= SAILWWMCCV.CCU002_END_DATE
    AND alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914)
    AND outcome_name IN ('AMI', 'STROKE_ISCH_CCU002', 'HF', 'ANGINA', 'STROKE_TIA');
   -- AND outcome_name IN ('AMI', 'STROKE_ISCH_CCU002', 'DVT_ICVT', 'PE', 'HF', 'ANGINA', 'STROKE_TIA', 
   --                      'STROKE_SAH_HS_CCU002', 'THROMBOPHILIA', 'CARDIOMYOPATHY');

SELECT * FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914
ORDER BY name;
-- ***********************************************************************************************
-- Create a table containing first event per CVD category
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914 (
    alf_e  		      bigint,
    wob               date,
    dod               date,
    gndr_cd           char(1),
    record_date       date,
    code              char(5),
    name              char(50),
    description       char(255),
    terminology       char(9),     -- ICD10, Read code
    source            char(5),     -- WLGP, Death, PEDW, OPDW
    seq               smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914;
TRUNCATE TABLE SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914
    SELECT * FROM (SELECT alf_e,
                          wob,
                          dod,
                          gndr_cd,
                          record_date,
                          code,
                          name,
                          description,
                          terminology,
                          SOURCE,
                          ROW_NUMBER() OVER(PARTITION BY alf_e, name ORDER BY record_date) AS seq
                   FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914
                   )
    WHERE seq = 1
    ORDER BY alf_e;

UPDATE SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914 tgt
set tgt.wob = src.wob,
    tgt.dod = src.dod
FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914 src
WHERE tgt.alf_e = src.alf_e;

-- ***********************************************************************************************
-- Create a table containing first arterial/event details
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_CVD_OUTCOMES_SUMMARY_FIRST_20210914 (
    alf_e  		             bigint,
    arterial_date            date,
    arterial_event           smallint,
    arterial_source          char(5),
    venous_date              date,
    venous_event             smallint,
    venous_source            char(5),
    haematological_date      date,
    haematological_event     smallint,
    haematological_source    char(5)
    )
DISTRIBUTE BY HASH(alf_e);
--DROP TABLE SAILWWMCCV.CCU002_CVD_OUTCOMES_SUMMARY_FIRST_20210914;
TRUNCATE TABLE SAILWWMCCV.CCU002_CVD_OUTCOMES_SUMMARY_FIRST_20210914 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_CVD_OUTCOMES_SUMMARY_FIRST_20210914
SELECT alf_e,
       arterial_date,
       arterial_event,
       arterial_source,
       venous_date,
       venous_event,
       venous_source,
       haematological_date,
       haematological_event,
       haematological_source
FROM (
     SELECT combined_alf,
           id2,
           CASE WHEN combined_alf IS NOT NULL THEN combined_alf
                ELSE id2
                END AS alf_e,
           arterial_date,
           arterial_event,
           arterial_source,
           venous_date,
           venous_event,
           venous_source,
           haematological_date,
           haematological_event,
           haematological_source
    FROM (SELECT alf_e,
                 id,
                 CASE WHEN alf_e IS NOT NULL THEN alf_e
                      ELSE id
                 END AS combined_alf,
                 arterial_date,
                 arterial_event,
                 arterial_source,
                 venous_date,
                 venous_event,
                 venous_source
          FROM (SELECT *
                FROM (SELECT alf_e,
                             record_date AS arterial_date,
                             arterial_event,
                             source AS arterial_source,
                             ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY record_date) AS seq
                      FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914
                      WHERE arterial_event = 1
                      )
               WHERE seq =1
               ORDER BY alf_e,seq
               ) t1
           FULL OUTER JOIN (SELECT *
                            FROM (SELECT alf_e AS id,
                                         record_date AS venous_date,
                                         venous_event,
                                         source AS venous_source,
                                         ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY record_date) AS seq
                                  FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914
                                  WHERE venous_event = 1
                                  )
                            WHERE seq =1
                            ) t2
           ON t1.alf_e = t2.id
          ) t_c
    FULL OUTER JOIN (SELECT *
                     FROM (SELECT alf_e AS id2,
                                  record_date AS haematological_date,
                                  haematological_event,
                                  source AS haematological_source,
                                  ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY record_date) AS seq
                           FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914
                           WHERE haematological_event = 1)
                     WHERE seq =1
                    ) t3
    ON t_c.alf_e =t3.id2
);

SELECT * FROM SAILWWMCCV.CCU002_CVD_OUTCOMES_SUMMARY_FIRST_20210914;
