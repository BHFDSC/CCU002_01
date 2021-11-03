--************************************************************************************************
-- Script:       3-2_CCU002_COVID19_exposure.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
-- About:        Create COVID-19 related tables for CCU002-01 project

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.CCU002_START_DATE DATE DEFAULT '2020-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.CCU002_END_DATE DATE DEFAULT '2020-12-07';
-- ***********************************************************************************************
-- Create a table containing all COVID19 test results for CCU002 cohort
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914 (
    alf_e                bigint,
    wob                  date,
    dod                  date,
    gndr_cd              char(1),
    record_date          date,
    covid19_status       char(40),
    source               char(4),   -- PATD, PEDW, WLGP
    code                 char(9),   -- ICD10 or Read code
    clinical_code        char(5),
    description          char(60),
    pillar               int,
    status               char(20)    -- Confirmed or Suspected
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914;
TRUNCATE TABLE SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914 IMMEDIATE;
-------------------------------------------------------------------------------------------------
-- Add PCR test results
INSERT INTO SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914
    SELECT alf_e,
           wob,
           dod,
           gndr_cd,
           spcm_collected_dt AS record_date,
           'Positive PCR test' AS covid19_status,
           'PATD' AS source,
           NULL AS code,
           NULL AS clinical_code,
           NULL AS description,
           null AS pillar, -- pillar 1 & 2 in PATD
           'Confirmed COVID19' AS status
    FROM SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19
    WHERE covid19testresult = 'Positive'
    AND alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914)
    AND spcm_collected_dt >= SAILWWMCCV.CCU002_START_DATE
    AND spcm_collected_dt <= SAILWWMCCV.CCU002_END_DATE;

-------------------------------------------------------------------------------------------------
-- Add COVID19 related records from hospital data
INSERT INTO SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914
    SELECT alf_e,
           wob,
           dod,
           gndr_cd,
           admis_dt AS record_date,
           CASE WHEN diag_cd = 'U071' OR epi_diag_cd = 'U071' THEN 'Confirmed_COVID19'
                WHEN diag_cd = 'U072' OR epi_diag_cd = 'U072' THEN 'Suspected_COVID19'
           END AS covid19_status,
           'PEDW' AS source,
           'ICD10' AS code,
           CASE WHEN diag_cd IN ('U071', 'U072') THEN diag_cd
                WHEN epi_diag_cd IN ('U071', 'U072') THEN epi_diag_cd
                ELSE NULL
           END AS clinical_code,
           NULL AS description,
           NULL AS pillar,
           NULL AS status
    FROM SAILWWMCCV.PHEN_PEDW_COVID19
    WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914)
    AND admis_dt >= SAILWWMCCV.CCU002_START_DATE
    AND admis_dt <= SAILWWMCCV.CCU002_END_DATE;

UPDATE SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914
SET status =  CASE WHEN covid19_status = 'Confirmed_COVID19' THEN 'Confirmed COVID19'
                   WHEN covid19_status= 'Suspected_COVID19' THEN 'Suspected COVID19'
              END
WHERE source = 'PEDW';
-------------------------------------------------------------------------------------------------
-- Add COVID19 related records from Primary care data
INSERT INTO SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914
    SELECT alf_e,
           wob,
           dod,
           gndr_cd,
           event_dt AS record_date,
           event_cd_category AS covid19_status,
           'WLGP' AS source,
           'Read code' AS code,
           event_cd AS clinical_code,
           NULL AS description,
           NULL AS pillar,
           NULL AS status
    FROM SAILWWMCCV.PHEN_WLGP_COVID19
    WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914)
    AND event_dt >= SAILWWMCCV.CCU002_START_DATE
    AND event_dt <= SAILWWMCCV.CCU002_END_DATE;

UPDATE SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914
SET status =  CASE WHEN covid19_status IN ('Confirmed') THEN 'Confirmed COVID19'
                   WHEN covid19_status IN ('Suspected') THEN 'Suspected COVID19'
                   -- How about 'possible COVID19'?
              END
WHERE source = 'WLGP';

SELECT * FROM SAILWWMCCV.PHEN_READ_COVID19_20210914;

-- ***********************************************************************************************
-- Create CCU002 COVID cohort
-- ***********************************************************************************************
 CREATE TABLE SAILWWMCCV.CCU002_COVID19_COHORT_20210914 (
    alf_e                               bigint,
    covid19_confirmed_date              date,
    covid19_hospitalisation_phenotype   char(30)
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.CCU002_COVID19_COHORT_20210914;
TRUNCATE TABLE SAILWWMCCV.CCU002_COVID19_COHORT_20210914 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_COVID19_COHORT_20210914
    SELECT alf_e,
           min(record_date) AS covid19_confirmed_date,
           covid19_hospitalisation_phenotype
    FROM (SELECT a.alf_e,
                 a.wob,
                 a.dod,
                 a.record_date,
                 a.covid19_status,
                 b.covid19_hospitalisation AS covid19_hospitalisation_phenotype
          FROM SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914 a -- ccu002_covid19_confirmed
          LEFT JOIN (SELECT DISTINCT alf_e,
                            'hospitalised' AS covid19_hospitalisation
                     FROM (SELECT t1.alf_e,
                                  t1.first_admission_date,
                                  t2.first_covid_event,
                                  TIMESTAMPDIFF(16,TIMESTAMP(first_admission_date) - TIMESTAMP(first_covid_event)) AS days_between
                           FROM (SELECT alf_e,
                                         min(record_date) AS first_admission_date
                                 FROM (SELECT alf_e,
                                              record_date
                                       FROM SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914 a
                                       WHERE covid19_status IN  ('Confirmed_COVID19') -- only hospital admissions
                                      )
                                 GROUP BY alf_e
                                ) t1  -- min_date_admission
                           LEFT JOIN (SELECT alf_e,
                                             min(record_date) AS first_covid_event
                                      FROM (SELECT alf_e,
                                                   record_date
                                            FROM SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914 a
                                            WHERE covid19_status IN  ('Positive PCR test', 'Confirmed_COVID19', 'Confirmed')
                                           )
                                      GROUP BY alf_e
                                       ) t2  -- min_date_covid
                           ON t1.alf_e = t2.alf_e
                           ) -- ccu002_days_to_covid_admission
                     WHERE days_between <= 28
                    ) b  -- ccu002_admission_primary
          ON a.alf_e = b.alf_e
          WHERE a.covid19_status IN  ('Positive PCR test', 'Confirmed_COVID19', 'Confirmed')
          )
    GROUP BY alf_e, covid19_hospitalisation_phenotype; -- ccu002_covid19_hospitalised


UPDATE SAILWWMCCV.CCU002_COVID19_COHORT_20210914
SET covid19_hospitalisation_phenotype = 'non-hospitalised'
WHERE covid19_hospitalisation_phenotype IS NULL;

