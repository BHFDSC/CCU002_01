--************************************************************************************************
-- Script:       3-1_CCU002_included_patients.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-01: SARS-CoV-2 infection and risk of venous thromboembolism and arterial thrombotic events
-- About:        Derive list of eligible individuals for CCU002-01 project

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Quality assurance
-- ***********************************************************************************************
SELECT count(DISTINCT alf_e) FROM SAILWMCCV.C19_COHORT20;
--------------------------------------------------------------------------------------------------
-- Rule 1: check if year(dod) < year(wob), also in Wales, dod < wob
--------------------------------------------------------------------------------------------------
SELECT count(DISTINCT alf_e) FROM SAILWMCCV.C19_COHORT20 WHERE year(dod) < year(wob);
SELECT count(DISTINCT alf_e) FROM SAILWMCCV.C19_COHORT20 WHERE dod < wob;
--------------------------------------------------------------------------------------------------
-- Rule 2: all ALFs, gndr_cd and wob should be known
--------------------------------------------------------------------------------------------------
SELECT count(*) FROM SAILWMCCV.C19_COHORT20 WHERE gndr_cd IN NULL;
SELECT count(*) FROM SAILWMCCV.C19_COHORT20 WHERE wob IN NULL;
--------------------------------------------------------------------------------------------------
-- Rule 3: check if wob is over the current date
--------------------------------------------------------------------------------------------------
SELECT min(wob), max(wob) FROM SAILWMCCV.C19_COHORT20;
SELECT count(*) FROM SAILWMCCV.C19_COHORT20 WHERE current_date - wob < 0;
--------------------------------------------------------------------------------------------------
-- Rule 4: remove those with invalid dod
--------------------------------------------------------------------------------------------------
SELECT count(*) FROM SAILWMCCV.C19_COHORT20 WHERE year(dod) < 2020;
--------------------------------------------------------------------------------------------------
-- Rule 5: remove those where registered dod < actual dod
--------------------------------------------------------------------------------------------------
SELECT count(DISTINCT alf_e) FROM SAILWMCCV.C19_COHORT20
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT_ADDE_DEATHS
                WHERE death_dt > death_reg_dt);
--------------------------------------------------------------------------------------------------
-- Rule 6: check if men have pregnancy related codes
--------------------------------------------------------------------------------------------------
-- Done
--------------------------------------------------------------------------------------------------
-- Rule 7: check if women have a code related to prostate cancer
-------------------------------------------------------------------------------------------------
-- Done
--------------------------------------------------------------------------------------------------
-- Rule 8: check if patients have missing event dates in GP data
--------------------------------------------------------------------------------------------------
SELECT count(*) FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
WHERE event_dt IS NULL; 
-- ***********************************************************************************************
-- Inclusion/exclusion
-- ***********************************************************************************************
-- Create CCU002_INCLUDED_PATIENTS table
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914 LIKE SAILWMCCV.C19_COHORT20;

--DROP TABLE SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914;
TRUNCATE TABLE SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914
    (SELECT * FROM SAILWMCCV.C19_COHORT20
     WHERE der_age_ >= 18    -- Exclude individuals whose age is under 18 at cohort start date
     AND gndr_cd IS NOT NULL -- Exclude individuals whose gndr_cd is null
     AND migration = 0       -- Exclude those who moved into Wales or born after 2020-01-01
     AND (dod >= '2020-01-01' OR dod IS NULL)
    );

--------------------------------------------------------------------------------------------------
-- Excclude people with a posistive test before '2020-01-01'
--------------------------------------------------------------------------------------------------
DELETE FROM SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914
WHERE alf_e IN (SELECT DISTINCT alf_e
                FROM  SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_TESTRESULTS
                WHERE date(spcm_collected_dt) < '2020-01-01'
                AND covid19testresult = 'Positive');
