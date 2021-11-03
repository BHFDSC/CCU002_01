--************************************************************************************************
-- Script:       1-1_WMCC_data.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- About:        Create all WMCC level data

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Define coverage
-- ***********************************************************************************************
CREATE OR REPLACE VARIABLE SAILWWMCCV.COHORT_START_DATE DATE DEFAULT '2020-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.COHORT_END_DATE DATE DEFAULT ((SELECT max(COHORT_END_DATE) FROM SAILWMCCV.C19_COHORT20));

SELECT max(dod) FROM SAILWMCCV.C19_COHORT20;
--DROP VARIABLE SAILWWMCCV.COHORT_START_DATE
--DROP VARIABLE SAILWWMCCV.COHORT_END_DATE

-- ***********************************************************************************************
-- Create WMCC level data
-- ***********************************************************************************************
-- PEDW
CREATE TABLE SAILWWMCCV.WMCC_PEDW_SPELL LIKE SAILWMCCV.C19_COHORT_PEDW_SPELL;
INSERT INTO SAILWWMCCV.WMCC_PEDW_SPELL
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_PEDW_SPELL
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_SPELL IMMEDIATE;

CREATE TABLE SAILWWMCCV.WMCC_PEDW_EPISODE LIKE SAILWMCCV.C19_COHORT_PEDW_EPISODE;
INSERT INTO SAILWWMCCV.WMCC_PEDW_EPISODE
    (SELECT e.*
     FROM SAILWWMCCV.WMCC_PEDW_SPELL s
     LEFT JOIN SAILWMCCV.C19_COHORT_PEDW_EPISODE e
     ON s.prov_unit_cd = e.prov_unit_cd
     AND s.spell_num_e = e.spell_num_e
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_EPISODE IMMEDIATE;

CREATE TABLE SAILWWMCCV.WMCC_PEDW_DIAG LIKE SAILWMCCV.C19_COHORT_PEDW_DIAG;
INSERT INTO SAILWWMCCV.WMCC_PEDW_DIAG
    (SELECT * FROM (SELECT d.*
                    FROM SAILWWMCCV.WMCC_PEDW_EPISODE e
                    LEFT JOIN SAILWMCCV.C19_COHORT_PEDW_DIAG d
                    ON e.prov_unit_cd = d.prov_unit_cd
                    AND e.spell_num_e = d.spell_num_e
                    AND e.epi_num = d.epi_num)
     WHERE diag_cd IS NOT null
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_DIAG IMMEDIATE;

CREATE TABLE SAILWWMCCV.WMCC_PEDW_OPER LIKE SAILWMCCV.C19_COHORT_PEDW_OPER;
INSERT INTO SAILWWMCCV.WMCC_PEDW_OPER
    (SELECT o.*
     FROM SAILWWMCCV.WMCC_PEDW_EPISODE e
     LEFT JOIN SAILWMCCV.C19_COHORT_PEDW_OPER o
     ON e.prov_unit_cd = o.prov_unit_cd
     AND e.spell_num_e = o.spell_num_e
     AND e.spell_num_e = o.spell_num_e
     WHERE o.oper_cd IS NOT null
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_OPER IMMEDIATE;

CREATE TABLE SAILWWMCCV.WMCC_PEDW_SUPERSPELL LIKE SAILWMCCV.C19_COHORT_PEDW_SUPERSPELL;
INSERT INTO SAILWWMCCV.WMCC_PEDW_SUPERSPELL
    (SELECT s.*
     FROM SAILWMCCV.C19_COHORT_PEDW_SUPERSPELL s
     JOIN SAILWWMCCV.WMCC_PEDW_SPELL p
     ON s.prov_unit_cd = p.prov_unit_cd
     AND s.spell_num_e = p.spell_num_e
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_SUPERSPELL IMMEDIATE;
----------------------------------------------------------------------------------------------------
-- Out patient data
CREATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS LIKE SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS;
INSERT INTO SAILWWMCCV.WMCC_OPDW_OUTPATIENTS
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS IMMEDIATE;

CREATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG LIKE SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS_DIAG;
INSERT INTO SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG
    (SELECT d.*
     FROM SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS_DIAG d
     JOIN SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS o
     ON d.prov_unit_cd = o.prov_unit_cd
     AND d.case_rec_num_e = o.case_rec_num_e
     AND d.att_id_e = o.att_id_e
     AND d.attend_dt = o.attend_dt
     WHERE o.alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG IMMEDIATE;

CREATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_OPER LIKE SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS_OPER;
INSERT INTO SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_OPER
    (SELECT p.*
     FROM SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS_OPER p
     JOIN SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS o
     ON p.prov_unit_cd = o.prov_unit_cd
     AND p.case_rec_num_e = o.case_rec_num_e
     AND p.att_id_e = o.att_id_e
     AND p.attend_dt = o.attend_dt
     WHERE o.alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_OPER IMMEDIATE;
----------------------------------------------------------------------------------------------------
-- GP data
CREATE TABLE SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED LIKE SAILWMCCV.C19_COHORT_WLGP_PATIENT_ALF_CLEANSED;
INSERT INTO SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_WLGP_PATIENT_ALF_CLEANSED
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED IMMEDIATE;

CREATE TABLE SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED (
    alf_e  		      bigint,
    alf_sts_cd        char(2),
    alf_mtch_pct      decimal(7,6),
    gndr_cd           char(1),
    wob               date,
    lsoa_cd           char(10),
    prac_cd_e         bigint,
    event_cd          char(40),
    event_val         decimal(31,8),
    event_dt          date,
    episode           char(1),
    sequence          int
    )
DISTRIBUTE BY HASH(alf_e);

INSERT INTO SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
    (SELECT alf_e, alf_sts_cd, alf_mtch_pct, gndr_cd, wob, lsoa_cd,
            prac_cd_e, event_cd, event_val, event_dt, episode, sequence
     FROM SAILWMCCV.C19_COHORT_WLGP_GP_EVENT_CLEANSED
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED)
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED IMMEDIATE;
----------------------------------------------------------------------------------------------------
-- COVID19 test data
CREATE TABLE SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_ANTIBODYRESULTS LIKE SAILWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_ANTIBODYRESULTS;
INSERT INTO SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_ANTIBODYRESULTS
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_ANTIBODYRESULTS
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_ANTIBODYRESULTS IMMEDIATE;

CREATE TABLE SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_TESTRESULTS LIKE SAILWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_TESTRESULTS;
INSERT INTO SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_TESTRESULTS
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_TESTRESULTS
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_TESTRESULTS IMMEDIATE;
----------------------------------------------------------------------------------------------------
-- WDSD ADD
CREATE TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD (
    alf_e  		        bigint,
    gndr_cd             char(1),
    wob                 date,
    dod                 date,
    pers_id_e           bigint,
    ralf_e              bigint,
    from_dt             date,
    to_dt               date,
    lsoa2001_cd         char(10),
    lsoa2011_cd         char(10),
    uprn_qas_match_cd   char(50),
    row_sts             char(1),
    ralf_sts_cd         char(2),
    avail_from_dt       date
    )
DISTRIBUTE BY HASH(alf_e);

--TRUNCATE TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD
    (SELECT a.alf_e, a.gndr_cd, a.wob, a.dod, d.*
     FROM SAILWMCCV.C19_COHORT_WDSD_AR_PERS a
     JOIN SAILWMCCV.C19_COHORT_WDSD_AR_PERS_ADD d
     ON a.pers_id_e = d.pers_id_e
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );
-----------------------------------------------------------------------------------------------------
-- WDSD GP
CREATE TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_GP (
    alf_e  		        bigint,
    gndr_cd             char(1),
    wob                 date,
    dod                 date,
    pers_id_e           bigint,
    from_dt             date,
    to_dt               date,
    prac_cd_e           bigint,
    row_sts             char(1),
    avail_from_dt       date
    )
DISTRIBUTE BY HASH(alf_e);

INSERT INTO SAILWWMCCV.WMCC_WDSD_AR_PERS_GP
    (SELECT a.alf_e, a.gndr_cd, a.wob, a.dod, g.*
     FROM SAILWMCCV.C19_COHORT_WDSD_AR_PERS a
     JOIN SAILWMCCV.C19_COHORT_WDSD_AR_PERS_GP g
     ON a.pers_id_e = g.pers_id_e
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );
--TRUNCATE TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_GP IMMEDIATE;
-- ***********************************************************************************************
-- Create a table containing all COVID19 related deaths
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.WMCC_DEATH_COVID19 LIKE SAILWMCCV.C19_COHORT20_MORTALITY;

INSERT INTO SAILWWMCCV.WMCC_DEATH_COVID19
    SELECT *
    FROM SAILWMCCV.C19_COHORT20_MORTALITY
    WHERE covid_yn_underlying = 'y'
    OR covid_yn_underlying_qcovid = 'y'
    OR covid_yn_underlying_or_secondary = 'y'
    OR covid_yn_underlying_or_secondary_qcovid = 'y'
    OR covid_yn_secondary = 'y';

--TRUNCATE TABLE SAILWWMCCV.WMCC_DEATH_COVID19 IMMEDIATE;