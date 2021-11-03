--************************************************************************************************
-- Script:       2-1_PHEN_COVID19.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- About:        Create COVID-19 related PHEN tables

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Create a table containing all COVID19 test results for C20 e-cohort
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19 (
    alf_e                      bigint,
    wob                        date,
    gndr_cd                    char(1),
    dod                        date,
    first_positive_test_dt     date,
    test_order                 int,
    spcm_collected_dt          date,
    spcm_received_dt           date,
    spcm_cd                    char(5),
    spcm_name                  char(60),
    result_cd                  char(10),
    result_name                char(30),
    covid19testresult          char(11),
    ctu_pers_type              char(100),
    ctu_pers_typeconsolidated  char(5),
    pat_type_cd                char(5),
    pat_type_desc              char(30)
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19;
TRUNCATE TABLE SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19 IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19 (alf_e, wob, gndr_cd, dod, test_order,
            spcm_collected_dt, spcm_received_dt, spcm_cd, spcm_name, result_cd, result_name,
            covid19testresult, ctu_pers_type, ctu_pers_typeconsolidated, pat_type_cd, pat_type_desc)
    (SELECT t.alf_e, c.wob, c.gndr_cd, c.dod,
            ROW_NUMBER() OVER(PARTITION BY t.alf_e ORDER BY spcm_collected_dt) AS test_order,
            date(t.spcm_collected_dt), date(spcm_received_dt), t.spcm_cd, t.spcm_name, t.result_cd, t.resultname,
            t.covid19testresult, t.ctu_pers_type, t.ctu_pers_typeconsolidated, t.pat_type_cd, t.pat_type_desc
     FROM SAILWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_TESTRESULTS t
     JOIN SAILWMCCV.C19_COHORT20 c
     ON t.alf_e = c.alf_e
     WHERE testsetname = 'COVID19'
     AND date(spcm_collected_dt) >= '2020-01-01'
     AND year(spcm_collected_dt) <= 2021
     ORDER BY t.alf_e, t.spcm_collected_dt);

------------------------------------------------------------------------------------------------
-- Update first_positive_test_dt
UPDATE SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19 tgt
SET tgt.first_positive_test_dt = src.first_positive_test_dt
FROM (SELECT alf_e, min(spcm_collected_dt) AS first_positive_test_dt
      FROM SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19
      WHERE covid19testresult = 'Positive'
      GROUP BY alf_e) src
WHERE tgt.alf_e = src.alf_e;

SELECT count(*), count(DISTINCT alf_e) FROM SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19 WHERE covid19testresult = 'Positive';

SELECT count(DISTINCT alf_e) FROM SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19 WHERE first_positive_test_dt IS NOT NULL;
-- ***********************************************************************************************
-- Create a table containing all COVID19 related records in hospital data
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_PEDW_COVID19 (
    alf_e                      bigint,
    prov_unit_cd               char(3),
    spell_num_e                bigint,
    epi_num                    char(3),
    wob                        date,
    gndr_cd                    char(1),
    dod                        date,
    record_order               int,
    admis_dt                   date,
    admis_mthd_cd              char(2),
    admis_source_cd            char(2),
    admis_spec_cd              char(3),
    disch_dt                   date,
    disch_mthd_cd              char(2),
    disch_spec_cd              char(3),
    epi_str_dt                 date,
    epi_end_dt                 date,
    epi_diag_cd                char(20),
    res_ward_cd                char(2),
    diag_cd_123                char(5),
    diag_cd                    char(20),
    diag_cd_categry            char(20)
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_PEDW_COVID19;
TRUNCATE TABLE SAILWWMCCV.PHEN_PEDW_COVID19 IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_PEDW_COVID19 (alf_e,prov_unit_cd,spell_num_e,epi_num,record_order,admis_dt,
                                          admis_mthd_cd,admis_source_cd,admis_spec_cd,disch_dt,disch_mthd_cd,
                                          disch_spec_cd,epi_str_dt,epi_end_dt,epi_diag_cd,res_ward_cd,
                                          diag_cd_123,diag_cd, diag_cd_categry)
    SELECT s.alf_e,
           s.prov_unit_cd,
           s.spell_num_e,
           e.epi_num,
           ROW_NUMBER() OVER(PARTITION BY s.alf_e ORDER BY s.admis_dt, e.epi_str_dt) AS record_order,
           s.admis_dt,
           s.admis_mthd_cd,
           s.admis_source_cd,
           s.admis_spec_cd,
           s.disch_dt,
           s.disch_mthd_cd,
           s.disch_spec_cd,
           e.epi_str_dt,
           e.epi_end_dt,
           e.diag_cd_1234,
           s.res_ward_cd,
           d.diag_cd_123,
           d.diag_cd,
           c.category
    FROM SAILWWMCCV.WMCC_PEDW_SPELL s
    LEFT JOIN SAILWWMCCV.WMCC_PEDW_EPISODE e
    ON s.prov_unit_cd = e.prov_unit_cd
    AND s.spell_num_e = e.spell_num_e
    LEFT JOIN SAILWWMCCV.WMCC_PEDW_DIAG d
    ON e.prov_unit_cd = d.prov_unit_cd
    AND e.spell_num_e = d.spell_num_e
    AND e.epi_num = d.epi_num
    JOIN SAILWWMCCV.PHEN_ICD10_COVID19 c
    ON d.diag_cd_1234 = c.code OR e.diag_cd_1234 = c.code
    WHERE s.admis_yr >= 2020
    AND s.admis_yr <= 2021
    AND c.is_latest = 1
    ORDER BY s.alf_e, s.admis_dt, e.epi_str_dt;

-- Update wob, gndr_cd, dod using C20 e-cohort
UPDATE SAILWWMCCV.PHEN_PEDW_COVID19 tgt
SET tgt.wob = src.wob,
    tgt.gndr_cd = src.gndr_cd,
    tgt.dod = src.dod
FROM SAILWMCCV.C19_COHORT20 src
WHERE tgt.alf_e = src.alf_e;

-- ***********************************************************************************************
-- Create a table containing all COVID19 related records in GP data
-- ***********************************************************************************************
REATE TABLE SAILWWMCCV.PHEN_WLGP_COVID19 (
    alf_e                      bigint,
    wob                        date,
    gndr_cd                    char(1),
    dod                        date,
    record_order               integer,
    event_dt                   date,
    event_cd                   char(40),
    event_cd_description       char(255),
    event_cd_category          char(35),
    event_val                  decimal(31,8),
    prac_cd_e                  integer
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_WLGP_COVID19;
TRUNCATE TABLE SAILWWMCCV.PHEN_WLGP_COVID19 IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_WLGP_COVID19 (alf_e, wob, gndr_cd, record_order, event_dt, event_cd,
                                             event_cd_description, event_cd_category, event_val, prac_cd_e)
    SELECT alf_e,
           wob,
           gndr_cd,
           ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt) AS record_order,
           event_dt,
           event_cd,
           r.desc AS event_cd_description,
           r.category AS event_cd_category,
           event_val,
           prac_cd_e
    FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED g
    JOIN SAILWWMCCV.PHEN_READ_COVID19 r
    ON g.event_cd = r.code
    WHERE event_dt >= '2020-01-01'
    AND YEAR(event_dt) <= 2021
    AND r.is_latest = 1;
