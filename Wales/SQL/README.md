<!-------------------------------------------------------------------------------------------->
# SQL scripts

This directory contains the SQL files used in the Welsh SAIL Databank TRE for this project.

-   Contact Info: Hoda Abbasizanjani
    (<Hoda.Abbasizanjani@swansea.ac.uk>)

## Original data sources in SAIL

-   (PATD) Pathology data COVID-19 Daily
-   (PEDW) Patient Episode Database for Wales
-   (WDSD) Welsh Demographic Service Dataset
-   (WLGP) Wales Longitudinal General Practice
-   (OPDW) Outpatient Dataset for Wales
-   (EDDS) Emergency Department Data Set
-   (WDSD) Welsh Demographic Service Dataset
-   (WDDS) Welsh Dispensing Dataset

For information on the original SAIL data sources please see the HDR innovation gateway.

## WMCC layer tables

-   COVID-19 C19\_COHORT20 e-cohort:
    -   `SAILWMCCV.C19_COHORT20`
-   COVID-19 mortality e-cohort:
    -   `SAILWMCCV.C19_COHORT20_MORTALITY`
-   Drug Dispensing e-cohort:
    -   `SAILWMCCV.C19_COHORT20_RRDA_WDDS`
-   PATD:
    -   `SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_ANTIBODYRESULTS`
    -   `SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_TESTRESULTS`
-   PEDW:
    -   `SAILWWMCCV.WMCC_PEDW_EPISODE`
    -   `SAILWWMCCV.WMCC_PEDW_DIAG`
    -   `SAILWWMCCV.WMCC_PEDW_OPER`
    -   `SAILWWMCCV.WMCC_PEDW_SPELL`
    -   `SAILWWMCCV.WMCC_PEDW_SUPERSPELL`
-   WDSD:
    -   `SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD`
    -   `SAILWWMCCV.WMCC_WDSD_AR_PERS_GP`
-   WLGP:
    -   `SAILWWMCCV.WMCC_WLGP_GP_EVENT`
    -   `SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED`
-   OPDW:
    -   `SAILWWMCCV.WMCC_OPDW_OUTPATIENTS`
    -   `SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG`
    -   `SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_OPER`


## Phenotype layer tables

-   `SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19`
-   `SAILWWMCCV.PHEN_PEDW_COVID19`
-   `SAILWWMCCV.PHEN_WLGP_COVID19`
-   `SAILWWMCCV.PHEN_DEATH_CVD`
-   `SAILWWMCCV.PHEN_PEDW_CVD`
-   `SAILWWMCCV.PHEN_OPDW_CVD`
-   `SAILWWMCCV.PHEN_WLGP_CVD`
-   `SAILWWMCCV.PHEN_WDDS_COVARIATES`
-   `SAILWWMCCV.PHEN_PEDW_COVARIATES`
-   `SAILWWMCCV.PHEN_WLGP_COVARIATES`


## Project specific tables (CCU002-01)

-   CCU002-01 study population and their demographic details:
    -   `SAILWWMCCV.CCU002_INCLUDED_PATIENTS_20210914`
-   Outcome tables for the CCU002-01 cohort:
    -   `SAILWWMCCV.CCU002_CVD_OUTCOMES_20210914`
    -   `SAILWWMCCV.CCU002_CVD_OUTCOMES_FIRST_20210914`
    -   `SAILWWMCCV.CCU002_CVD_OUTCOMES_SUMMARY_FIRST_20210914`
-   Exposure tables for the CCU002-01 cohort:
    -   `SAILWWMCCV.CCU002_COVID19_TEST_ALL_20210914`
    -   `SAILWWMCCV.CCU002_COVID19_PRIMARY_EXPOSURE_20210914`
    -   `SAILWWMCCV.CCU002_COVID19_SECONDARY_EXPOSURE_20210914`
    -   `SAILWWMCCV.CCU002_COVID19_COHORT_20210914`
-   Covariates for the CCU002-01 cohort:
    -   `SAILWWMCCV.CCU002_COVARIATES_20210914`
-   CCU002-01 study cohort:
    -   `SAILWWMCCV.CCU002-01_COHORT_FULL_20210914`

