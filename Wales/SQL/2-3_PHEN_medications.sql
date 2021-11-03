--************************************************************************************************
-- Script:       2-3_PHEN_medications.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- About:        Determine covariate drug exposures (PHEN level)

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Create a table for medications
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_WDDS_COVARIATES (
    alf_e                                      bigint,
    bnf_combined                               char(255),
    dmdcode_prescribed                         char(30),
    dt_prescribed                              date,
    prac_cd_e                                  bigint,
    angiotensin_converting_enzyme_inhibitors   smallint,
    angiotensin_II_receptor_antagonists        smallint,
    other_antihypertensives                    smallint,
    proton_pump_inhibitors                     smallint,
    nsaids                                     smallint,
    corticosteriods                            smallint,
    other_immunosuppressants                   smallint,
    antiplatelet_agents                        smallint,
    oral_anticoagulants                        smallint,  -- PHEN_BNF_OACS
    anticoagulant                              smallint,  -- PHEN_BNF_ANTICOAGULANT_CCU002
    lipid_regulating_drugs                     smallint,  -- lipid_lowering
    insulin                                    smallint,
    sulphonylurea                              smallint,
    metformin                                  smallint,
    other_diabetic_drugs                       smallint,
    bp_lowering                                smallint,
    diabetes_medication                        smallint,
    hypertension_medication                    smallint,
    cocp                                       smallint,
    hrt                                        smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_WDDS_COVARIATES;
TRUNCATE TABLE SAILWWMCCV.PHEN_WDDS_COVARIATES IMMEDIATE;


INSERT INTO SAILWWMCCV.PHEN_WDDS_COVARIATES (alf_e,bnf_combined,dmdcode_prescribed,dt_prescribed,prac_cd_e)
    SELECT alf_e,
           bnf_combined,
           dmdcode_prescribed,
           dt_prescribed,
           prac_cd_e
    FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
    WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_ACEI
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_AIIRA
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_OAH
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_PROTON_PUMP_INH
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_NSAIDS
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_CORTICOSTEROIDS
                           WHERE is_latest = 1
                           )
     AND alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20);

INSERT INTO SAILWWMCCV.PHEN_WDDS_COVARIATES (alf_e,bnf_combined,dmdcode_prescribed,dt_prescribed,prac_cd_e)
    SELECT alf_e,
           bnf_combined,
           dmdcode_prescribed,
           dt_prescribed,
           prac_cd_e
    FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
    WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_IMMS_OTHER
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_ANTIPLATELET
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_OACS
                           WHERE is_latest = 1
                           )
     AND alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20);

INSERT INTO SAILWWMCCV.PHEN_WDDS_COVARIATES (alf_e,bnf_combined,dmdcode_prescribed,dt_prescribed,prac_cd_e)
    SELECT alf_e,
           bnf_combined,
           dmdcode_prescribed,
           dt_prescribed,
           prac_cd_e
    FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
    WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_LIPID_REG
                           WHERE is_latest = 1
                           )
     AND alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20)

INSERT INTO SAILWWMCCV.PHEN_WDDS_COVARIATES (alf_e,bnf_combined,dmdcode_prescribed,dt_prescribed,prac_cd_e)
    SELECT alf_e,
           bnf_combined,
           dmdcode_prescribed,
           dt_prescribed,
           prac_cd_e
    FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
    WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_INSULIN
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_SULFONYLUREAS
                           WHERE is_latest = 1
                           )
     AND alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20);

INSERT INTO SAILWWMCCV.PHEN_WDDS_COVARIATES (alf_e,bnf_combined,dmdcode_prescribed,dt_prescribed,prac_cd_e)
    SELECT alf_e,
           bnf_combined,
           dmdcode_prescribed,
           dt_prescribed,
           prac_cd_e
    FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
    WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_METFORMIN
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_ANTIDIABETIC_OTHER
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_BP_LOWERING
                           WHERE is_latest = 1
                           )
     AND alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20);

INSERT INTO SAILWWMCCV.PHEN_WDDS_COVARIATES (alf_e,bnf_combined,dmdcode_prescribed,dt_prescribed,prac_cd_e)
    SELECT alf_e,
           bnf_combined,
           dmdcode_prescribed,
           dt_prescribed,
           prac_cd_e
    FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
    WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_ANTICOAGULANT_CCU002
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_COCP
                           WHERE is_latest = 1
                           UNION ALL
                           SELECT code FROM SAILWWMCCV.PHEN_BNF_HRT
                           WHERE is_latest = 1
                           )
     AND alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20);

INSERT INTO SAILWWMCCV.PHEN_WDDS_COVARIATES (alf_e,bnf_combined,dmdcode_prescribed,dt_prescribed,prac_cd_e)
    SELECT alf_e,
           bnf_combined,
           dmdcode_prescribed,
           dt_prescribed,
           prac_cd_e
    FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
    WHERE dmdcode_prescribed IN (SELECT code FROM SAILWWMCCV.PHEN_DMD_DIABETES
                                 WHERE is_latest = 1
                                 )
     AND alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20);

INSERT INTO SAILWWMCCV.PHEN_WDDS_COVARIATES (alf_e,bnf_combined,dmdcode_prescribed,dt_prescribed,prac_cd_e)
    SELECT alf_e,
           bnf_combined,
           dmdcode_prescribed,
           dt_prescribed,
           prac_cd_e
    FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
    WHERE dmdcode_prescribed IN (SELECT code FROM SAILWWMCCV.PHEN_DMD_HYPERTENSION
                                 WHERE is_latest = 1
                                 )
     AND alf_e IN (SELECT alf_e FROM SAILWMCCV.C19_COHORT20)

-------------------------------------------------------------------------------------------------
-- angiotensin_converting_enzyme_inhibitors
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.angiotensin_converting_enzyme_inhibitors = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_ACEI)

-------------------------------------------------------------------------------------------------
-- angiotensin_II_receptor_antagonists
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.angiotensin_II_receptor_antagonists = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_AIIRA)

-------------------------------------------------------------------------------------------------
-- other_antihypertensives
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.other_antihypertensives = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_OAH)

-------------------------------------------------------------------------------------------------
-- proton_pump_inhibitors
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.proton_pump_inhibitors = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_PROTON_PUMP_INH);

-------------------------------------------------------------------------------------------------
-- nsaids
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.nsaids = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_NSAIDS);

-------------------------------------------------------------------------------------------------
-- corticosteriods
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.corticosteriods = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_CORTICOSTEROIDS);

-------------------------------------------------------------------------------------------------
-- other_immunosuppressants
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.other_immunosuppressants = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_IMMS_OTHER);

-------------------------------------------------------------------------------------------------
-- antiplatelet_agents
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.antiplatelet_agents = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_ANTIPLATELET);

-------------------------------------------------------------------------------------------------
-- oral_anticoagulants
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.oral_anticoagulants = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_OACS);

-------------------------------------------------------------------------------------------------
-- Anticoagulants (CCU002 codelist)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.anticoagulant = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_ANTICOAGULANT_CCU002);

-------------------------------------------------------------------------------------------------
-- lipid_regulating_drugs
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.lipid_regulating_drugs = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_LIPID_REG)

-------------------------------------------------------------------------------------------------
-- insulin
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.insulin = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_INSULIN);

-------------------------------------------------------------------------------------------------
-- sulphonylurea
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.sulphonylurea = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_SULFONYLUREAS);

-------------------------------------------------------------------------------------------------
-- metformin
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.metformin = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_METFORMIN);

-------------------------------------------------------------------------------------------------
-- other_diabetic_drugs
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.other_diabetic_drugs = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_ANTIDIABETIC_OTHER);

-------------------------------------------------------------------------------------------------
-- bp_lowering
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.bp_lowering = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_BP_LOWERING)

-------------------------------------------------------------------------------------------------
-- diabetes_medication
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.diabetes_medication = 1
WHERE dmdcode_prescribed IN (SELECT code FROM SAILWWMCCV.PHEN_DMD_DIABETES)

-------------------------------------------------------------------------------------------------
-- hypertension_medication
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.hypertension_medication = 1
WHERE dmdcode_prescribed IN (SELECT code FROM SAILWWMCCV.PHEN_DMD_HYPERTENSION)

-------------------------------------------------------------------------------------------------
-- cocp
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.cocp = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_COCP);

-------------------------------------------------------------------------------------------------
-- hrt
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WDDS_COVARIATES tgt
SET tgt.hrt = 1
WHERE bnf_combined IN (SELECT code FROM SAILWWMCCV.PHEN_BNF_HRT);

-------------------------------------------------------------------------------------------------