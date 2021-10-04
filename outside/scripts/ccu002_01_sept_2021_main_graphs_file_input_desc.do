*description of file input layout for the corresponding do file*

****local fully_adj="" // file with fully adjusted results

*column 1 - row numbers, blank header
*column 2 - event
*column 3 - agegp
*column 4 - sex
*column 5 - term
*column 6 - estimate
*column 7 - conf.low
*column 8 - conf.high
*column 9 - p.value
*column 10 - std.error


****local age_sex_reg_adj="" // file with age/sex/region adjusted results

**as above**


****local full_adj_sex_strat_m="" // file with male stratified fully adjusted results
****local full_adj_sex_strat_f="" // file with female stratified fully adjusted results

*as above*

****local full_adj_ethn_strat_asian="" // file with ethnicity stratified fully adjusted results - asian
****local full_adj_ethn_strat_black="" // file with ethnicity stratified fully adjusted results - black
****local full_adj_ethn_strat_mixed="" // file with ethnicity stratified fully adjusted results - mixed
****local full_adj_ethn_strat_other="" // file with ethnicity stratified fully adjusted results - other
****local full_adj_ethn_strat_white="" // file with ethnicity stratified fully adjusted results - white

*as above*

****local full_adj_sever_strat="" // file with disease severity stratified (hospitalised/non-hospitalised) fully adjusted results

*column 1 - row numbers, blank header
*column 2 - covidpheno
*column 3 - event
*column 4 - agegp
*column 5 - sex
*column 6 - term
*column 7 - estimate
*column 8 - conf.low
*column 9 - conf.high
*column 10 - p.value
*column 11 - std.error


****local full_adj_medhis_strat_event="" // file with medical history stratified fully adjusted results - yes event

*as per lines 5-14*


****local full_adj_medhis_strat_noevent="" // file with medical history stratified fully adjusted results - no event

*as per lines 5-14*







