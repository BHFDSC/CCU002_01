*define working directory and change to this directory
global folder ""
cd "$folder"
clear

*install metan

INSERT METAN INSTALL HERE

*open log file
capture log close
log using 20211001_ccu002_01_sept_2021_main_graphs, replace

*define filename local variables
***for venous and arterial events***
local fully_adj="" // file with fully adjusted results - age stratified not meta-analysed
local age_sex_reg_adj="" // file with age/sex/region adjusted results - age stratified not meta-analysed
local full_adj_sex_strat_m="" // file with male stratified fully adjusted results - meta-analysed across age groups
local full_adj_sex_strat_f="" // file with female stratified fully adjusted results - meta-analysed across age groups
local full_adj_ethn_strat_asian="" // file with ethnicity stratified fully adjusted results - asian (meta-analysed across age groups)
local full_adj_ethn_strat_black="" // file with ethnicity stratified fully adjusted results - black (meta-analysed across age groups)
local full_adj_ethn_strat_mixed="" // file with ethnicity stratified fully adjusted results - mixed (meta-analysed across age groups)
local full_adj_ethn_strat_other="" // file with ethnicity stratified fully adjusted results - other (meta-analysed across age groups)
local full_adj_ethn_strat_white="" // file with ethnicity stratified fully adjusted results - white (meta-analysed across age groups)
local full_adj_sever_strat="" // file with disease severity stratified (hospitalised/non-hospitalised) fully adjusted results - age stratified not meta-analysed
local full_adj_medhis_strat_event="" // file with medical history stratified fully adjusted results (yes event) - age stratified not meta-analysed
local full_adj_medhis_strat_noevent="" // file with medical history stratified fully adjusted results (no event) - age stratified not meta-analysed


*loop over graph creation for arterial and venous outcomes
local scen "Arterial Venous"
foreach out of local scen {

	clear
	import delimited using "`fully_adj'" , varnames(1)
	keep if outcome=="`scen'_event"
	local eventnam="`scen' event"
	generate keeper=index(term,"week")
	tab keeper
	keep if keeper==1
	drop keeper
	gen weeks=substr(term,5,5)
	tab weeks

	*apply mid-points
	replace weeks="0.5" if weeks=="1"
	replace weeks="1.5" if weeks=="2"
	replace weeks="3" if weeks=="3_4"
	replace weeks="2" if weeks=="1_4"
	replace weeks="6" if weeks=="5_8"
	replace weeks="10" if weeks=="9_12"
	replace weeks="19" if weeks=="13_26"
	replace weeks="37.5" if weeks=="27_49"
	replace weeks="26.5" if weeks=="5_49"

	destring weeks, replace

	table weeks term 
	
	gen byte fuladj=1
	
	keep event term sex estimate conflow confhigh stderror weeks agegp fuladj
	replace estimate=. if conflow<0.001
	replace confhigh=. if conflow<0.001
	replace conflow=. if conflow<0.001
	replace estimate=. if confhigh>1000
	replace conflow=. if confhigh>1000
	replace confhigh=. if confhigh>1000
	replace estimate=. if stderror>1000
	replace conflow=. if stderror>1000
	replace confhigh=. if stderror>1000
	
	***meta-analyse across age groups to give summary estimates***
	
	INSERT CODE
	gen lnestimate=ln(estimate)
	gen lnconflow=ln(conflow)
	gen lnconfhigh=ln(confhigh)
	metan lnestimate lnconflow lnconfhigh, eform effect (OR) 
	
	*now make the meta-analysed estimates appear as a row also
	
	
	
	*now clean meta-analysed estimates removing unreliable results
	
	INSERT CODE
	
	**************************************************************
	
	
	compress
	save fully_adj_est_clean.dta, replace // this can be used for plotting age-stratified results also
	
	clear 
	import delimited using "`age_sex_reg_adj''" , varnames(1)
	keep if outcome=="`scen'_event"
	generate keeper=index(term,"week")
	tab keeper
	keep if keeper==1
	drop keeper
	gen weeks=substr(term,5,5)
	tab weeks

	replace weeks="0.5" if weeks=="1"
	replace weeks="1.5" if weeks=="2"
	replace weeks="3" if weeks=="3_4"
	replace weeks="2" if weeks=="1_4"
	replace weeks="6" if weeks=="5_8"
	replace weeks="10" if weeks=="9_12"
	replace weeks="19" if weeks=="13_26"
	replace weeks="37.5" if weeks=="27_49"
	replace weeks="26.5" if weeks=="5_49"

	destring weeks, replace

	table weeks term 
	
	gen byte fuladj=0
	
	keep event term sex estimate conflow confhigh stderror weeks agegp fuladj
	replace estimate=. if conflow<0.001
	replace confhigh=. if conflow<0.001
	replace conflow=. if conflow<0.001
	replace estimate=. if confhigh>1000
	replace conflow=. if confhigh>1000
	replace confhigh=. if confhigh>1000
	replace estimate=. if stderror>1000
	replace conflow=. if stderror>1000
	replace confhigh=. if stderror>1000
	
	***meta-analyse across age groups to give summary estimates***
	
	INSERT CODE
	gen lnestimate=ln(estimate)
	gen lnconflow=ln(conflow)
	gen lnconfhigh=ln(confhigh)
	metan lnestimate lnconflow lnconfhigh, eform effect (OR) 
	
	*now make the meta-analysed estimates appear as a row also
	
	
	
	*now clean meta-analysed estimates removing unreliable results
	
	INSERT CODE
	
	**************************************************************
	
	compress
	save agesexreg_adj_est_clean.dta, replace
	
	*append so we have fully adjusted and age/sex/reg adjusted together
	append using fully_adj_est_clean.dta
	
	*re-shape wide
	reshape wide estimate conflow confhigh stderror, i(event term sex weeks agegp) j(adjusted)

	
	
	
	
	
	
	
	
	
	
	
	
	START HERE - MAKE PLOTS 1 AND 2 AND SAVE, THEN MOVE ONTO PLOTS 3-6
	
	
	
	
	
	gen hospitalised=.
	replace hospitalised=0 if covidpheno=="non_hospitalised"
	replace hospitalised=1 if covidpheno=="hospitalised"
	compress
	save postcovidhr_august_2021_hosp_nonhosp, replace

	keep event hospitalised term sex estimate conflow confhigh stderror weeks agegp 
	replace estimate=. if conflow<0.001
	replace confhigh=. if conflow<0.001
	replace conflow=. if conflow<0.001
	replace estimate=. if confhigh>1000
	replace conflow=. if confhigh>1000
	replace confhigh=. if confhigh>1000
	replace estimate=. if stderror>1000
	replace conflow=. if stderror>1000
	replace confhigh=. if stderror>1000

	reshape wide estimate conflow confhigh stderror, i(event term sex weeks agegp) j(hospitalised)

	save postcovidhr_august_2021_hosp_nonhosp_wide, replace


	global eventnum=0

	tempfile tmpp // TB added 20210812

	foreach outcome in "Venous_event" "PE" "Arterial_event" "AMI" "stroke_isch"  {

		use postcovidhr_august_2021_hosp_nonhosp_wide, clear

		global eventnum=$eventnum+1
		local en=$eventnum

		display _n "event number " $eventnum

		keep if event=="`outcome'"

		local eventnam "`outcome'"


		if "`outcome'"=="Venous_event" { 
			display "Here VT"
			local eventnam "venous event"
		}

		if "`outcome'"=="DVT_event" {
			local eventnam "DVT"
		}
		 
		if "`outcome'"=="other_DVT" { 
			local eventnam "other DVT"
		}

		if "`outcome'"=="Arterial_event" {
			local eventnam "arterial event"
		}
		
		if "`outcome'"=="AMI" {
			local eventnam "acute MI"
		}

		if "`outcome'"=="stroke_isch" { 
			local eventnam "Ischaemic stroke"
		}

		if "`outcome'"=="stroke_SAH_HS" { 
			local eventnam "Haemorrhagic stroke"
		}
	
		if "`outcome'"=="stroke_TIA" {
			local eventnam "TIA"
		}

		if "`outcome'"=="life_arrhythmia" {
			local eventnam "LT arrhythmia"
		}

		if "`outcome'"=="other_arterial_embolism" {
			local eventnam "other arterial embolism"
		}

		if "`outcome'"=="mesenteric_thrombus" {
			local eventnam "mesenteric thrombus"
		}

		if "`outcome'"=="Haematological_event" {
			local eventnam "haematological event"
		}

		keep if sex=="all"
		count
		tab agegp

		gen agenam=agegp
		replace agenam="lt40" if agenam=="<40"
		replace agenam="ge80" if agenam==">=80"

		list, noobs nodisp clean
		*	keep event term estimate conflow confhigh stderror weeks agegp agenam
		
		replace term=trim(term)
		sort term
	
		save `tmpp', replace // TB added 20210812
	
		foreach agegrp in "<40" "40-59" "60-79" ">=80" {
			//preserve
			use `tmpp', clear // TB added 20210812
			assert event=="`outcome'"
			drop event
		
			keep if agegp=="`agegrp'"
			local nam=agenam[1]

			drop if estimate0==0
			drop if estimate1==0

			count
			sort weeks 
			list weeks term estimate0 conflow0 confhigh0 estimate1 conflow1 confhigh1 weeks agegp, noobs nodisp clean

			

			graph twoway (connected estimate0 weeks, mcolor(gs7) lcolor(gs7) msize(small)) || ///
			(rspike conflow0 confhigh0 weeks, lcolor(gs7)) || ///
			(connected estimate1 weeks, mcolor(blue) lcolor(blue) msize(small)) || ///
			(rspike conflow1 confhigh1 weeks, lcolor(blue)), ////
			yscale(log range(0.25 24)) ///		 
			xlab(0(4)44) yline(1) ylab(0.25 0.5 1 2 4 8 16 32) ytitle("Hazard ratio for `eventnam'") ///
			legend(off) xtitle("Weeks since COVID") ///
			subtitle("Age `agegrp'" "Pre-non-hospitalised COVID: `eventcountpre_nohosp', post-non-hospitalised COVID: `eventcountpost_nohosp'" "Pre-hospitalised COVID: `eventcountpre_hosp', post-hospitalised COVID: `eventcountpost_hosp'", position(1) ring(0) size(vsmall)) ///
			saving("$folder\agegraphs_hosp\\`en'_`outcome'_postcovidhr`nam'.gph", replace) 

			graph export "$folder\agegraphs_hosp\\`en'_`outcome'postcovid`nam'.svg", as(svg) replace
			more
			//restore
		}

		graph combine "$folder\agegraphs_hosp\\`en'_`outcome'_postcovidhrlt40.gph" "$folder\agegraphs_hosp\\`en'_`outcome'_postcovidhr40-59.gph" ///
		"$folder\agegraphs_hosp\\`en'_`outcome'_postcovidhr60-79.gph" "$folder\agegraphs_hosp\\`en'_`outcome'_postcovidhrge80.gph", ///
		xcommon saving("$folder\graphs3\\`en'_`outcome'_postcovid.gph", replace)
		graph export "$folder\graphs3\\`en'_`outcome'_postcovid.svg", as(svg) replace

	}



}


*close log file
log close 
clear 