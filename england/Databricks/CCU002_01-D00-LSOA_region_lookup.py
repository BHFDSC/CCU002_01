# Databricks notebook source
# MAGIC 
# MAGIC %md # CCU002_01-D00-LSOA_region_lookup
# MAGIC  
# MAGIC **Description** Creates a LSOA to region lookup table. This code was originally created by by Sam Hollings.
# MAGIC 
# MAGIC **Author(s)** Sam Hollings
# MAGIC 
# MAGIC **Project(s)** CCU002_01
# MAGIC 
# MAGIC **Reviewer(s)** 
# MAGIC  
# MAGIC **Date last updated** 
# MAGIC  
# MAGIC **Date last reviewed** 
# MAGIC  
# MAGIC **Date last run** # MAGIC  
# MAGIC **Data input** `dss_corporate.ons_chd_geo_listings`
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC **Software and versions** SQL, Python
# MAGIC  
# MAGIC **Packages and versions** Not applicable

# COMMAND ----------

%run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU002_01/CCU002_01-functions/wrang000_functions"

# COMMAND ----------

%sql
CREATE or replace global temporary view  ccu002_01_lsoa_region_lookup AS
with 
curren_chd_geo_listings as (SELECT * FROM dss_corporate.ons_chd_geo_listings WHERE IS_CURRENT = 1),
lsoa_auth as (
   	SELECT e01.geography_code as lsoa_code, e01.geography_name         	lsoa_name, 
	e02.geography_code as msoa_code, e02.geography_name as 	msoa_name, 
	e0789.geography_code as authority_code, e0789.geography_name 	as authority_name,
	e0789.parent_geography_code as authority_parent_geography
	FROM curren_chd_geo_listings e01
	LEFT JOIN curren_chd_geo_listings e02 on e02.geography_code 	= e01.parent_geography_code
	LEFT JOIN curren_chd_geo_listings e0789 on 	e0789.geography_code = e02.parent_geography_code
	WHERE e01.geography_code like 'E01%' and e02.geography_code 	like 'E02%'
),
auth_county as (
   SELECT lsoa_code, lsoa_name,
          msoa_code, msoa_name,
          authority_code, authority_name,
          e10.geography_code as county_code, e10.geography_name as county_name,
          e10.parent_geography_code as parent_geography
   FROM 
   lsoa_auth
     LEFT JOIN dss_corporate.ons_chd_geo_listings e10 on e10.geography_code = lsoa_auth.authority_parent_geography
   
   WHERE LEFT(authority_parent_geography,3) = 'E10'
 ),
 auth_met_county as (
   SELECT lsoa_code, lsoa_name,
            msoa_code, msoa_name,
            authority_code, authority_name,
            NULL as county_code, NULL as county_name,           
            lsoa_auth.authority_parent_geography as region_code
   FROM lsoa_auth
   WHERE LEFT(authority_parent_geography,3) = 'E12'
 ),
 lsoa_region_code as (
   SELECT lsoa_code, lsoa_name,
            msoa_code, msoa_name,
            authority_code, authority_name,
            county_code, county_name, auth_county.parent_geography as region_code
   FROM auth_county
   UNION ALL
   SELECT lsoa_code, lsoa_name,
            msoa_code, msoa_name,
            authority_code, authority_name,
            county_code, county_name, region_code 
   FROM auth_met_county
 ),
 lsoa_region as (
   SELECT lsoa_code, lsoa_name,
            msoa_code, msoa_name,
            authority_code, authority_name,
            county_code, county_name, region_code, e12.geography_name as region_name FROM lsoa_region_code
   LEFT JOIN dss_corporate.ons_chd_geo_listings e12 on lsoa_region_code.region_code = e12.geography_code
 )
 SELECT * FROM lsoa_region


 