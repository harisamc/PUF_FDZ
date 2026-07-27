#!/usr/bin/env Rscript


# This version uses DuckDB and CSV files for local testing.
# For FDZ submission, use main.R with SAP HANA connection.
# ============================================
# LOCAL TESTING VERSION FOR CREATING DEMOGRAPHICS INFORMATION FOR ICD-10 Q780, OSTEOGENESIS IMPERFECTA
# ============================================
# INPUT: Public Use File, Datenmodell 3; source: https://zenodo.org/records/15057924
# DM3 (AMBDIAG, KHDIAG, VERS, VERSQ)
# RUN: source("./fdz_main_OI.r"); con=run_local_analysis()
# ============================================

#TODO; COLUMN ZUSATZ_CODE
#TODO; HOW IS THE OUTPUT PROVIDED?

# ============================================
# TBD:
# Approved Institution: [Institution Name]
# Projekttitel: ICD-10 .. Demographic and Comorbidity Analysis
# Projektkürzel: <ICD_DEMO_2025>; Projektbeschreibung: ...
# Aktenzeichen: <Aktenzeichen>

# ============================================
# Selection Criteria: 

# - PSIDs with the ICD-10 Q780; 
# - Q780 PSIDS with comorbidity codes "S02","S12","S22","S32","S42","S52","S62","S72",
# "S82","S92","T02","T08","T10","T12","I10","I15","M54",
# "R52","M25","M796","M798","M799")
# ============================================
# Workflow:
#
#
# ============================================

# Load libraries

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(ggplot2)
  library(tidyr)
})

if (!requireNamespace("duckdb", quietly = TRUE)) {
  cat("Installing duckdb package...\n")
  install.packages("duckdb")
}
library(duckdb)


suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
})

if (!requireNamespace("duckdb", quietly = TRUE)) install.packages("duckdb")
library(duckdb)

# ----------------------------
# Setup local DB from CSVs
setup_local_database = function(csv_dir = ".") {
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  dbWriteTable(con, "AMBDIAG", read.csv(file.path(csv_dir, "AMBDIAG.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
  dbWriteTable(con, "KHDIAG", read.csv(file.path(csv_dir, "KHDIAG.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
  dbWriteTable(con, "VERS", read.csv(file.path(csv_dir, "VERS.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
  dbWriteTable(con, "VERSQ", read.csv(file.path(csv_dir, "VERSQ.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
  
  dbExecute(con, "CREATE SCHEMA IF NOT EXISTS P31851_123")
  cat("Local DB setup done.\n")
  return(con)
}

# Description: Create base RD population from AMBDIAG and KHDIAG; ICD(RD) = Q780
# ------------------------------------------------------------------------------ #
# 1. Temporary Table 1: TT_BASE_ICD with the columns:  PSID ICD_CODE BJAHR
# ------------------------------------------------------------------------ #

create_base_tables = function(con, icd_rd_code, exact_match = TRUE) {
  if (exact_match) {
    where_amb <- sprintf("WHERE ICDAMB_CODE = '%s'", icd_rd_code)
    where_kh  <- sprintf("WHERE ICDKH_CODE = '%s'", icd_rd_code)
  } else {
    where_amb <- sprintf("WHERE ICDAMB_CODE LIKE '%s%%'", icd_rd_code)
    where_kh  <- sprintf("WHERE ICDKH_CODE LIKE '%s%%'", icd_rd_code)
  }
  
  sql <- paste0(
    "CREATE LOCAL TEMP TABLE TT_BASE_ICD AS
     SELECT DISTINCT PSID, ICDAMB_CODE AS ICD_CODE, BJAHR FROM AMBDIAG ", where_amb, "
     UNION ALL
     SELECT DISTINCT PSID, ICDKH_CODE AS ICD_CODE, BJAHR FROM KHDIAG ", where_kh
  )
  
  dbExecute(con, sql)

}

# Description: Create the demographics table for the RD 
# ------------------------------------------------------------------------ #
# Temporary Table 2: TT_DEMOGRAPHICS_RD_UNIQUE with the columns:
# PSID ICD_CODE BJAHR PLZ ALTER AGE_GROUP GESCHLECHT_LABEL
# ------------------------------------------------------------------------ #

create_demographics_table = function(con, current_year = 2025) {
  sql <- paste0("
   CREATE LOCAL TEMP TABLE TT_DEMOGRAPHICS_RD_UNIQUE AS
  SELECT 
    i.PSID, 
    i.ICD_CODE, 
    i.BJAHR,
    v.PLZ,                                
    (2025 - v.GEBJAHR) AS ALTER,
    CASE 
        WHEN (2025 - v.GEBJAHR) BETWEEN 0 AND 19 THEN '0-19'
        WHEN (2025 - v.GEBJAHR) BETWEEN 20 AND 39 THEN '20-39'
        WHEN (2025 - v.GEBJAHR) BETWEEN 40 AND 59 THEN '40-59'
        WHEN (2025 - v.GEBJAHR) >= 60 THEN '60+'
        ELSE 'Unknown' END AS AGE_GROUP,
    CASE 
        WHEN vq.GESCHLECHT = 1 THEN 'Female'
        WHEN vq.GESCHLECHT = 2 THEN 'Male'
        ELSE 'Unknown' END AS GESCHLECHT_LABEL
  FROM TT_BASE_ICD i
  INNER JOIN (
    SELECT PSID, MIN(GEBJAHR) AS GEBJAHR, MIN(PLZ) AS PLZ -- do i need this?
    FROM VERS
    GROUP BY PSID
  ) v ON i.PSID = v.PSID
  INNER JOIN (
    SELECT PSID, GESCHLECHT
    FROM (
        SELECT PSID, GESCHLECHT, ROW_NUMBER() OVER (PARTITION BY PSID ORDER BY PSID) AS rn
        FROM VERSQ
    ) sub
    WHERE rn = 1
  ) vq ON i.PSID = vq.PSID;
")
  dbExecute(con, sql)
}


# Description: Create the table for the 2-digit PLZ of RD PSIDs (Q780)
# ------------------------------------------------------------------------ #
# Results Table 1: RT_RD_PLZ_GROUPED with the columns:  PLZ2 CNT_D_PSID  
# ------------------------------------------------------------------------ #

count_rd_plz=function(con){
  dbExecute(con, "
  CREATE TABLE RT_RD_PLZ_GROUPED AS
    SELECT
      SUBSTRING(PLZ, 1, 2) AS PLZ2,
      COUNT(DISTINCT PSID) AS CNT_D_PSID
    FROM TT_DEMOGRAPHICS_RD_UNIQUE
    GROUP BY SUBSTRING(PLZ, 1, 2)
    ORDER BY CNT_D_PSID DESC;
  ")
}



# Description: Create the demographics table excluding RDs, remaining pop TT_REMAINING_POP
# ---------------------------------------------------------------------------------------- #
# Temporary Table 3: TT_REMAINING_POP with the columns: PSID BJAHR ICD_CODE
# ------------------------------------------------------------------------ #
 
create_base_population_remaining = function(con) {
  cat("Creating remaining population (excluding RD patients)...\n")
  
  dbExecute(con, "
    CREATE LOCAL TEMP TABLE TT_REMAINING_POP AS
    -- Take all patients from AMBDIAG
    SELECT a.PSID, a.BJAHR, a.ICDAMB_CODE AS ICD_CODE
    FROM AMBDIAG a
    LEFT JOIN TT_BASE_ICD r
      ON a.PSID = r.PSID
    WHERE r.PSID IS NULL

    UNION ALL

    -- Take all patients from KHDIAG
    SELECT k.PSID, k.BJAHR, k.ICDKH_CODE AS ICD_CODE
    FROM KHDIAG k
    LEFT JOIN TT_BASE_ICD r
      ON k.PSID = r.PSID
    WHERE r.PSID IS NULL
  ")
}

# Description: Create the demographics table for the remaining population
# ------------------------------------------------------------------------ #
# Temporary Table 4: TT_DEMOGRAPHICS_REMAIN with the columns: 
# PSID ICD_CODE BJAHR   PLZ ALTER AGE_GROUP GESCHLECHT_LABEL
# ------------------------------------------------------------------------ #

create_demographics_table_remaining = function(con, current_year = 2025) {
  sql <- paste0("
    CREATE LOCAL TEMP TABLE TT_DEMOGRAPHICS_REMAIN AS
    SELECT 
      r.PSID, 
      r.ICD_CODE, 
      r.BJAHR,
      v.PLZ,
      (", current_year, " - v.GEBJAHR) AS ALTER,
      CASE 
          WHEN (", current_year, " - v.GEBJAHR) BETWEEN 0 AND 19 THEN '0-19'
          WHEN (", current_year, " - v.GEBJAHR) BETWEEN 20 AND 39 THEN '20-39'
          WHEN (", current_year, " - v.GEBJAHR) BETWEEN 40 AND 59 THEN '40-59'
          WHEN (", current_year, " - v.GEBJAHR) >= 60 THEN '60+'
          ELSE 'Unknown' END AS AGE_GROUP,
      CASE 
          WHEN vq.GESCHLECHT = 1 THEN 'Female'
          WHEN vq.GESCHLECHT = 2 THEN 'Male'
          ELSE 'Unknown' END AS GESCHLECHT_LABEL
    FROM TT_REMAINING_POP r
    INNER JOIN (
      SELECT PSID, MIN(GEBJAHR) AS GEBJAHR, MIN(PLZ) AS PLZ
      FROM VERS
      GROUP BY PSID
    ) v ON r.PSID = v.PSID
    INNER JOIN (
      SELECT PSID, GESCHLECHT
      FROM (
          SELECT PSID, GESCHLECHT, ROW_NUMBER() OVER (PARTITION BY PSID ORDER BY PSID) AS rn
          FROM VERSQ
      ) sub
      WHERE rn = 1
    ) vq ON r.PSID = vq.PSID;
  ")
  
  dbExecute(con, sql)
}



# Description:
# Create the resulting table for the age and sex distribution of the remaining population
# --------------------------------------------------------------------------------------- #
# Results Table 2: RT_REMAINING_POP_AGE_SEX_DIST with the columns:
#  ICD_CODE BJAHR ALTERSGRUPPE GESCHLECHT_LABEL CNT_D_PSID
# ------------------------------------------------------------------------ #
create_age_sex_distribution_remaining = function(con) {
  sql <- "
   CREATE TABLE RT_REMAINING_POP_AGE_SEX_DIST AS
    SELECT 
      ICD_CODE,
      BJAHR,
      AGE_GROUP AS ALTERSGRUPPE,
      GESCHLECHT_LABEL,
      COUNT(PSID) AS CNT_D_PSID,
     -- STRING_AGG(DISTINCT PLZ, ',') AS PLZ      
    FROM TT_DEMOGRAPHICS_REMAIN
  GROUP BY ICD_CODE, BJAHR, AGE_GROUP, GESCHLECHT_LABEL;

  "
  dbExecute(con, sql)
}



# Description: Age-sex distribution of RD
# ------------------------------------------------------------------------ #
# Results Table 3: RT_RD_AGE_SEX_DIST with the columns:
# ICD_CODE BJAHR ALTERSGRUPPE GESCHLECHT_LABEL CNT_D_PSID
# ------------------------------------------------------------------------ #
create_age_sex_distribution = function(con) {
  sql <- "
   CREATE TABLE RT_RD_AGE_SEX_DIST AS
    SELECT 
      ICD_CODE,
      BJAHR,
      AGE_GROUP AS ALTERSGRUPPE,
      GESCHLECHT_LABEL,
      COUNT(PSID) AS CNT_D_PSID,
     -- STRING_AGG(DISTINCT PLZ, ',') AS PLZ      
    FROM TT_DEMOGRAPHICS_RD_UNIQUE
  GROUP BY ICD_CODE, BJAHR, AGE_GROUP, GESCHLECHT_LABEL;

  "
  dbExecute(con, sql)
}



# Description: Create a table of age and sex distribution for the specific ICD-Codes that accompany 
# the rare disease Q780
# ------------------------------------------------------------------------ #
# Temporary Table 5: TT_SPECIFIC_ICD with columns:
# PSID ICD_CODE BJAHR
# Temporary Table 6: TT_SPECIFIC_ICD_AGE_SEX_DIST with columns:
# PSID ICD_CODE BJAHR GESCHLECHT_LABEL AGE_GROUP
# ------------------------------------------------------------------------ #
# Results Table 4: RT_SPECIFIC_ICD_AGE_SEX_DIST with columns:
# ICD_CODE BJAHR GESCHLECHT_LABEL AGE_GROUP CNT_D_PSID
# ------------------------------------------------------------------------ #

create_icd_occurrence_table_rd = function(con, icd_list) {
  
  # ---- Define which codes are exact matches ----
  exact_codes <- c("R53", "F480", "T733")
  
  # ---- Split lists ----
  prefix_codes <- setdiff(icd_list, exact_codes)  # codes for prefix grouping
  
  # Build exact match clauses
  exact_clause_amb <- paste0("ICDAMB_CODE IN ('", paste(exact_codes, collapse="','"), "')")
  exact_clause_kh  <- paste0("ICDKH_CODE IN ('", paste(exact_codes, collapse="','"), "')")
  
  # Build prefix LIKE clauses
  prefix_clause_amb <- paste0("ICDAMB_CODE LIKE '", prefix_codes, "%'", collapse = " OR ")
  prefix_clause_kh  <- paste0("ICDKH_CODE LIKE '", prefix_codes, "%'", collapse = " OR ")
  
  sql <- paste0("
    CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD AS

    -- EXACT MATCH BLOCK
    SELECT DISTINCT
      a.PSID,
      a.ICDAMB_CODE AS ICD_CODE,
      a.ICDAMB_CODE AS ICD_GROUP,     
      a.BJAHR
    FROM AMBDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID=b.PSID
    WHERE ", exact_clause_amb, "

    UNION ALL

    SELECT DISTINCT
      a.PSID,
      a.ICDKH_CODE AS ICD_CODE,
      a.ICDKH_CODE AS ICD_GROUP,     
      a.BJAHR
    FROM KHDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID=b.PSID
    WHERE ", exact_clause_kh, "

    UNION ALL

    -- PREFIX GROUPING BLOCK
    SELECT DISTINCT
      a.PSID,
      a.ICDAMB_CODE AS ICD_CODE,
      SUBSTRING(a.ICDAMB_CODE, 1, 3) AS ICD_GROUP,   -- group by prefix
      a.BJAHR
    FROM AMBDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID=b.PSID
    WHERE ", prefix_clause_amb, "

    UNION ALL

    SELECT DISTINCT
      a.PSID,
      a.ICDKH_CODE AS ICD_CODE,
      SUBSTRING(a.ICDKH_CODE, 1, 3) AS ICD_GROUP,   -- group by prefix
      a.BJAHR
    FROM KHDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID=b.PSID
    WHERE ", prefix_clause_kh, "
  ")
  
  dbExecute(con, sql)
  
  # demographics join
  dbExecute(con, "
    CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD_AGE_SEX_DIST AS
    SELECT 
      i.PSID,
      i.ICD_GROUP,
      i.BJAHR,
      d.GESCHLECHT_LABEL,
      d.AGE_GROUP
    FROM TT_SPECIFIC_ICD i
    LEFT JOIN TT_DEMOGRAPHICS_RD_UNIQUE d
      ON i.PSID = d.PSID
  ")
  
  # final aggregation
  dbExecute(con, "
    CREATE TABLE RT_SPECIFIC_ICD_AGE_SEX_DIST AS
    SELECT 
      ICD_GROUP,
      BJAHR,
      GESCHLECHT_LABEL,
      AGE_GROUP,
      COUNT(DISTINCT PSID) AS CNT_D_PSID
    FROM TT_SPECIFIC_ICD_AGE_SEX_DIST
    GROUP BY ICD_GROUP, BJAHR, GESCHLECHT_LABEL, AGE_GROUP
    ORDER BY ICD_GROUP, BJAHR, GESCHLECHT_LABEL, AGE_GROUP
  ")
}



# Main workflow
run_local_analysis = function() {
  con <- setup_local_database(".")
  
  icd_rd_code <- "Q780"
  icd_list <- c("S02","S12","S22","S32","S42","S52","S62","S72",
                "S82","S92","T02","T08","T10","T12","I10","I15","M54",
                "R52","M25","M796","M798","M799")
  
  create_base_tables(con, icd_rd_code, exact_match=TRUE)
  create_demographics_table(con, current_year=2025)
  create_age_sex_distribution(con)
  create_icd_occurrence_table_rd(con, icd_list)
  create_base_population_remaining(con)
  create_demographics_table_remaining(con, current_year=2025)
  create_age_sex_distribution_remaining(con)
  count_rd_plz(con)
  

  cat("All RD tables created successfully.\n")
  return(con)
}







con <- run_local_analysis()
rt_remaining_pop_age_sex_dist=dbGetQuery(con, "SELECT * FROM RT_REMAINING_POP_AGE_SEX_DIST") 
write.csv(rt_remaining_pop_age_sex_dist, "rt_remaining_pop_age_sex_dist.csv", row.names = FALSE)

rt_rd_plz_grouped=dbGetQuery(con, "SELECT * FROM RT_RD_PLZ_GROUPED")
write.csv(rt_rd_plz_grouped, "rt_rd_plz_grouped.csv", row.names = FALSE)

rt_rd_age_sex_dist = dbGetQuery(con, "SELECT * FROM RT_RD_AGE_SEX_DIST")
write.csv(rt_rd_age_sex_dist, "rt_rd_age_sex_dist.csv", row.names = FALSE)


rt_specific_icd_age_sex_dist=dbGetQuery(con, "SELECT * FROM RT_SPECIFIC_ICD_AGE_SEX_DIST")
write.csv(rt_specific_icd_age_sex_dist, "rt_specific_icd_age_sex_dist.csv", row.names = FALSE)


# Example inspect table
#dbGetQuery(con, "SELECT * FROM TT_SPECIFIC_ICD")

