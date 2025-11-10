#!/usr/bin/env Rscript

# ============================================
# LOCAL TESTING VERSION

# This version uses DuckDB and CSV files for local testing.
# For FDZ submission, use main.R with SAP HANA connection.
# ============================================

# INPUT: Public Use File, Datenmodell 3; source: https://zenodo.org/records/15057924
# RUN: source("./fdz_main_r_script.r")


# Approved Institution: [Institution Name]
# Projekttitel: ICD-10 Demographic and Comorbidity Analysis
# Projektkürzel: <ICD_DEMO_2024>; Projektbescheirbung: ...
# Aktenzeichen: <Aktenzeichen>
# Selektionskriterien: Ambulante Diagnosen, ICD-10 Codes C22.2, C72.8, C81.2, C81.7
# ============================================

#TODO; remove year filtering?

# ============================================
# 1. LOAD LIBRARIES

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

# 2. DEFINE FUNCTIONS
# ============================================

# Function: Load CSV files and create DuckDB database
# This step is only for local testing. Inside FDZ, the schema and tables are provided based on proposal
setup_local_database = function(csv_dir = ".") {
  cat("Setting up local database from CSV files...\n")
  
  con = dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  ambdiag_file = file.path(csv_dir, "AMBDIAG.csv")
  vers_file = file.path(csv_dir, "VERS.csv")
  versq_file = file.path(csv_dir, "VERSQ.csv")
  
  cat("Loading CSV files...\n")
  ambdiag = read.csv(ambdiag_file, stringsAsFactors = FALSE)
  vers = read.csv(vers_file, stringsAsFactors = FALSE)
  versq = read.csv(versq_file, stringsAsFactors = FALSE)
  
  dbWriteTable(con, "AMBDIAG", ambdiag, overwrite = TRUE)
  dbWriteTable(con, "VERS", vers, overwrite = TRUE)
  dbWriteTable(con, "VERSQ", versq, overwrite = TRUE)
  
  dbExecute(con, "CREATE SCHEMA IF NOT EXISTS P31851_123")
  
  cat("Database setup complete.\n")
  return(con)
}

# ============================================
# 1. Select rare disease population (multi ICD-code)

create_base_population_multi = function(con, icd_codes, exact_match = FALSE) {
  cat("Creating RD population for multiple ICD codes...\n")
  
  if (exact_match) {
    icd_list = paste0("'", icd_codes, "'", collapse = ", ")
    where_clause = paste0("WHERE ICDAMB_CODE IN (", icd_list, ")")
  } else {
    like_clauses = paste0("ICDAMB_CODE LIKE '", icd_codes, "%'", collapse = " OR ")
    where_clause = paste0("WHERE ", like_clauses)
  }
  
  sql_query = sprintf("
    CREATE LOCAL TEMP TABLE TT_BASE_ICD AS
    SELECT DISTINCT PSID, ICDAMB_CODE, BJAHR
    FROM AMBDIAG
    %s", where_clause)
  
  dbExecute(con, sql_query)
  # Forbidden to print; remove for final
  count = dbGetQuery(con, "SELECT COUNT(DISTINCT PSID) as n FROM TT_BASE_ICD")$n
  cat("Found", count, "patients with selected ICD codes.\n")
}

# ============================================
# 2. Demographics table

create_demographics_table = function(con, current_year = as.numeric(format(Sys.Date(), "%Y"))) {
  cat("Creating demographics table...\n")
  
  sql_query = sprintf("
    CREATE LOCAL TEMP TABLE TT_DEMOGRAPHICS AS
    SELECT DISTINCT
      i.PSID,
      i.ICDAMB_CODE,
      i.BJAHR,
      v.GEBJAHR,
      vq.GESCHLECHT,
      (%d - v.GEBJAHR) AS ALTER
    FROM TT_BASE_ICD i
    INNER JOIN VERS v ON i.PSID = v.PSID
    INNER JOIN VERSQ vq ON i.PSID = vq.PSID
    WHERE v.GEBJAHR IS NOT NULL
      AND vq.GESCHLECHT IN (1, 2)
  ", current_year)
  
  dbExecute(con, sql_query)
  
  # For local debugging only (comment out for FDZ)
  if (interactive()) {
    sample_data = dbGetQuery(con, "SELECT * FROM TT_DEMOGRAPHICS LIMIT 5")
    print(sample_data)
  }
}

# ============================================
# 3. Age-sex distribution

create_age_sex_distribution = function(con) {
  cat("Creating age-sex distribution table...\n")
  
  sql_query = "
    CREATE TABLE RT_AGE_SEX_DIST AS
    SELECT 
      ICDAMB_CODE,
      BJAHR,
      CASE 
        WHEN ALTER < 18 THEN '0-17'
        WHEN ALTER BETWEEN 18 AND 29 THEN '18-29'
        WHEN ALTER BETWEEN 30 AND 39 THEN '30-39'
        WHEN ALTER BETWEEN 40 AND 49 THEN '40-49'
        WHEN ALTER BETWEEN 50 AND 59 THEN '50-59'
        WHEN ALTER BETWEEN 60 AND 69 THEN '60-69'
        WHEN ALTER BETWEEN 70 AND 79 THEN '70-79'
        WHEN ALTER >= 80 THEN '80+'
        ELSE 'Unknown'
      END AS ALTERSGRUPPE,
      CASE 
        WHEN GESCHLECHT = 1 THEN 'Female'
        WHEN GESCHLECHT = 2 THEN 'Male'
        ELSE 'Unknown'
      END AS GESCHLECHT_LABEL,
      COUNT(DISTINCT PSID) AS CNT_D_PSID
    FROM TT_DEMOGRAPHICS
    GROUP BY ICDAMB_CODE, BJAHR, ALTERSGRUPPE, GESCHLECHT_LABEL;"

  
  dbExecute(con, sql_query)
}

# ============================================
# 4. Comorbidity table (multi-code)

create_comorbidity_table = function(con, icd_list) {
  cat("Creating comorbidity table (per ICD)...\n")
  
  total_patients = dbGetQuery(con, "SELECT COUNT(DISTINCT PSID) AS TOTAL FROM TT_BASE_ICD")$TOTAL
  min_count = ceiling(total_patients * 0.10)
  not_in_clause = paste(sprintf("'%s'", icd_list), collapse = ", ")
  
  cat("Total patients:", total_patients, "| Minimum count (10%):", min_count, "\n")
  
  # Step 1: Create intermediate table linking each patient to their base ICDs
  dbExecute(con, "
    CREATE LOCAL TEMP TABLE TT_PATIENT_BASE AS
    SELECT DISTINCT PSID, ICDAMB_CODE AS ICDAMB_CODE_BASE, BJAHR
    FROM TT_BASE_ICD;
  ")
  
  # Step 2: Count co-occurring ICD codes per base ICD per year
  sql_query = sprintf("
    CREATE LOCAL TEMP TABLE TT_COMORBIDITY_RAW AS
    SELECT 
      b.ICDAMB_CODE_BASE,
      a.BJAHR,
      a.ICDAMB_CODE AS ICDAMB_CODE_COMORBID,
      COUNT(DISTINCT a.PSID) AS CNT_D_PSID
    FROM AMBDIAG a
    INNER JOIN TT_PATIENT_BASE b ON a.PSID = b.PSID AND a.BJAHR = b.BJAHR
    WHERE a.ICDAMB_CODE NOT IN (%s)
      AND a.ICDAMB_CODE != 'UUU'
    GROUP BY b.ICDAMB_CODE_BASE, a.BJAHR, a.ICDAMB_CODE
    HAVING COUNT(DISTINCT a.PSID) >= %d
  ", not_in_clause, min_count)
  
  dbExecute(con, sql_query)
  
  # Step 3: Keep top 20 per base ICD and year
  dbExecute(con, "
    CREATE TABLE RT_COMORBIDITIES AS
    SELECT *
    FROM (
      SELECT 
        ICDAMB_CODE_BASE,
        BJAHR,
        ICDAMB_CODE_COMORBID,
        CNT_D_PSID,
        DENSE_RANK() OVER (
          PARTITION BY ICDAMB_CODE_BASE, BJAHR 
          ORDER BY CNT_D_PSID DESC
        ) AS RN
        FROM TT_COMORBIDITY_RAW
        ) sub
        WHERE RN <= 20;
")
}


# ============================================
# 5. Metadata tables

create_metadata_tables = function(con, icd_list) {
  cat("Creating metadata tables...\n")
  
  icd_text = paste(icd_list, collapse = ", ")
  
  # =============================
  # 1. DD_RESULT_TABLES
 
  sql_tables = sprintf("
    CREATE TABLE DD_RESULT_TABLES AS
    SELECT 'RT_AGE_SEX_DIST' AS TABELLENNAME, 
           'Versichertenbezug' AS KLASSIFIKATION_VERSICHERTENBEZUG_LEISTUNGSTRAEGERBEZUG,
           'Häufigkeitstabelle' AS KLASSIFIKATION_STATISTISCH,
           'Alters- und Geschlechtsverteilung für ICD-10: %s' AS BESCHREIBUNG,
           'Versicherte mit ambulanter Diagnose(n): %s' AS EINSCHLUSS_FILTERKRITERIEN
    UNION ALL
    SELECT 'RT_COMORBIDITIES',
           'Versichertenbezug',
           'Häufigkeitstabelle',
           'Top 20 Komorbiditäten (≥10%% Prävalenz) für ICD-10: %s',
           'Versicherte mit ambulanter Diagnose(n): %s'
  ", icd_text, icd_text, icd_text, icd_text)
  
  dbExecute(con, sql_tables)
  
  # =============================
  # 2. DD_RESULT_COLS

  sql_cols = "
    CREATE TABLE DD_RESULT_COLS AS
    ------ RT_AGE_SEX_DIST ----
    SELECT 'RT_AGE_SEX_DIST' AS TABELLENNAME,
           'Häufigkeitstabelle' AS KLASSIFIKATION_STATISTISCH,
           'BJAHR' AS SPALTENNAME,
           'Berichtsjahr aus AMBDIAG' AS BESCHREIBUNG
    UNION ALL
    SELECT 'RT_AGE_SEX_DIST', 'Häufigkeitstabelle', 'ICDAMB_CODE', 'ICD-10 Code der seltenen Erkrankung' 
    UNION ALL
    SELECT 'RT_AGE_SEX_DIST', 'Häufigkeitstabelle', 'ALTERSGRUPPE', 'Altersgruppe der betrachteten Versicherten'
    UNION ALL
    SELECT 'RT_AGE_SEX_DIST', 'Häufigkeitstabelle', 'GESCHLECHT_LABEL', 'Geschlecht der betrachteten Versicherten'
    UNION ALL
    SELECT 'RT_AGE_SEX_DIST', 'Fallzahl', 'CNT_D_PSID', 'Anzahl distinkt gezählter Versicherten'
    
    ------ RT_COMORBIDITIES ----
    UNION ALL
    SELECT 'RT_COMORBIDITIES', 'Häufigkeitstabelle', 'BJAHR', 'Behandlungsjahr aus AMBDIAG'
    UNION ALL
    SELECT 'RT_COMORBIDITIES', 'Häufigkeitstabelle', 'ICDAMB_CODE_BASE', 'ICD-10 Code der seltenen Erkrankung'
    UNION ALL
    SELECT 'RT_COMORBIDITIES', 'Häufigkeitstabelle', 'ICDAMB_CODE_COMORBID', 'Komorbide ICD-10 Diagnose'
    UNION ALL
    SELECT 'RT_COMORBIDITIES', 'Fallzahl', 'CNT_D_PSID', 'Anzahl distinkt gezählter Versicherten mit dieser Komorbidität'
  "
  
  dbExecute(con, sql_cols)
}

# ============================================
# 3. MAIN WORKFLOW

run_local_analysis = function() {
  icd_list = c("C22.2", "C72.8", "C81.2", "C81.7")
  ICD_CODE = icd_list[1]
  EXACT_MATCH = TRUE
  CSV_DIR = "."
  
  cat("===========================================\n")
  cat("Starting LOCAL FDZ Analysis\n")
  cat("ICD Codes:", paste(icd_list, collapse = ", "), "\n")
  cat("Match Type:", ifelse(EXACT_MATCH, "Exact", "Prefix"), "\n")
  cat("===========================================\n\n")
  
  con = setup_local_database(CSV_DIR)
  
  tryCatch({
    create_base_population_multi(con, icd_list, exact_match = EXACT_MATCH)
    create_demographics_table(con)
    create_age_sex_distribution(con)
    create_comorbidity_table(con, icd_list)
    create_metadata_tables(con, icd_list)
    
    cat("\n===========================================\n")
    cat("Result Tables Created:\n")
    cat("- RT_AGE_SEX_DIST\n")
    cat("- RT_COMORBIDITIES\n")
    cat("- DD_RESULT_TABLES\n")
    cat("- DD_RESULT_COLS\n")
    
    if (interactive()) {
      print(dbGetQuery(con, "SELECT * FROM RT_AGE_SEX_DIST LIMIT 10"))
      print(dbGetQuery(con, "SELECT * FROM RT_COMORBIDITIES LIMIT 10"))
    }
    
    cat("Analysis completed successfully!\n")
    cat("===========================================\n")
    
  }, error = function(e) {
    cat("ERROR:", conditionMessage(e), "\n")
  }, finally = {
    dbDisconnect(con, shutdown = TRUE)
  })
}


# ============================================
# 4. EXECUTE

if (!interactive()) {
  start_time = Sys.time()
  run_local_analysis()
  cat("\nScript execution time:",
      difftime(Sys.time(), start_time, units = "secs"), "seconds\n")
}

# ============================================
# Test and View
# source("~/Downloads/fdz_main_r_script.r")
# ICD_CODE = "E84"
# icd_list = c("C222", "C728", "C812", "C817")
# EXACT_MATCH = TRUE
# CURRENT_YEAR = 2024
# CSV_DIR = "."
# 
# # Setup and keep connection
# con = setup_local_database(CSV_DIR)
# # Run all the creation functions
# create_base_population_multi(con, icd_list, exact_match = TRUE)
# create_demographics_table(con, CURRENT_YEAR) # forbidden to print out in FDZ
# create_age_sex_distribution(con)
# create_comorbidity_table(con, icd_list)
# create_metadata_tables(con, icd_list)
# 
# # Query the tables:
# age_sex=dbGetQuery(con, "SELECT * FROM RT_AGE_SEX_DIST")
# comorbidities=dbGetQuery(con, "SELECT * FROM RT_COMORBIDITIES")
# dd_res_tab=dbGetQuery(con, "SELECT * FROM DD_RESULT_TABLES")
# dd_res_col=dbGetQuery(con, "SELECT * FROM DD_RESULT_COLS")
