# =============================================================================
# INSTITUTION:   Berlin Institute of Health @ Charité (BIH)
# PROJECT TITLE: Screen-for-10_OI: Osteogenesis Imperfecta (ICD-Q78.0)
#                A study on the epidemiology of Osteogenesis Imperfecta
#                within the Innovation Fund project FAIR4Rare
#                (Funding Reference: 01VSF22026)
# PROJECT ABBREV.:  Screen for OI
# FILE NUMBER (AKTENZEICHEN):   P31851-81
# =============================================================================

# =============================================================================
#             SELECTION CRITERIA FOR GROUPS AND SUBGROUPS:
# =============================================================================
# Patients are divided into two groups based on the presence or absence of the
# ICD-10 code Q78.0 across the selected study years:
#   - Rare Disease Population:  At least one recorded Q78.0 code
#   - Remaining Population:     No recorded Q78.0 code
# =============================================================================

# =============================================================================
#                               SUMMARY:
# =============================================================================

# We identify all patients carrying at least one recorded ICD-10 Q78.0 diagnosis
# across ambulatory and inpatient records, forming the rare disease base population.
# Using this base, it then derives a demographics table capturing age, sex, and
# postal code for each patient. In parallel, the script constructs a reference
# population from all remaining patients with no Q78.0 record, and applies the
# same demographic extraction. With both populations characterized, it aggregates
# an age and sex distribution table covering both groups. The script then moves on
# to co-morbidity profiling, scanning for a predefined set of co-occurring ICD-10
# codes within the rare disease population and subsequently within the reference
# population, each stratified by age group and sex. Finally, it produces a
# geographical overview by summarizing patient counts by postal code across both
# groups.
# =============================================================================



# suppressPackageStartupMessages({
#   library(DBI)
#   library(dplyr)
# })
# 
# if (!requireNamespace("duckdb", quietly = TRUE))
#   install.packages("duckdb")
# library(duckdb)
# 
# # ----------------------------
# # Setup local DB from CSVs
# setup_local_database = function(csv_dir = ".") {
#   CONN <- dbconnect(duckdb::duckdb(), dbdir = ":memory:")
# 
#   dbWriteTable(CONN, "AMBDIAG", read.csv(file.path(csv_dir, "AMBDIAG1.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
#   dbWriteTable(CONN, "KHDIAG", read.csv(file.path(csv_dir, "KHDIAG.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
#   dbWriteTable(CONN, "VERS", read.csv(file.path(csv_dir, "VERS.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
#   dbWriteTable(CONN, "VERSQ", read.csv(file.path(csv_dir, "VERSQ.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
# 
#   dbExecute(CONN, "CREATE SCHEMA IF NOT EXISTS P31851_123")
#   cat("Local DB setup done.\n")
#   return(CONN)
# }

# =============================================================================
#                                   I
# =============================================================================
#                   CREATE RARE DISEASE BASE POPULATION
#                           Temporary Table:
#                             TT_BASE_ICD
# =============================================================================
# GOAL: search for PSIDs with the ICD10 Q78.0
# =============================================================================


#create_base_tables = function(CONN, icd_rd_code, exact_match = TRUE) {
#   
#   where_amb <- if (exact_match) {
#     sprintf("WHERE ICDAMB_CODE = '%s'", icd_rd_code)
#   } else {
#     sprintf("WHERE ICDAMB_CODE LIKE '%s%%'", icd_rd_code)
#   }
#   
#   where_kh <- if (exact_match) {
#     sprintf("WHERE ICDKH_CODE = '%s' OR SEKICD_CODE = '%s'", icd_rd_code, icd_rd_code)
#   } else {
#     sprintf("WHERE ICDKH_CODE LIKE '%s%%' OR SEKICD_CODE LIKE '%s%%'", icd_rd_code, icd_rd_code)
#   }
#   
#   sql <- paste0("
#     CREATE LOCAL TEMP TABLE TT_BASE_ICD AS
#     SELECT 
#       PSID,
#       VSID,
#       ICD_CODE,
#       MIN(BJAHR) AS BJAHR
#     FROM (
#       SELECT 
#         PSID,
#         VSID,
#         ICDAMB_CODE AS ICD_CODE, 
#         BJAHR
#       FROM P31851_81.DATRAV_AMBDIAG ", where_amb, " 
#         AND DIAGSICH = 'G'
#  
#       UNION ALL
#  
#       SELECT 
#         PSID,
#         VSID,
#         ICDKH_CODE AS ICD_CODE, 
#         BJAHR
#       FROM P31851_81.DATRAV_KHDIAG ", where_kh, "
#     )
#     GROUP BY PSID, VSID, ICD_CODE
#   ")
#   
#   dbExecute(CONN, sql)
# }

# =============================================================================
#                                   II
# =============================================================================
#               CREATE A DEMOGRAPHICS TABLE FOR THE RARE DISEASE 
#                 POPULATION (PSIDs BASED ON THE BASE TABLE)
#                           Temporary Table:
#                       TT_DEMOGRAPHICS_RD_UNIQUE
# =============================================================================
# GOAL: Identify birth years, sex, geographical distribution for the rare disease 
# population
# =============================================================================

# create_demographics_table = function(CONN) {
#   
#   sql <- "
#     CREATE LOCAL TEMP TABLE TT_DEMOGRAPHICS_RD_UNIQUE AS
#  
#     WITH base_vers AS (
#       SELECT PSID,
#         VSID,
#         MIN(GEBJAHR) AS GEBJAHR,
#         MIN(PLZ) AS PLZ
#       FROM P31851_81.DATRAV_VERS
#       GROUP BY PSID, VSID
#     ),
#  
#     cleaned_sex AS (
#       SELECT 
#         PSID,
#         VSID,
#         CASE
#           WHEN GESCHLECHT IN (1,2) THEN GESCHLECHT
#           ELSE NULL 
#         END AS VALID_SEX
#       FROM P31851_81.DATRAV_VERSQ
#     ),
#  
#     sex_counts AS (
#       SELECT
#         PSID,
#         VSID,
#         COUNT(DISTINCT VALID_SEX) AS NUM_VALID,
#         MIN(VALID_SEX) AS VALID_SEX_VALUE
#       FROM cleaned_sex
#       GROUP BY PSID, VSID
#     ),
#  
#     base_sex AS (
#       SELECT
#         PSID,
#         VSID,
#         CASE 
#           WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 1 THEN 'Female'
#           WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 2 THEN 'Male'
#           ELSE 'Unknown'
#         END AS GESCHLECHT_LABEL
#       FROM sex_counts
#     )
#  
#     SELECT
#       i.PSID,
#       i.ICD_CODE,
#       i.BJAHR,
#       v.PLZ,
#       (i.BJAHR - v.GEBJAHR) AS ALTER,
#       CASE 
#         WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 THEN '0-18'
#         WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 THEN '19-49'
#         WHEN (i.BJAHR - v.GEBJAHR) >= 50 THEN '50+'
#         ELSE 'Unknown'
#       END AS AGE_GROUP,
#       s.GESCHLECHT_LABEL
#     FROM TT_BASE_ICD i
#     JOIN base_vers v ON i.PSID = v.PSID AND i.VSID = v.VSID
#     LEFT JOIN base_sex s ON i.PSID = s.PSID AND i.VSID = s.VSID
#     WHERE s.GESCHLECHT_LABEL IN ('Male', 'Female')
#     "
#   
#   dbExecute(CONN, sql)
# }


# =============================================================================
#                                   III
# =============================================================================
#             CREATE REMAINING POPULATION BASE TABLE 
#                         Temporary Table:
#                         TT_REMAINING_POP
# =============================================================================
# GOAL: search for PSIDs without the ICD10 Q78.0
# =============================================================================

# create_base_population_remaining = function(CONN) {
#   
#   dbExecute(CONN, "
#     CREATE LOCAL TEMP TABLE TT_REMAINING_POP AS
#     SELECT 
#       PSID,
#       VSID,
#       ICD_CODE,
#       MIN(BJAHR) AS BJAHR
#     FROM (
#       SELECT 
#         a.PSID,
#         a.VSID,
#         a.ICDAMB_CODE AS ICD_CODE,
#         a.BJAHR
#       FROM P31851_81.DATRAV_AMBDIAG a
#       LEFT JOIN TT_BASE_ICD r ON a.PSID = r.PSID
#       WHERE r.PSID IS NULL
#         AND a.DIAGSICH = 'G'
#  
#       UNION ALL
#  
#       SELECT 
#         k.PSID,
#         k.VSID,
#         k.ICDKH_CODE AS ICD_CODE,
#         k.BJAHR
#       FROM P31851_81.DATRAV_KHDIAG k
#       LEFT JOIN TT_BASE_ICD r ON k.PSID = r.PSID
#       WHERE r.PSID IS NULL
#     )
#     GROUP BY PSID, VSID, ICD_CODE
#   ")
# }

# =============================================================================
#                                   IV
# =============================================================================
#               CREATE A DEMOGRAPHICS TABLE FOR THE REMAINING POPULATION  
#               POPULATION (PSIDs BASED ON THE REMAINING POP. BASE TABLE)
#                           Temporary Table:
#                         TT_DEMOGRAPHICS_REMAIN
# =============================================================================
# GOAL: Identify birth years, sex, geographical distribution for the remaining 
# population
# =============================================================================

#create_demographics_table_remaining = function(CONN) {
  
  sql <- "
    CREATE LOCAL TEMP TABLE TT_DEMOGRAPHICS_REMAIN AS
 
    WITH vers_clean AS (
      SELECT 
        PSID,
        VSID,
        MIN(GEBJAHR) AS GEBJAHR,
        MIN(PLZ) AS PLZ
      FROM P31851_81.DATRAV_VERS
      GROUP BY PSID, VSID
    ),
 
    cleaned_sex AS (
      SELECT 
        PSID,
        VSID,
        CASE 
          WHEN GESCHLECHT IN (1,2) THEN GESCHLECHT
          ELSE NULL
        END AS valid_sex
      FROM P31851_81.DATRAV_VERSQ
    ),
 
    sex_counts AS (
      SELECT
        PSID,
        VSID,
        COUNT(DISTINCT valid_sex) AS num_valid,
        MIN(valid_sex) AS valid_sex_value
      FROM cleaned_sex
      GROUP BY PSID, VSID
    ),
 
    final_sex AS (
      SELECT
        PSID,
        VSID,
        CASE
          WHEN num_valid = 1 AND valid_sex_value = 1 THEN 'Female'
          WHEN num_valid = 1 AND valid_sex_value = 2 THEN 'Male'
        END AS geschlecht_label
      FROM sex_counts
    )
 
    SELECT
      b.PSID,
      b.ICD_CODE,
      b.BJAHR,
      v.PLZ,
      (b.BJAHR - v.GEBJAHR) AS ALTER,
 
      CASE 
        WHEN (b.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 THEN '0-18'
        WHEN (b.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 THEN '19-49'
        WHEN (b.BJAHR - v.GEBJAHR) >= 50 THEN '50+'
      END AS AGE_GROUP,
 
      s.geschlecht_label AS GESCHLECHT_LABEL
 
    FROM TT_REMAINING_POP b
    JOIN vers_clean v ON b.PSID = v.PSID AND b.VSID = v.VSID
    LEFT JOIN final_sex s ON b.PSID = s.PSID AND b.VSID = s.VSID
  ";
  
  dbExecute(CONN, sql)
}

# =============================================================================
#                                   V
# =============================================================================
#                 CREATE A SEX-AGE DISTRIBUTION
#         (PSIDs BASED ON THE REMAINING POP. BASE TABLE)
#                         Result Table 1:
#                   RT_RD_NON_RD_AGE_SEX_DIST 
# =============================================================================

# GOAL: Aggregated overview of age and sex groups for the rare disease
# and remaining population
# =============================================================================

# create_age_sex_distribution = function(CONN) {
#   
#   sql <- "
#     CREATE TABLE RT_RD_NON_RD_AGE_SEX_DIST AS
#  
#     -- RD PATIENTS aggregated over all years
#     SELECT 
#       ICD_CODE,
#       AGE_GROUP AS ALTERSGRUPPE,
#       GESCHLECHT_LABEL,
#       SUM(CAST(CNT_D_PSID AS INTEGER)) AS CNT_D_PSID
#     FROM (
#       SELECT 
#         ICD_CODE,
#         BJAHR,
#         AGE_GROUP,
#         GESCHLECHT_LABEL,
#         COUNT(DISTINCT PSID) AS CNT_D_PSID
#       FROM TT_DEMOGRAPHICS_RD_UNIQUE
#       WHERE AGE_GROUP IS NOT NULL
#         AND GESCHLECHT_LABEL IS NOT NULL
#       GROUP BY ICD_CODE, BJAHR, AGE_GROUP, GESCHLECHT_LABEL
#     )
#     GROUP BY ICD_CODE, AGE_GROUP, GESCHLECHT_LABEL
#  
#     UNION ALL
#  
#     -- NON-RD PATIENTS aggregated over all years
#     SELECT
#       ICD_CODE,
#       AGE_GROUP AS ALTERSGRUPPE,
#       GESCHLECHT_LABEL,
#       SUM(CAST(CNT_D_PSID AS INTEGER)) AS CNT_D_PSID
#     FROM (
#       SELECT
#         '!Q780' AS ICD_CODE,
#         BJAHR,
#         AGE_GROUP,
#         GESCHLECHT_LABEL,
#         COUNT(DISTINCT PSID) AS CNT_D_PSID
#       FROM TT_DEMOGRAPHICS_REMAIN
#       WHERE AGE_GROUP IS NOT NULL
#         AND GESCHLECHT_LABEL IS NOT NULL
#       GROUP BY BJAHR, AGE_GROUP, GESCHLECHT_LABEL
#     )
#     GROUP BY ICD_CODE, AGE_GROUP, GESCHLECHT_LABEL;
#   "
#   
#   dbExecute(CONN, sql)
# }

# =============================================================================
#                                   VI
# =============================================================================
# =============================================================================
#                 CREATE AN OVERVIEW OF SPECIFIC 
#                 CO-OCCURRING ICD-10 CODES FOR 
#                       THE RARE DISEASE
#                       Temporary Tables:
#                       TT_SPECIFIC_ICD, 
#                       TT_SPECIFIC_ICD_AGE_SEX
#                       Result Table 2:
#                       RT_SPECIFIC_ICD_AGE_SEX_DIST  
# =============================================================================
# GOAL: Counts of specific ICD-10 codes found in the rare disease population
# =============================================================================

# create_icd_occurrence_table_rd = function(CONN, icd_list) {
#   
#   # -----------------------------------
#   # 0. Exact vs prefix codes
#   # -----------------------------------
#   exact_codes <- c("R53", "F480", "T733", "O80", "O81", "O82")
#   prefix_codes <- setdiff(icd_list, exact_codes)
#   
#   exact_clause_amb <- paste0("ICDAMB_CODE IN ('", paste(exact_codes, collapse="','"), "')")
#   exact_clause_kh  <- paste0("ICDKH_CODE IN ('", paste(exact_codes, collapse="','"), "')")
#   prefix_clause_amb <- paste0("ICDAMB_CODE LIKE '", prefix_codes, "%'", collapse = " OR ")
#   prefix_clause_kh  <- paste0("ICDKH_CODE LIKE '", prefix_codes, "%'", collapse = " OR ")
#   
#   # -----------------------------------
#   # 1. Build ICD occurrence table
#   # -----------------------------------
#   sql <- paste0("
#     CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD AS
#     
#     -- EXACT AMB
#     SELECT DISTINCT
#       a.PSID,
#       a.VSID,
#       a.ICDAMB_CODE AS ICD_CODE,
#       a.ICDAMB_CODE AS ICD_GROUP,
#       a.BJAHR
#     FROM P31851_81.DATRAV_AMBDIAG a
#     INNER JOIN TT_BASE_ICD b ON a.PSID = b.PSID AND a.VSID = b.VSID AND a.BJAHR = b.BJAHR
#     WHERE ", exact_clause_amb, "
#       AND a.DIAGSICH = 'G'
#     
#     UNION ALL
#     
#     -- EXACT KH
#     SELECT DISTINCT
#       a.PSID,
#       a.VSID,
#       a.ICDKH_CODE AS ICD_CODE,
#       a.ICDKH_CODE AS ICD_GROUP,
#       a.BJAHR
#     FROM KHDIAG a
#     INNER JOIN TT_BASE_ICD b ON a.PSID = b.PSID AND a.VSID = b.VSID AND a.BJAHR = b.BJAHR
#     WHERE ", exact_clause_kh, "
#     
#     UNION ALL
#     
#     -- PREFIX AMB
#     SELECT DISTINCT
#       a.PSID,
#       a.VSID,
#       a.ICDAMB_CODE AS ICD_CODE,
#       SUBSTRING(a.ICDAMB_CODE,1,3) AS ICD_GROUP,
#       a.BJAHR
#     FROM P31851_81.DATRAV_AMBDIAG a
#     INNER JOIN TT_BASE_ICD b ON a.PSID = b.PSID AND a.VSID = b.VSID AND a.BJAHR = b.BJAHR
#     WHERE ", prefix_clause_amb, "
#       AND a.DIAGSICH = 'G'
#     
#     UNION ALL
#     
#     -- PREFIX KH
#     SELECT DISTINCT
#       a.PSID,
#       a.VSID,
#       a.ICDKH_CODE AS ICD_CODE,
#       SUBSTRING(a.ICDKH_CODE,1,3) AS ICD_GROUP,
#       a.BJAHR
#     FROM KHDIAG a
#     INNER JOIN TT_BASE_ICD b ON a.PSID = b.PSID AND a.VSID = b.VSID AND a.BJAHR = b.BJAHR
#     WHERE ", prefix_clause_kh, ";
#   ")
#   dbExecute(CONN, sql)
#   
#   # -----------------------------------
#   # 2. Compute age + cleaned sex; VERS and VERSQ joined on PSID + VSID
#   # -----------------------------------
#   dbExecute(CONN, "
#     CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD_AGE_SEX AS
#     WITH vers_clean AS (
#       SELECT 
#         PSID,
#         VSID,
#         MIN(GEBJAHR) AS GEBJAHR
#       FROM P31851_81.DATRAV_VERS
#       GROUP BY PSID, VSID
#     ),
#     cleaned_sex AS (
#       SELECT 
#         PSID,
#         VSID,
#         CASE 
#           WHEN GESCHLECHT IN (1,2) THEN GESCHLECHT
#           ELSE NULL
#         END AS VALID_SEX
#       FROM P31851_81.DATRAV_VERSQ
#     ),
#     sex_counts AS (
#       SELECT
#         PSID,
#         VSID,
#         COUNT(DISTINCT VALID_SEX) AS NUM_VALID,
#         MIN(VALID_SEX) AS VALID_SEX_VALUE
#       FROM cleaned_sex
#       GROUP BY PSID, VSID
#     ),
#     base_sex AS (
#       SELECT
#         PSID,
#         VSID,
#         CASE 
#           WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 1 THEN 'Female'
#           WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 2 THEN 'Male'
#         END AS GESCHLECHT_LABEL
#       FROM sex_counts
#     )
#     SELECT
#       i.PSID,
#       i.ICD_GROUP,
#       i.BJAHR,
#       s.GESCHLECHT_LABEL,
#       (i.BJAHR - v.GEBJAHR) AS ALTER,
#       CASE
#         WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 THEN '0-18'
#         WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 THEN '19-49'
#         WHEN (i.BJAHR - v.GEBJAHR) >= 50 THEN '50+'
#       END AS AGE_GROUP
#     FROM TT_SPECIFIC_ICD i
#     JOIN vers_clean v ON i.PSID = v.PSID AND i.VSID = v.VSID
#     LEFT JOIN base_sex s ON i.PSID = s.PSID AND i.VSID = s.VSID
#     WHERE 
#       s.GESCHLECHT_LABEL IS NOT NULL
#       AND (i.BJAHR - v.GEBJAHR) IS NOT NULL
#       AND (
#             (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 OR
#             (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 OR
#             (i.BJAHR - v.GEBJAHR) >= 50
#       );
#   ")
#   
#   # -----------------------------------
#   # 3. Final aggregated ICD x Sex x Age table - no changes needed
#   # -----------------------------------
#   dbExecute(CONN, "
#     CREATE TABLE RT_SPECIFIC_ICD_AGE_SEX_DIST AS
#     SELECT 
#       ICD_GROUP,
#       GESCHLECHT_LABEL,
#       AGE_GROUP,
#       SUM(CNT_D_PSID) AS CNT_D_PSID
#     FROM (
#       SELECT 
#         ICD_GROUP,
#         BJAHR,
#         GESCHLECHT_LABEL,
#         AGE_GROUP,
#         COUNT(DISTINCT PSID) AS CNT_D_PSID
#       FROM TT_SPECIFIC_ICD_AGE_SEX
#       GROUP BY ICD_GROUP, BJAHR, GESCHLECHT_LABEL, AGE_GROUP
#     )
#     GROUP BY ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP
#     ORDER BY ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP;
#   ")
# }

# =============================================================================
#                                   VII
# =============================================================================
#                 CREATE AN OVERVIEW OF SPECIFIC 
#                   CO-OCCURRING ICD-10 CODES 
#                   FOR THE REMAINING POPULATION
#                       Temporary Tables:
#                       TT_SPECIFIC_ICD_REMAIN, 
#                       TT_SPECIFIC_ICD_REMAIN_AGE_SEX
#                       Result Table:
#                       RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN
# =============================================================================
# GOAL: Counts of specific ICD-10 codes found in the rare disease population
# =============================================================================

# create_icd_occurrence_table_remaining = function(CONN, icd_list) {
#   
#   # -------------------------------
#   # 0. Exact vs prefix codes
#   # -------------------------------
#   exact_codes <- c("R53", "F480", "T733", "O80", "O81", "O82")
#   prefix_codes <- setdiff(icd_list, exact_codes)
#   
#   exact_clause <- paste0("ICD_CODE IN ('", paste(exact_codes, collapse="','"), "')")
#   prefix_clause <- paste0(
#     "ICD_CODE LIKE '",
#     prefix_codes,
#     "%'",
#     collapse = " OR "
#   )
#   
#   # -------------------------------
#   # 1. Build ICD occurrence table
#   # -------------------------------
#   sql <- paste0("
#     CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD_REMAIN AS
#     
#     -- Exact matches
#     SELECT DISTINCT
#       PSID,
#       VSID,
#       ICD_CODE,
#       ICD_CODE AS ICD_GROUP,
#       BJAHR
#     FROM TT_REMAINING_POP
#     WHERE ", exact_clause, "
#     
#     UNION ALL
#     
#     -- Prefix matches
#     SELECT DISTINCT
#       PSID,
#       VSID,
#       ICD_CODE,
#       SUBSTRING(ICD_CODE, 1, 3) AS ICD_GROUP,
#       BJAHR
#     FROM TT_REMAINING_POP
#     WHERE ", prefix_clause, "
#   ")
#   dbExecute(CONN, sql)
#   
#   # --------------------------------------------------------------
#   # 2. Compute age per event year; VERS and VERSQ joined on PSID + VSID
#   # --------------------------------------------------------------
#   dbExecute(CONN, "
#     CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD_REMAIN_AGE_SEX AS
#     WITH vers_clean AS (
#       SELECT 
#         PSID,
#         VSID,
#         MIN(GEBJAHR) AS GEBJAHR
#       FROM P31851_81.DATRAV_VERS
#       GROUP BY PSID, VSID
#     ),
#     cleaned_sex AS (
#       SELECT 
#         PSID,
#         VSID,
#         CASE 
#           WHEN GESCHLECHT IN (1,2) THEN GESCHLECHT
#           ELSE NULL
#         END AS VALID_SEX
#       FROM P31851_81.DATRAV_VERSQ
#     ),
#     sex_counts AS (
#       SELECT
#         PSID,
#         VSID,
#         COUNT(DISTINCT VALID_SEX) AS NUM_VALID,
#         MIN(VALID_SEX) AS VALID_SEX_VALUE
#       FROM cleaned_sex
#       GROUP BY PSID, VSID
#     ),
#     base_sex AS (
#       SELECT
#         PSID,
#         VSID,
#         CASE 
#           WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 1 THEN 'Female'
#           WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 2 THEN 'Male'
#         END AS GESCHLECHT_LABEL
#       FROM sex_counts
#     )
#     SELECT
#       i.PSID,
#       i.ICD_GROUP,
#       i.BJAHR,
#       s.GESCHLECHT_LABEL,
#       (i.BJAHR - v.GEBJAHR) AS ALTER,
#       CASE
#         WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 THEN '0-18'
#         WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 THEN '19-49'
#         WHEN (i.BJAHR - v.GEBJAHR) >= 50 THEN '50+'
#       END AS AGE_GROUP
#     FROM TT_SPECIFIC_ICD_REMAIN i
#     JOIN vers_clean v ON i.PSID = v.PSID AND i.VSID = v.VSID
#     LEFT JOIN base_sex s ON i.PSID = s.PSID AND i.VSID = s.VSID
#     WHERE s.GESCHLECHT_LABEL IS NOT NULL
#       AND (i.BJAHR - v.GEBJAHR) IS NOT NULL
#       AND (
#             (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 OR
#             (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 OR
#             (i.BJAHR - v.GEBJAHR) >= 50
#       );
#   ")
#   
#   # -------------------------------
#   # 3. Aggregate final table - no changes needed
#   # -------------------------------
#   dbExecute(CONN, "
#     CREATE TABLE RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN AS
#     SELECT
#       ICD_GROUP,
#       GESCHLECHT_LABEL,
#       AGE_GROUP,
#       SUM(CNT_D_PSID) AS CNT_D_PSID
#     FROM (
#       SELECT 
#         ICD_GROUP,
#         BJAHR,
#         GESCHLECHT_LABEL,
#         AGE_GROUP,
#         COUNT(DISTINCT PSID) AS CNT_D_PSID
#       FROM TT_SPECIFIC_ICD_REMAIN_AGE_SEX
#       GROUP BY ICD_GROUP, BJAHR, GESCHLECHT_LABEL, AGE_GROUP
#     )
#     GROUP BY ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP
#     ORDER BY ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP;
#   ")
# }

# =============================================================================
#                                   VIII
# =============================================================================
#                 CREATE AN OVERVIEW OF GEOGRAPHICAL DISTRIBUTION
#             FOR THE RARE DISEASE POPULATION RELATIVE TO THE REMANINING 
#                           POPULATION  
#                           Result Table:
#                       RT_PLZ_OI_NON_OI_SEX
# =============================================================================
# GOAL: Count the rare disease PSIDs in the respective 1-digit PLZs,
# relative to the remaining population
# =============================================================================

# create_plz_oi_non_oi_sex_table = function(CONN) {
#   
#   dbExecute(CONN, "
#     CREATE TABLE RT_PLZ_OI_NON_OI_SEX AS
#     WITH rd_base AS (
#       SELECT 
#         PLZ,
#         COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Female' THEN PSID END) AS CNT_D_PSID_OI_F,
#         COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Male' THEN PSID END) AS CNT_D_PSID_OI_M
#       FROM TT_DEMOGRAPHICS_RD_UNIQUE
#       WHERE PLZ IS NOT NULL AND PLZ != ''
#       GROUP BY PLZ
#     ),
#     remain_base AS (
#       SELECT 
#         PLZ,
#         COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Female' THEN PSID END) AS CNT_D_PSID_REMAIN_F,
#         COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Male' THEN PSID END) AS CNT_D_PSID_REMAIN_M
#       FROM TT_DEMOGRAPHICS_REMAIN
#       WHERE PLZ IS NOT NULL AND PLZ != ''
#       GROUP BY PLZ
#     )
#     SELECT 
#       COALESCE(rd.PLZ, rm.PLZ) AS PLZ,
#       COALESCE(CNT_D_PSID_OI_F,0) + COALESCE(CNT_D_PSID_OI_M,0) +
#       COALESCE(CNT_D_PSID_REMAIN_F,0) + COALESCE(CNT_D_PSID_REMAIN_M,0) AS CNT_D_PSID_TOTAL,
#       COALESCE(CNT_D_PSID_OI_F,0) AS CNT_D_PSID_OI_F,
#       COALESCE(CNT_D_PSID_OI_M,0) AS CNT_D_PSID_OI_M,
#       COALESCE(CNT_D_PSID_REMAIN_F,0) AS CNT_D_PSID_REMAIN_F,
#       COALESCE(CNT_D_PSID_REMAIN_M,0) AS CNT_D_PSID_REMAIN_M
#     FROM rd_base rd
#     FULL OUTER JOIN remain_base rm ON rd.PLZ = rm.PLZ
#     ORDER BY CNT_D_PSID_TOTAL DESC;
#   ")
# }
# 

# =============================================================================
#                                   RUN
# =============================================================================

run_local_analysis = function(disconnect = FALSE) {
  CONN <- setup_local_database(".")
  
  icd_rd_code <- "Q780"
  icd_list <- c("S02","S12","S22","S32","S42","S52","S62","S72",
                "S82","S92","T02","T08","T10","T12","M54","I10","I15",
                "R52","M25","M796","M798","M799", "O30")
  
  create_base_tables(CONN, icd_rd_code, exact_match=TRUE)
  create_demographics_table(CONN)
  create_base_population_remaining(CONN)
  create_demographics_table_remaining(CONN)
  create_age_sex_distribution(CONN)
  create_icd_occurrence_table_rd(CONN, icd_list)
  create_icd_occurrence_table_remaining(CONN, icd_list)
  create_plz_oi_non_oi_sex_table(CONN)
  
  cat("All RT tables created successfully.\n")
  if (disConnect) {
    dbDisonnect(CONN, shutdown = TRUE)
    return(NULL)
  }
  return(CONN)
}

# Open Connection
# CONN <- run_local_analysis(disconnect=FALSE)


# RT_RD_NON_RD_AGE_SEX_DIST=dbGetQuery(CONN, "SELECT * FROM RT_RD_NON_RD_AGE_SEX_DIST")
# write.csv(RT_RD_NON_RD_AGE_SEX_DIST, "RT_RD_NON_RD_AGE_SEX_DIST.csv", row.names = FALSE)
# RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN=dbGetQuery(CONN, "SELECT * FROM RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN")
# write.csv(RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN, "RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN.csv", row.names = FALSE)
# RT_SPECIFIC_ICD_AGE_SEX_DIST=dbGetQuery(CONN, "SELECT * FROM RT_SPECIFIC_ICD_AGE_SEX_DIST")
# write.csv(RT_SPECIFIC_ICD_AGE_SEX_DIST, "RT_SPECIFIC_ICD_AGE_SEX_DIST.csv", row.names = FALSE)
# RT_PLZ_OI_NON_OI_SEX=dbGetQuery(CONN, "SELECT * FROM RT_PLZ_OI_NON_OI_SEX")
# write.csv(RT_PLZ_OI_NON_OI_SEX, "RT_PLZ_OI_NON_OI_SEX.csv", row.names = FALSE)

