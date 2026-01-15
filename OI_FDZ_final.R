# Best version yet; 
# Load libraries


suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
})

if (!requireNamespace("duckdb", quietly = TRUE)) 
  install.packages("duckdb")
library(duckdb)

# ----------------------------
# Setup local DB from CSVs
setup_local_database = function(csv_dir = ".") {
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  dbWriteTable(con, "AMBDIAG", read.csv(file.path(csv_dir, "AMBDIAG1.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
  dbWriteTable(con, "KHDIAG", read.csv(file.path(csv_dir, "KHDIAG.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
  dbWriteTable(con, "VERS", read.csv(file.path(csv_dir, "VERS.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
  dbWriteTable(con, "VERSQ", read.csv(file.path(csv_dir, "VERSQ.csv"), stringsAsFactors = FALSE), overwrite=TRUE)
  
  dbExecute(con, "CREATE SCHEMA IF NOT EXISTS P31851_123")
  cat("Local DB setup done.\n")
  return(con)
}


# ------------------------------------------------------------------------ #
### if a patient has the same ICD_CODE (Q780) in multiple years, only the earliest year (MIN(BJAHR)) is kept

create_base_tables = function(con, icd_rd_code, exact_match = TRUE) {
  
  where_amb <- if (exact_match) {
    sprintf("WHERE ICDAMB_CODE = '%s'", icd_rd_code)
  } else {
    sprintf("WHERE ICDAMB_CODE LIKE '%s%%'", icd_rd_code)
  }
  
  where_kh <- if (exact_match) {
    sprintf("WHERE ICDKH_CODE = '%s'", icd_rd_code)
  } else {
    sprintf("WHERE ICDKH_CODE LIKE '%s%%'", icd_rd_code)
  }
  
  sql <- paste0("
    CREATE LOCAL TEMP TABLE TT_BASE_ICD AS
    SELECT 
      PSID,
      ICD_CODE,
      MIN(BJAHR) AS BJAHR
    FROM (
      SELECT 
        PSID, 
        ICDAMB_CODE AS ICD_CODE, 
        BJAHR
      FROM AMBDIAG ", where_amb, "
        AND DIAGSICH = 'G' --new

      UNION ALL

      SELECT 
        PSID, 
        ICDKH_CODE AS ICD_CODE, 
        BJAHR
      FROM KHDIAG ", where_kh, "
    )
    GROUP BY PSID, ICD_CODE
  ")
  
  dbExecute(con, sql)
}



create_demographics_table = function(con) {
  
  sql <- "
    CREATE LOCAL TEMP TABLE TT_DEMOGRAPHICS_RD_UNIQUE AS

    WITH base_vers AS (
      SELECT PSID, 
        MIN(GEBJAHR) AS GEBJAHR, -- select minimum GEBJAHR 
        MIN(PLZ) AS PLZ -- remove min?
      FROM VERS
      GROUP BY PSID
    ),

    -- Clean and classify sex 
    cleaned_sex AS (
      SELECT 
        PSID,
        CASE -- select sex entries and name VALID_SEX; take value 1 or value 2
          WHEN GESCHLECHT IN (1,2) THEN GESCHLECHT
          ELSE NULL 
        END AS VALID_SEX
      FROM VERSQ
    ),

    sex_counts AS (
      SELECT
        PSID, -- count the number of sexes specific for a psid NUM_VALID
        COUNT(DISTINCT VALID_SEX) AS NUM_VALID, -- a valid nr is 1 sex!
        MIN(VALID_SEX) AS VALID_SEX_VALUE -- take only psids for gender assignment that have
      FROM cleaned_sex -- one value, as indicated from min(VALID_SEX)
      GROUP BY PSID
    ),

    base_sex AS (
      SELECT
        PSID,
        CASE 
          WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 1 THEN 'Female' -- if psid has one 1 value, and its a 1
          WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 2 THEN 'Male' -- same
          ELSE 'Unknown' -- place remaining
        END AS GESCHLECHT_LABEL
      FROM sex_counts
    )

    SELECT
      i.PSID,
      i.ICD_CODE,
      i.BJAHR,
      v.PLZ,
      (i.BJAHR - v.GEBJAHR) AS ALTER,
      CASE 
        WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 THEN '0-18'
        WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 THEN '19-49'
        WHEN (i.BJAHR - v.GEBJAHR) >= 50 THEN '50+'
        ELSE 'Unknown'
      END AS AGE_GROUP,
      s.GESCHLECHT_LABEL
    FROM TT_BASE_ICD i
    JOIN base_vers v ON i.PSID = v.PSID
    LEFT JOIN base_sex s ON i.PSID = s.PSID
    WHERE s.GESCHLECHT_LABEL IN ('Male', 'Female')
    "
  
  dbExecute(con, sql)
}



create_base_population_remaining = function(con) {
  
  dbExecute(con, "
    CREATE LOCAL TEMP TABLE TT_REMAINING_POP AS
    SELECT 
      PSID,
      ICD_CODE,
      MIN(BJAHR) AS BJAHR
    FROM (
      SELECT 
        a.PSID,
        a.ICDAMB_CODE AS ICD_CODE,
        a.BJAHR
      FROM AMBDIAG a
      LEFT JOIN TT_BASE_ICD r ON a.PSID = r.PSID
      WHERE r.PSID IS NULL
        AND a.DIAGSICH = 'G' --new

      UNION ALL

      SELECT 
        k.PSID,
        k.ICDKH_CODE AS ICD_CODE,
        k.BJAHR
      FROM KHDIAG k
      LEFT JOIN TT_BASE_ICD r ON k.PSID = r.PSID
      WHERE r.PSID IS NULL
    )
    GROUP BY PSID, ICD_CODE
  ")
}



create_demographics_table_remaining = function(con) {
  
  sql <- "
    CREATE LOCAL TEMP TABLE TT_DEMOGRAPHICS_REMAIN AS

    WITH vers_clean AS (
      SELECT 
        PSID,
        MIN(GEBJAHR) AS GEBJAHR,
        MIN(PLZ) AS PLZ
      FROM VERS
      GROUP BY PSID
    ),

   -- clean sex codes (only keep 1 or 2, else NULL) 
    cleaned_sex AS (
      SELECT 
        PSID,
        CASE 
          WHEN GESCHLECHT IN (1,2) THEN GESCHLECHT
          ELSE NULL
        END AS valid_sex
      FROM VERSQ
    ),

    -- summarize per PSID 
    sex_counts AS (
      SELECT
        PSID,
        COUNT(DISTINCT valid_sex) AS num_valid,
        MIN(valid_sex) AS valid_sex_value
      FROM cleaned_sex
      GROUP BY PSID
    ),

    -- assign sex label 
    final_sex AS (
      SELECT
        PSID,
        CASE
          WHEN num_valid = 1 AND valid_sex_value = 1 THEN 'Female'
          WHEN num_valid = 1 AND valid_sex_value = 2 THEN 'Male'
         -- ELSE 'Unknown'
        END AS geschlecht_label
      FROM sex_counts
    )

  -- final demographics table 
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
        -- ELSE 'Unknown'
      END AS AGE_GROUP,

      s.geschlecht_label AS GESCHLECHT_LABEL

    FROM TT_REMAINING_POP b
    JOIN vers_clean v ON b.PSID = v.PSID
    LEFT JOIN final_sex s ON b.PSID = s.PSID

  ";
  
  dbExecute(con, sql)
}

create_age_sex_distribution = function(con) {
  
  sql <- "
    CREATE TABLE RT_RD_NON_RD_AGE_SEX_DIST AS

  -- RD PATIENTS aggregated over all years
    SELECT 
      ICD_CODE,
      AGE_GROUP AS ALTERSGRUPPE,
      GESCHLECHT_LABEL,
      SUM(CAST(CNT_D_PSID AS INTEGER)) AS CNT_D_PSID
    FROM (
    SELECT 
      ICD_CODE,
      BJAHR,
      AGE_GROUP,
      GESCHLECHT_LABEL,
      COUNT(DISTINCT PSID) AS CNT_D_PSID
    FROM TT_DEMOGRAPHICS_RD_UNIQUE
    WHERE AGE_GROUP IS NOT NULL
      AND GESCHLECHT_LABEL IS NOT NULL -- think about doing the filtering in the demog table
    GROUP BY ICD_CODE, BJAHR, AGE_GROUP, GESCHLECHT_LABEL
  )
  GROUP BY ICD_CODE, AGE_GROUP, GESCHLECHT_LABEL

  UNION ALL

  -- NON-RD PATIENTS aggregated over all years
  SELECT
    ICD_CODE,
    AGE_GROUP AS ALTERSGRUPPE,
    GESCHLECHT_LABEL,
    SUM(CAST(CNT_D_PSID AS INTEGER)) AS CNT_D_PSID
  FROM (
    SELECT
      '!Q780' AS ICD_CODE,
      BJAHR,
      AGE_GROUP,
      GESCHLECHT_LABEL,
      COUNT(DISTINCT PSID) AS CNT_D_PSID
    FROM TT_DEMOGRAPHICS_REMAIN
    WHERE AGE_GROUP IS NOT NULL
      AND GESCHLECHT_LABEL IS NOT NULL
    GROUP BY BJAHR, AGE_GROUP, GESCHLECHT_LABEL
  )
  GROUP BY ICD_CODE, AGE_GROUP, GESCHLECHT_LABEL;

  "
  
  dbExecute(con, sql)
}


create_icd_occurrence_table_rd = function(con, icd_list) {
  
  # -----------------------------------
  # 0. Exact vs prefix codes
  # -----------------------------------
  exact_codes <- c("R53", "F480", "T733", "O80", "O81", "O82")
  prefix_codes <- setdiff(icd_list, exact_codes)
  
  exact_clause_amb <- paste0("ICDAMB_CODE IN ('", paste(exact_codes, collapse="','"), "')")
  exact_clause_kh  <- paste0("ICDKH_CODE IN ('", paste(exact_codes, collapse="','"), "')")
  prefix_clause_amb <- paste0("ICDAMB_CODE LIKE '", prefix_codes, "%'", collapse = " OR ")
  prefix_clause_kh  <- paste0("ICDKH_CODE LIKE '", prefix_codes, "%'", collapse = " OR ")
  
  # -----------------------------------
  # 1. Build ICD occurrence table
  # -----------------------------------
  sql <- paste0("
    CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD AS
    
    -- EXACT AMB
    SELECT DISTINCT
      a.PSID,
      a.ICDAMB_CODE AS ICD_CODE,
      a.ICDAMB_CODE AS ICD_GROUP,
      a.BJAHR
    FROM AMBDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID = b.PSID
    WHERE ", exact_clause_amb, "
      AND a.DIAGSICH = 'G' --new
    
    UNION ALL
    
    -- EXACT KH
    SELECT DISTINCT
      a.PSID,
      a.ICDKH_CODE AS ICD_CODE,
      a.ICDKH_CODE AS ICD_GROUP,
      a.BJAHR
    FROM KHDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID = b.PSID
    WHERE ", exact_clause_kh, "
    
    UNION ALL
    
    -- PREFIX AMB
    SELECT DISTINCT
      a.PSID,
      a.ICDAMB_CODE AS ICD_CODE,
      SUBSTRING(a.ICDAMB_CODE,1,3) AS ICD_GROUP,
      a.BJAHR
    FROM AMBDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID = b.PSID
    WHERE ", prefix_clause_amb, "
      AND a.DIAGSICH = 'G'
    
    UNION ALL
    
    -- PREFIX KH
    SELECT DISTINCT
      a.PSID,
      a.ICDKH_CODE AS ICD_CODE,
      SUBSTRING(a.ICDKH_CODE,1,3) AS ICD_GROUP,
      a.BJAHR
    FROM KHDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID = b.PSID
    WHERE ", prefix_clause_kh, ";
  ")
  dbExecute(con, sql)
  
  # -----------------------------------
  # 2. Compute age + cleaned sex (same logic as REMAIN)
  # -----------------------------------
  dbExecute(con, "
    CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD_AGE_SEX AS
    WITH vers_clean AS (
      SELECT 
          PSID,
          MIN(GEBJAHR) AS GEBJAHR
      FROM VERS
      GROUP BY PSID
    ),
    cleaned_sex AS (
      SELECT 
          PSID,
          CASE 
              WHEN GESCHLECHT IN (1,2) THEN GESCHLECHT
              ELSE NULL
          END AS VALID_SEX
      FROM VERSQ
    ),
    sex_counts AS (
      SELECT
          PSID,
          COUNT(DISTINCT VALID_SEX) AS NUM_VALID,
          MIN(VALID_SEX) AS VALID_SEX_VALUE
      FROM cleaned_sex
      GROUP BY PSID
    ),
    base_sex AS (
      SELECT
          PSID,
          CASE 
              WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 1 THEN 'Female'
              WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 2 THEN 'Male'
          END AS GESCHLECHT_LABEL
      FROM sex_counts
    )
    SELECT
      i.PSID,
      i.ICD_GROUP,
      i.BJAHR,
      s.GESCHLECHT_LABEL,
      (i.BJAHR - v.GEBJAHR) AS ALTER,
      CASE
          WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 THEN '0-18'
          WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 THEN '19-49'
          WHEN (i.BJAHR - v.GEBJAHR) >= 50 THEN '50+'
      END AS AGE_GROUP
    FROM TT_SPECIFIC_ICD i
    JOIN vers_clean v ON i.PSID = v.PSID
    LEFT JOIN base_sex s ON i.PSID = s.PSID
    WHERE 
        s.GESCHLECHT_LABEL IS NOT NULL
        AND (i.BJAHR - v.GEBJAHR) IS NOT NULL
        AND (
              (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 OR
              (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 OR
              (i.BJAHR - v.GEBJAHR) >= 50
        );
  ")
  
  # -----------------------------------
  # 3. Final aggregated ICD x Sex x Age table
  # -----------------------------------
  dbExecute(con, "
    CREATE TABLE RT_SPECIFIC_ICD_AGE_SEX_DIST AS
    SELECT 
      ICD_GROUP,
      GESCHLECHT_LABEL,
      AGE_GROUP,
      SUM(CNT_D_PSID) AS CNT_D_PSID
    FROM (
        SELECT 
          ICD_GROUP,
          BJAHR,
          GESCHLECHT_LABEL,
          AGE_GROUP,
          COUNT(DISTINCT PSID) AS CNT_D_PSID
        FROM TT_SPECIFIC_ICD_AGE_SEX
        GROUP BY ICD_GROUP, BJAHR, GESCHLECHT_LABEL, AGE_GROUP
    )
    GROUP BY ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP
    ORDER BY ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP;
  ")
}


create_icd_occurrence_table_remaining = function(con, icd_list) {
  
  # -------------------------------
  # 0. Exact vs prefix codes
  # -------------------------------
  exact_codes <- c("R53", "F480", "T733", "O80", "O81", "O82")
  prefix_codes <- setdiff(icd_list, exact_codes)
  
  exact_clause <- paste0("ICD_CODE IN ('", paste(exact_codes, collapse="','"), "')")
  prefix_clause <- paste0(
    "ICD_CODE LIKE '",
    prefix_codes,
    "%'",
    collapse = " OR "
  )
  
  # -------------------------------
  # 1. Build ICD occurrence table
  # -------------------------------
  sql <- paste0("
    CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD_REMAIN AS
    
    -- Exact matches
    SELECT DISTINCT
      PSID,
      ICD_CODE,
      ICD_CODE AS ICD_GROUP,
      BJAHR
    FROM TT_REMAINING_POP
    WHERE ", exact_clause, "
    
    UNION ALL
    
    -- Prefix matches
    SELECT DISTINCT
      PSID,
      ICD_CODE,
      SUBSTRING(ICD_CODE, 1, 3) AS ICD_GROUP,
      BJAHR
    FROM TT_REMAINING_POP
    WHERE ", prefix_clause, "
  ")
  dbExecute(con, sql)
  
  # --------------------------------------------------------------
  # 2. Compute age per event year using VERS (GEBJAHR) + VERSQ (SEX)
  # --------------------------------------------------------------
  dbExecute(con, "
    CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD_REMAIN_AGE_SEX AS
    WITH vers_clean AS (
      SELECT 
          PSID,
          MIN(GEBJAHR) AS GEBJAHR
      FROM VERS
      GROUP BY PSID
    ),
    cleaned_sex AS (
      SELECT 
          PSID,
          CASE 
              WHEN GESCHLECHT IN (1,2) THEN GESCHLECHT
              ELSE NULL
          END AS VALID_SEX
      FROM VERSQ
    ),
    sex_counts AS (
      SELECT
          PSID,
          COUNT(DISTINCT VALID_SEX) AS NUM_VALID,
          MIN(VALID_SEX) AS VALID_SEX_VALUE
      FROM cleaned_sex
      GROUP BY PSID
    ),
    base_sex AS (
      SELECT
          PSID,
          CASE 
              WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 1 THEN 'Female'
              WHEN NUM_VALID = 1 AND VALID_SEX_VALUE = 2 THEN 'Male'
          END AS GESCHLECHT_LABEL
      FROM sex_counts
    )
    SELECT
      i.PSID,
      i.ICD_GROUP,
      i.BJAHR,
      s.GESCHLECHT_LABEL,
      (i.BJAHR - v.GEBJAHR) AS ALTER,
      CASE
          WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 THEN '0-18'
          WHEN (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 THEN '19-49'
          WHEN (i.BJAHR - v.GEBJAHR) >= 50 THEN '50+'
      END AS AGE_GROUP
    FROM TT_SPECIFIC_ICD_REMAIN i
    JOIN vers_clean v ON i.PSID = v.PSID
    LEFT JOIN base_sex s ON i.PSID = s.PSID
    WHERE s.GESCHLECHT_LABEL IS NOT NULL
      AND (i.BJAHR - v.GEBJAHR) IS NOT NULL
      AND (
            (i.BJAHR - v.GEBJAHR) BETWEEN 0 AND 18 OR
            (i.BJAHR - v.GEBJAHR) BETWEEN 19 AND 49 OR
            (i.BJAHR - v.GEBJAHR) >= 50
      );
  ")
  
  # -------------------------------
  # 3. Aggregate final table
  # -------------------------------
  dbExecute(con, "
    CREATE TABLE RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN AS

    SELECT
      ICD_GROUP,
      GESCHLECHT_LABEL,
      AGE_GROUP,
      SUM(CNT_D_PSID) AS CNT_D_PSID
    FROM (
        SELECT 
          ICD_GROUP,
          BJAHR,
          GESCHLECHT_LABEL,
          AGE_GROUP,
          COUNT(DISTINCT PSID) AS CNT_D_PSID
        FROM TT_SPECIFIC_ICD_REMAIN_AGE_SEX
        GROUP BY ICD_GROUP, BJAHR, GESCHLECHT_LABEL, AGE_GROUP
    )
    GROUP BY ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP
    ORDER BY ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP;
  ")
}



create_plz_oi_non_oi_sex_table = function(con) {
  
  dbExecute(con, "
    CREATE TABLE RT_PLZ_OI_NON_OI_SEX AS
    WITH rd_base AS (
      SELECT 
        PLZ,
        COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Female' THEN PSID END) AS CNT_D_PSID_OI_F,
        COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Male' THEN PSID END) AS CNT_D_PSID_OI_M
      FROM TT_DEMOGRAPHICS_RD_UNIQUE
      WHERE PLZ IS NOT NULL AND PLZ != ''
      GROUP BY PLZ
    ),
    remain_base AS (
      SELECT 
        PLZ,
        COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Female' THEN PSID END) AS CNT_D_PSID_REMAIN_F,
        COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Male' THEN PSID END) AS CNT_D_PSID_REMAIN_M
      FROM TT_DEMOGRAPHICS_REMAIN
      WHERE PLZ IS NOT NULL AND PLZ != ''
      GROUP BY PLZ
    )
    SELECT 
      COALESCE(rd.PLZ, rm.PLZ) AS PLZ,
      COALESCE(CNT_D_PSID_OI_F,0) + COALESCE(CNT_D_PSID_OI_M,0) +
      COALESCE(CNT_D_PSID_REMAIN_F,0) + COALESCE(CNT_D_PSID_REMAIN_M,0) AS CNT_D_PSID_TOTAL,
      COALESCE(CNT_D_PSID_OI_F,0) AS CNT_D_PSID_OI_F,
      COALESCE(CNT_D_PSID_OI_M,0) AS CNT_D_PSID_OI_M,
      COALESCE(CNT_D_PSID_REMAIN_F,0) AS CNT_D_PSID_REMAIN_F,
      COALESCE(CNT_D_PSID_REMAIN_M,0) AS CNT_D_PSID_REMAIN_M
    FROM rd_base rd
    FULL OUTER JOIN remain_base rm ON rd.PLZ = rm.PLZ
    ORDER BY CNT_D_PSID_TOTAL DESC;
  ")
}




# Main workflow
run_local_analysis = function(disconnect = FALSE) {
  con <- setup_local_database(".")
  
  icd_rd_code <- "Q780"
  icd_list <- c("S02","S12","S22","S32","S42","S52","S62","S72",
                "S82","S92","T02","T08","T10","T12","M54","I10","I15",
                "R52","M25","M796","M798","M799", "O30")
  
  create_base_tables(con, icd_rd_code, exact_match=TRUE)
  create_demographics_table(con)
  create_base_population_remaining(con)
  create_demographics_table_remaining(con)
  create_age_sex_distribution(con)
  create_icd_occurrence_table_rd(con, icd_list)
  create_icd_occurrence_table_remaining(con, icd_list)
  create_plz_oi_non_oi_sex_table(con)
  
  cat("All RT tables created successfully.\n")
  if (disconnect) {
    dbDisconnect(con, shutdown = TRUE)
    return(NULL)
  }
  return(con)
}

# Open connection
con <- run_local_analysis(disconnect=FALSE)


RT_RD_NON_RD_AGE_SEX_DIST=dbGetQuery(con, "SELECT * FROM RT_RD_NON_RD_AGE_SEX_DIST")
write.csv(RT_RD_NON_RD_AGE_SEX_DIST, "RT_RD_NON_RD_AGE_SEX_DIST.csv", row.names = FALSE)
RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN=dbGetQuery(con, "SELECT * FROM RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN")
write.csv(RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN, "RT_SPECIFIC_ICD_AGE_SEX_DIST_REMAIN.csv", row.names = FALSE)
RT_SPECIFIC_ICD_AGE_SEX_DIST=dbGetQuery(con, "SELECT * FROM RT_SPECIFIC_ICD_AGE_SEX_DIST")
write.csv(RT_SPECIFIC_ICD_AGE_SEX_DIST, "RT_SPECIFIC_ICD_AGE_SEX_DIST.csv", row.names = FALSE)
RT_PLZ_OI_NON_OI_SEX=dbGetQuery(con, "SELECT * FROM RT_PLZ_OI_NON_OI_SEX")
write.csv(RT_PLZ_OI_NON_OI_SEX, "RT_PLZ_OI_NON_OI_SEX.csv", row.names = FALSE)


# # # Find PSIDs that appear in multiple age groups
# a=dbGetQuery(con, "
#   SELECT
#     PSID,
#     STRING_AGG(DISTINCT AGE_GROUP, ', ') AS age_groups,
#     STRING_AGG(DISTINCT CAST(BJAHR AS VARCHAR) || ':' || AGE_GROUP, ', ') AS year_age_pairs,
#     COUNT(DISTINCT AGE_GROUP) AS num_age_groups
#   FROM  TT_SPECIFIC_ICD_AGE_SEX_DIST -- TT_SPECIFIC_ICD_AGE_SEX_DIST; TT_SPECIFIC_ICD_REMAIN_AGE_SEX
#   GROUP BY PSID
#   HAVING COUNT(DISTINCT AGE_GROUP) > 1
#   ORDER BY num_age_groups DESC")
# 
# 
# psid_row <- dbGetQuery(con, "
#   SELECT *
#   FROM TT_SPECIFIC_ICD_AGE_SEX_DIST -- TT_SPECIFIC_ICD_AGE_SEX_DIST, ; TT_SPECIFIC_ICD_REMAIN_AGE_SEX
#   WHERE PSID = '2d92b4cf98e7901fb9d9ac80f7eef3c75ed0a6fa79dd61a7356476dd4c9b5af3'
# ")

#goal is to pick the ones that have one unique value, either unique male or unique female. 
#but if a psid has multiple and mixed entries then palce them into the unknown
# If a PSID has only 1 distinct Geschlecht, use that

# If a PSID has mixed values (e.g., both 1 and 2, or includes 0/9/NULL), → Unknown