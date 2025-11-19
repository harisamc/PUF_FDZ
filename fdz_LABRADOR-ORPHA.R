# INPUT: Public Use File, Datenmodell 3; source: https://zenodo.org/records/15057924
# DM3 (AMBDIAG, KHDIAG, VERS, VERSQ)

# Funktion zum Installieren und Laden von Paketen
install_and_load <- function(packages) {
  for (pkg in packages) {
    if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
      message(sprintf("Installiere Paket: %s", pkg))
      install.packages(pkg, dependencies = TRUE)
      suppressPackageStartupMessages(library(pkg, character.only = TRUE))
    } else {
      suppressPackageStartupMessages(library(pkg, character.only = TRUE))
    }
  }
}

# Installiere und lade Bibliotheken
required_packages <- c("DBI", "dplyr", "ggplot2", "tidyr", "duckdb")
install_and_load(required_packages)

# Hilfsfunktion: Alter und Altersgruppe berechnen (für SQL)
age_group_case <- function(year_col, current_year) {
  paste0(
    "CASE
       WHEN (", current_year, " - ", year_col, ") BETWEEN 0 AND 19 THEN '0-19'
       WHEN (", current_year, " - ", year_col, ") BETWEEN 20 AND 39 THEN '20-39'
       WHEN (", current_year, " - ", year_col, ") BETWEEN 40 AND 59 THEN '40-59'
       WHEN (", current_year, " - ", year_col, ") >= 60 THEN '60+'
       ELSE 'Unknown' END"
  )
}

# Hilfsfunktion: Geschlechtslabel (für SQL)
gender_case <- "CASE WHEN GESCHLECHT = 1 THEN 'Female' WHEN GESCHLECHT = 2 THEN 'Male' ELSE 'Unknown' END"

# Hilfsfunktion: ICD Filter Bedingung
get_icd_filter <- function(colname, codes, exact=TRUE) {
  if (exact) {
    paste0(colname, " IN ('", paste(codes, collapse = "','"), "')")
  } else {
    paste0(colname, " LIKE '", paste(codes, collapse = "%' OR ", sep=""), "%'")
  }
}

# SQL für Demografietabelle erstellen
create_demographics_query <- function(base_table, current_year) {
  paste0(
    "CREATE LOCAL TEMP TABLE ", base_table, " AS
     SELECT 
       i.PSID,
       i.ICD_CODE,
       i.BJAHR,
       v.PLZ,
       (", current_year, " - v.GEBJAHR) AS ALTER,
       ", age_group_case("v.GEBJAHR", current_year), " AS AGE_GROUP,
       ", gender_case, " AS GESCHLECHT_LABEL
     FROM TT_BASE_ICD i
     INNER JOIN (
       SELECT PSID, MIN(GEBJAHR) AS GEBJAHR, MIN(PLZ) AS PLZ
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
}

# Wrapper für sichere Ausführung von SQL mit Fehlerhandling
execute_query <- function(con, sql) {
  tryCatch({
    dbExecute(con, sql)
  }, error = function(e) {
    message("Fehler bei SQL-Ausführung: ", e$message)
    stop(e)
  })
}

# Funktion zum Einrichten der lokalen Datenbank aus CSV-Dateien
setup_local_database <- function(csv_dir = ".", overwrite = TRUE) {
  con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  
  tables <- c("AMBDIAG", "KHDIAG", "VERS", "VERSQ")
  
  for (tbl in tables) {
    csv_path <- file.path(csv_dir, paste0(tbl, ".csv"))
    message("Importiere Tabelle: ", tbl)
    data <- tryCatch({
      read.csv(csv_path, check.names = FALSE)
    }, error = function(e) {
      warning(sprintf("Fehler beim Einlesen von %s: %s", csv_path, e$message))
      NULL
    })
    if (!is.null(data)) {
      dbWriteTable(con, tbl, data, overwrite = overwrite)
    }
  }
  
  execute_query(con, "CREATE SCHEMA IF NOT EXISTS P31851_123")
  
  message("Lokale Datenbank erfolgreich eingerichtet.")
  return(con)
}

# Funktion zur Erstellung der Basistabellen
create_base_tables <- function(con, icd_rd_code, exact_match = TRUE) {
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
  
  execute_query(con, sql)
  message("Basistabellen erfolgreich eingerichtet.")
}

# Funktion zur Erstellung der Demografietabelle für RD
create_demographics_table <- function(con, current_year = 2025) {
  sql <- create_demographics_query("TT_DEMOGRAPHICS_RD_UNIQUE", current_year)
  execute_query(con, sql)
}

# Funktion zur Erstellung der Restpopulation (ohne RD Patienten)
create_base_population_remaining <- function(con) {
  message("Erstelle Restpopulation (ohne RD Patienten)...")
  execute_query(con, "
    CREATE LOCAL TEMP TABLE TT_REMAINING_POP AS
    SELECT a.PSID, a.BJAHR, a.ICDAMB_CODE AS ICD_CODE
    FROM AMBDIAG a
    LEFT JOIN TT_BASE_ICD r ON a.PSID = r.PSID
    WHERE r.PSID IS NULL
    UNION ALL
    SELECT k.PSID, k.BJAHR, k.ICDKH_CODE AS ICD_CODE
    FROM KHDIAG k
    LEFT JOIN TT_BASE_ICD r ON k.PSID = r.PSID
    WHERE r.PSID IS NULL
  ")
}

# Funktion zur Erstellung der Demografietabelle für die Restpopulation
create_demographics_table_remaining <- function(con, current_year = 2025) {
  sql <- paste0(
    "CREATE LOCAL TEMP TABLE TT_DEMOGRAPHICS_REMAIN AS
     SELECT 
       r.PSID, 
       r.ICD_CODE, 
       r.BJAHR,
       v.PLZ,
       (", current_year, " - v.GEBJAHR) AS ALTER,
       ", age_group_case("v.GEBJAHR", current_year), " AS AGE_GROUP,
       ", gender_case, " AS GESCHLECHT_LABEL
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
  execute_query(con, sql)
}

# Funktion: Erstellen der Tabelle für Alters- und Geschlechtsverteilung (Default: der RD-Population)
create_age_sex_distribution <- function(con, table_name = "TT_DEMOGRAPHICS_RD_UNIQUE", result_table = "RT_RD_AGE_SEX_DIST") {
  sql <- paste0(
    "CREATE TABLE ", result_table, " AS
     SELECT 
       ICD_CODE,
       BJAHR,
       AGE_GROUP AS ALTERSGRUPPE,
       GESCHLECHT_LABEL,
       COUNT(PSID) AS CNT_D_PSID
     FROM ", table_name, "
     GROUP BY ICD_CODE, BJAHR, AGE_GROUP, GESCHLECHT_LABEL;"
  )
  execute_query(con, sql)
}

# Funktion: Erstellen der Tabelle für Alters- und Geschlechtsverteilung der Restpopulation
create_age_sex_distribution_remaining <- function(con) {
  create_age_sex_distribution(con, table_name = "TT_DEMOGRAPHICS_REMAIN", result_table = "RT_REMAINING_POP_AGE_SEX_DIST")
}

# Funktion: Erstellen der Tabelle für die 2-stellige PLZ der RD PSIDs
count_rd_plz <- function(con) {
  execute_query(con, "
    CREATE TABLE RT_RD_PLZ_GROUPED AS
    SELECT SUBSTRING(PLZ, 1, 2) AS PLZ2, COUNT(DISTINCT PSID) AS CNT_D_PSID
    FROM TT_DEMOGRAPHICS_RD_UNIQUE
    GROUP BY SUBSTRING(PLZ, 1, 2)
    ORDER BY CNT_D_PSID DESC;
  ")
}

# Funktion: Erstellen der Alters- und Geschlechtsverteilung für spezifische ICD-Codes, die die seltene Krankheit begleiten
create_icd_occurrence_table_rd <- function(con, icd_list) {
  
  exact_codes <- c("R53", "F480", "T733")
  prefix_codes <- setdiff(icd_list, exact_codes)
  
  exact_clause_amb <- get_icd_filter("ICDAMB_CODE", exact_codes, exact = TRUE)
  exact_clause_kh  <- get_icd_filter("ICDKH_CODE", exact_codes, exact = TRUE)
  prefix_clause_amb <- paste0("(", paste(paste0("ICDAMB_CODE LIKE '", prefix_codes, "%'"), collapse = " OR "), ")")
  prefix_clause_kh  <- paste0("(", paste(paste0("ICDKH_CODE LIKE '", prefix_codes, "%'"), collapse = " OR "), ")")
  
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
      SUBSTRING(a.ICDAMB_CODE, 1, 3) AS ICD_GROUP,
      a.BJAHR
    FROM AMBDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID=b.PSID
    WHERE ", prefix_clause_amb, "

    UNION ALL

    SELECT DISTINCT
      a.PSID,
      a.ICDKH_CODE AS ICD_CODE,
      SUBSTRING(a.ICDKH_CODE, 1, 3) AS ICD_GROUP,
      a.BJAHR
    FROM KHDIAG a
    INNER JOIN TT_BASE_ICD b ON a.PSID=b.PSID
    WHERE ", prefix_clause_kh, ";
  ")
  execute_query(con, sql)

  execute_query(con, "
    CREATE LOCAL TEMP TABLE TT_SPECIFIC_ICD_AGE_SEX_DIST AS
    SELECT 
      i.PSID,
      i.ICD_GROUP,
      i.BJAHR,
      d.GESCHLECHT_LABEL,
      d.AGE_GROUP
    FROM TT_SPECIFIC_ICD i
    LEFT JOIN TT_DEMOGRAPHICS_RD_UNIQUE d ON i.PSID = d.PSID;
  ")
  
  execute_query(con, "
    CREATE TABLE RT_SPECIFIC_ICD_AGE_SEX_DIST AS
    SELECT 
      ICD_GROUP,
      BJAHR,
      GESCHLECHT_LABEL,
      AGE_GROUP,
      COUNT(DISTINCT PSID) AS CNT_D_PSID
    FROM TT_SPECIFIC_ICD_AGE_SEX_DIST
    GROUP BY ICD_GROUP, BJAHR, GESCHLECHT_LABEL, AGE_GROUP
    ORDER BY ICD_GROUP, BJAHR, GESCHLECHT_LABEL, AGE_GROUP;
  ")
}

# Funktion: Hauptworkflow
run_local_analysis <- function(csv_dir = ".", current_year = 2025) {
  con <- setup_local_database(csv_dir)
  
  icd_rd_code <- "Q780"
  icd_list <- c("S02","S12","S22","S32","S42","S52","S62","S72",
                "S82","S92","T02","T08","T10","T12","I10","I15","M54",
                "R52","M25","M796","M798","M799")
  
  create_base_tables(con, icd_rd_code, exact_match = TRUE)
  create_demographics_table(con, current_year)
  create_age_sex_distribution(con)
  create_icd_occurrence_table_rd(con, icd_list)
  create_base_population_remaining(con)
  create_demographics_table_remaining(con, current_year)
  create_age_sex_distribution_remaining(con)
  count_rd_plz(con)
  
  message("Alle Tabellen wurden erfolgreich erstellt.")
  return(con)
}

# Führe die Analyse durch
con <- run_local_analysis(".")

# Lies Ergebnisse aus und exportiere als CSV
rt_remaining_pop_age_sex_dist <- dbGetQuery(con, "SELECT * FROM RT_REMAINING_POP_AGE_SEX_DIST")
write.csv(rt_remaining_pop_age_sex_dist, "rt_remaining_pop_age_sex_dist.csv", row.names = FALSE)

rt_rd_plz_grouped <- dbGetQuery(con, "SELECT * FROM RT_RD_PLZ_GROUPED")
write.csv(rt_rd_plz_grouped, "rt_rd_plz_grouped.csv", row.names = FALSE)

rt_rd_age_sex_dist <- dbGetQuery(con, "SELECT * FROM RT_RD_AGE_SEX_DIST")
write.csv(rt_rd_age_sex_dist, "rt_rd_age_sex_dist.csv", row.names = FALSE)

rt_specific_icd_age_sex_dist <- dbGetQuery(con, "SELECT * FROM RT_SPECIFIC_ICD_AGE_SEX_DIST")
write.csv(rt_specific_icd_age_sex_dist, "rt_specific_icd_age_sex_dist.csv", row.names = FALSE)

# Schließe die Datenbankverbindung
dbDisconnect(con, shutdown = TRUE)