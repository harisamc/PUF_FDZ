###########################################################################
##       Skript zur Analyse von LABRADOR-ORPHA Tracerdiagnosen           ##
##     mit lokalen CSV-Dateien als Datenquelle (ohne FDZ-Anbindung)      ##
###########################################################################
## INPUT:                                                                ##
## - Public Use File, Datenmodell 3;                                     ##
##   source: https://zenodo.org/records/15057924                         ##
##   DM3 (AMBDIAG, KHDIAG, VERS, VERSQ)                                  ##
## - Tracerdiagnosenliste "LABRADOR-ORPHA-Tracer_2026_01_02.csv"         ##
##   (Tabulator-getrennt, mit Header und 1 Zeile Metadaten)              ##
## OUTPUT:                                                               ##
## - Für jede Tracerdiagnose:                                            ##
##   - Alters- und Geschlechtsverteilung der betroffenen PSIDs (CSV)     ##
##   - PLZ-Verteilung der betroffenen PSIDs (CSV)                        ##
###########################################################################


# Pakete installieren und laden
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
required_packages <- c("DBI", "dplyr", "duckdb")
install_and_load(required_packages)

# Hilfsfunktion: Alter und Altersgruppe berechnen (für SQL)
age_group_case <- function(year_col, current_year) {
  paste0(
    "CASE
       WHEN (", current_year, " - ", year_col, ") BETWEEN 0 AND 17 THEN '0-17'
       WHEN (", current_year, " - ", year_col, ") BETWEEN 18 AND 59 THEN '18-59'
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

# Wrapper für sichere Abfrage von SQL mit Fehlerhandling
fetch_query <- function(con, sql) {
  tryCatch({
    dbGetQuery(con, sql)
  }, error = function(e) {
    message("Fehler bei SQL-Abfrage: ", e$message)
    stop(e)
  })
}

# Lokale DB aus FDZ-Tabellen in CSV-Dateien laden
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
# ---------------------------------------------------------------
# Bemerkungen: 
# 1. Falls ein Patient in mehreren Jahren Diagnosestellungen mit einem RD-Code hat, 
#    wird nur das früheste Jahr (BJAHR) genommen.
# 2. Es werden nur gesicherte Diagnosen (DIAGSICH = 'G') aus AMBDIAG berücksichtigt.
create_base_tables <- function(con, icd_rd_code, exact_match = TRUE) {
  # Tabelle löschen, falls bereits vorhanden
  execute_query(con, "DROP TABLE IF EXISTS TT_BASE_ICD;")

  if (exact_match) {
    where_amb <- sprintf("ICDAMB_CODE = '%s'", icd_rd_code)
    where_kh  <- sprintf("ICDKH_CODE = '%s'", icd_rd_code)
  } else {
    where_amb <- sprintf("ICDAMB_CODE LIKE '%s%%'", icd_rd_code)
    where_kh  <- sprintf("ICDKH_CODE LIKE '%s%%'", icd_rd_code)
  }
  
  sql <- paste0("
    CREATE LOCAL TEMP TABLE TT_BASE_ICD AS
    SELECT PSID, ICD_CODE, MIN(BJAHR) AS BJAHR
    FROM (
      SELECT DISTINCT PSID, ICDAMB_CODE AS ICD_CODE, BJAHR
      FROM AMBDIAG 
      WHERE ", where_amb, "
        AND DIAGSICH = 'G'
    
      UNION ALL
    
      SELECT DISTINCT PSID, ICDKH_CODE AS ICD_CODE, BJAHR
      FROM KHDIAG 
      WHERE ", where_kh, "
    )
    GROUP BY PSID, ICD_CODE
  ")
  
  execute_query(con, sql)
}

# Funktion zur Erstellung der Demografietabelle für RD
create_demographics_table <- function(con, current_year = 2025) {
  # Tabelle löschen, falls bereits vorhanden
  execute_query(con, "DROP TABLE IF EXISTS TT_DEMOGRAPHICS;")

  sql <- create_demographics_query("TT_DEMOGRAPHICS", current_year)
  execute_query(con, sql)
}

# Funktion: Erstellen der Tabelle für Alters- und Geschlechtsverteilung (Default: der RD-Population)
create_age_sex_dist <- function(con) {
  # Tabelle löschen, falls bereits vorhanden
  execute_query(con, "DROP TABLE IF EXISTS RT_AGE_SEX_DIST;")
  
  sql <- paste0(
    "CREATE TABLE RT_AGE_SEX_DIST AS
     SELECT 
       ICD_CODE,
       BJAHR,
       AGE_GROUP AS ALTERSGRUPPE,
       GESCHLECHT_LABEL,
       COUNT(PSID) AS CNT_D_PSID
     FROM TT_DEMOGRAPHICS
     GROUP BY ICD_CODE, BJAHR, AGE_GROUP, GESCHLECHT_LABEL;"
  )
  execute_query(con, sql)

  # Tabelle auslesen und zurückgeben
  return(fetch_query(con, "SELECT * FROM RT_AGE_SEX_DIST"))
}

# Funktion: Erstellen der Tabelle für die 2-stellige PLZ der RD PSIDs
create_plz2_dist <- function(con) {
  # Tabelle löschen, falls bereits vorhanden
  execute_query(con, "DROP TABLE IF EXISTS RT_PLZ2_DIST;")

  execute_query(con, "
    CREATE TABLE RT_PLZ2_DIST AS
    SELECT SUBSTRING(PLZ, 1, 2) AS PLZ, COUNT(DISTINCT PSID) AS CNT_D_PSID
    FROM TT_DEMOGRAPHICS
    GROUP BY SUBSTRING(PLZ, 1, 2)
    ORDER BY CNT_D_PSID DESC;
  ")

  # Tabelle auslesen und zurückgeben
  return(fetch_query(con, "SELECT * FROM RT_PLZ2_DIST"))
}

# Funktion: Erstellen der Tabelle für die 1-stellige PLZ der RD PSIDs
create_plz1_dist <- function(con) {
  # Tabelle löschen, falls bereits vorhanden
  execute_query(con, "DROP TABLE IF EXISTS RT_PLZ1_DIST;")

  execute_query(con, "
    CREATE TABLE RT_PLZ1_DIST AS
    SELECT SUBSTRING(PLZ, 1, 1) AS PLZ, COUNT(DISTINCT PSID) AS CNT_D_PSID
    FROM TT_DEMOGRAPHICS
    GROUP BY SUBSTRING(PLZ, 1, 1)
    ORDER BY CNT_D_PSID DESC;
  ")

  # Tabelle auslesen und zurückgeben
  return(fetch_query(con, "SELECT * FROM RT_PLZ1_DIST"))
}

# Funktion: Lade Tracerdiagnosecodes aus CSV-Datei
load_tracer_codes <- function(dir, file_name) {
  tracer_path <- file.path(dir, file_name)
  if (!file.exists(tracer_path)) {
    cat("Tracerdiagnosenliste", file_name, "fehlt. Bitte Datei in Verzeichnis", dir, "kopieren.\n")
    stop("Skript wird abgebrochen, da Tracerdiagnosenliste fehlt.")
  }
  tracer_table <- read.csv(tracer_path,
                           header = TRUE,
                           skip = 1,
                           sep = ";",
                           dec = ",",
                           stringsAsFactors = FALSE)

  # Entferne den Punkt im ICD-Code falls vorhanden
  tracer_table[ , 1] <- gsub("\\.", "", tracer_table[ , 1])

  return(tracer_table[ , 1])
}

# Funktion: Hauptworkflow
run_local_analysis <- function(input_dir = ".", output_dir = ".", tracer_file_name, current_year = 2025) {
  con <- setup_local_database(input_dir)

  all_age_sex_dist <- NULL
  all_plz_dist <- NULL

  # Erstelle Ausgabeordner, falls nicht vorhanden
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Lade Tracerdiagnosen
  tracer_codes <- load_tracer_codes(input_dir, tracer_file_name)

  # Verarbeite jede Tracerdiagnose
  for (code in tracer_codes) {
    message("Verarbeite Tracerdiagnose: ", code)
    create_base_tables(con, code, exact_match = TRUE)
    create_demographics_table(con, current_year)
    age_sex_dist <- create_age_sex_dist(con)
    plz_dist <- create_plz1_dist(con)

    # Füge zu Gesamtergebnissen hinzu
    if (nrow(age_sex_dist) > 0) {
      all_age_sex_dist <- rbind(all_age_sex_dist, age_sex_dist)
    }
    if (nrow(plz_dist) > 0) {
      plz_dist$ICD_CODE <- code
      plz_dist <- plz_dist[, c("ICD_CODE", "PLZ", "CNT_D_PSID")]
      all_plz_dist <- rbind(all_plz_dist, plz_dist)
    }

    # Speichere Einzelergebnisse als CSV
    #write.csv(age_sex_dist, file = file.path(output_dir, sprintf("%s_age_sex_dist.csv", code)), row.names = FALSE)
    #write.csv(plz_dist, file = file.path(output_dir, sprintf("%s_plz_dist.csv", code)), row.names = FALSE)
  }

  # Speichere Gesamtergebnisse als CSV
  write.csv(all_age_sex_dist, file = file.path(output_dir, "ALL_age_sex_dist.csv"), row.names = FALSE)
  write.csv(all_plz_dist, file = file.path(output_dir, "ALL_plz_dist.csv"), row.names = FALSE)
  
  # Schließe die Datenbankverbindung
  dbDisconnect(con, shutdown = TRUE)

  message("Analyse abgeschlossen, alle Tracercodes verarbeitet.")
}

# Führe die Analyse durch
run_local_analysis(
  input_dir = file.path(getwd(), "input"),
  output_dir = file.path(getwd(), "output"),
  tracer_file_name = "LABRADOR-ORPHA-Tracer_2026_01_02.csv",
  current_year = 2026
)