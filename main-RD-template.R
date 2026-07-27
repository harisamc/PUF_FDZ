# =============================================================================
# INSTITUTION: XXXX
# PROJECT TITLE: XXXXX
# PROJECT ABBREV.: XXXX
# FILE NUMBER (AKTENZEICHEN): XXXXXX-XX
# =============================================================================
#
# ZUSAMMENFASSUNG
#   XXXXXXXXXXXXX
#
# =============================================================================


# =============================================================================
# KONFIGURATION
# =============================================================================
SCHEMA   <- "XXXXXX_XX"   
CODE  <- "XXX"                     
EXACT <- TRUE
REQUIRE_G <- TRUE

# Geschlechtskodierung laut Datensatzbeschreibung:
#   1 = weiblich, 2 = maennlich, 3 = fehlende Angabe (§30 RSAV), 4 = divers (§22 PStG)
# Code 3 und alle uebrigen Werte -> 'Unbestimmt' (Person bleibt in der Kohorte).
FEMALE_CODE <- 1L
MALE_CODE   <- 2L
DIVERS_CODE <- 4L



# Mindestfallzahl je Ergebniszelle. Zeilen mit 0 < Fallzahl < MIN_CELL werden
# aus TABELLE_ALTER_GESCHLECHT und TABELLE_PLZ vor dem Export entfernt (FDZ-Sekundaergeheimnis).
MIN_CELL <- 30L

# Fuer die Komorbiditaetstabellen: hat eine ICD_GROUP irgendeine Zelle
# < MIN_CELL_COMO, werden Male+Female zusammengefasst (nur noch Altersgruppen).
MIN_CELL_COMO <- 30L

# Komorbiditaets-/Zielcodes. Codes in EXACT_CODES werden exakt gematcht,
# alle uebrigen als 3-Steller-Praefix (LIKE 'XXX%').
ICD_LIST    <- c("XXX","XXX")
EXACT_CODES <- c("XXX","XXX")

# Zusammenfassung mehrerer Diagnose(-gruppen) zu einer benannten Gruppe.
ICD_GROUPS <- list(
  "XXX"       = c("XXX","XXX","XXX"),
  "XXX"           = c("XXX","XXX")
)
ICD_GROUP_MAP <- do.call(rbind, lapply(names(ICD_GROUPS), function(g)
  data.frame(SRC = ICD_GROUPS[[g]],
             GRP = sprintf("%s (%s)", g, paste(ICD_GROUPS[[g]], collapse = ",")),
             stringsAsFactors = FALSE)))


# =============================================================================
# SQL-BAUSTEINE  (eine einzige Quelle der Wahrheit fuer Alter & Geschlecht)
# =============================================================================

# Altersband aus einem Alters-Ausdruck. NULL/negativ -> 'Unknown' (nie loeschen)
age_case <- function(expr) sprintf(
"CASE
   WHEN (%1$s) IS NULL          THEN 'Unknown'
   WHEN (%1$s) < 0              THEN 'Unknown'
   WHEN (%1$s) BETWEEN 0 AND 18 THEN '0-18'
   WHEN (%1$s) BETWEEN 19 AND 49 THEN '19-49'
   ELSE '50+'
 END", expr)

# Geschlecht je PSID per Mehrheitsentscheid ueber alle VERSQ-Zeilen (weiblich/
# maennlich/divers). join_clause bindet VERSQ auf die jeweilige Kohorte ein.
# Kein Verlust: ohne eindeutige Mehrheit oder ohne Treffer -> 'Unbestimmt'.
sex_cte <- function(join_clause) sprintf(
"sex AS (
   SELECT PSID,
     CASE WHEN f_n >= m_n AND f_n >= d_n AND f_n > 0 THEN 'Female'
          WHEN m_n >= f_n AND m_n >= d_n AND m_n > 0 THEN 'Male'
          WHEN d_n >  f_n AND d_n >  m_n              THEN 'Divers'
          ELSE 'Unbestimmt' END AS SEX
   FROM (
     SELECT vq.PSID,
       SUM(CASE WHEN vq.GESCHLECHT = %d THEN 1 ELSE 0 END) AS f_n,
       SUM(CASE WHEN vq.GESCHLECHT = %d THEN 1 ELSE 0 END) AS m_n,
       SUM(CASE WHEN vq.GESCHLECHT = %d THEN 1 ELSE 0 END) AS d_n
     FROM DATRAV_VERSQ vq
     %s
     GROUP BY vq.PSID
   )
 )", FEMALE_CODE, MALE_CODE, DIVERS_CODE, join_clause)

# Komorbiditaets-WHERE-Klauseln fuer AMBDIAG/KHDIAG aus ICD_LIST bauen.
build_icd_clauses <- function(icd_list, exact_codes) {
  prefix_codes <- setdiff(icd_list, exact_codes)
  list(
    exact_amb  = paste0("a.ICDAMB_CODE IN ('", paste(exact_codes, collapse = "','"), "')"),
    exact_kh   = paste0("a.ICDKH_CODE  IN ('", paste(exact_codes, collapse = "','"), "')"),
    prefix_amb = paste0("a.ICDAMB_CODE LIKE '", prefix_codes, "%'", collapse = " OR "),
    prefix_kh  = paste0("a.ICDKH_CODE  LIKE '", prefix_codes, "%'", collapse = " OR ")
  )
}


# =============================================================================
# FUNKTION 1 - OI-Basiskohorte (Personenebene)
# =============================================================================
# Ein einziger Scan von AMBDIAG + KHDIAG. Ergebnis: je PSID mit OI-Diagnose
# das frueheste Diagnosejahr. KHDIAG deckt Haupt- (ICDKH_CODE) und
# Sekundaerdiagnose (SEKICD_CODE) ab.
# OUTPUT: V_BASE_ICD (PSID, BJAHR)
# =============================================================================
create_base_rd_psids <- function(CONN) {
  cmp_amb <- if (OI_EXACT) sprintf("ICDAMB_CODE = '%s'", OI_CODE)
             else          sprintf("ICDAMB_CODE LIKE '%s%%'", OI_CODE)
  cmp_kh  <- if (OI_EXACT) sprintf("(ICDKH_CODE = '%s' OR SEKICD_CODE = '%s')", OI_CODE, OI_CODE)
             else          sprintf("(ICDKH_CODE LIKE '%s%%' OR SEKICD_CODE LIKE '%s%%')", OI_CODE, OI_CODE)
  g_amb   <- if (OI_REQUIRE_G) "AND DIAGSICH = 'G'" else ""

  sql <- sprintf("
    CREATE OR REPLACE VIEW V_BASE_ICD AS (
      SELECT PSID, MIN(BJAHR) AS BJAHR
      FROM (
        SELECT PSID, BJAHR FROM DATRAV_AMBDIAG WHERE %s %s
        UNION ALL
        SELECT PSID, BJAHR FROM DATRAV_KHDIAG  WHERE %s
      )
      GROUP BY PSID
    )", cmp_amb, g_amb, cmp_kh)

  dbExecute(CONN, sql)
}


# =============================================================================
# FUNKTION 2 - Demografie der OI-Kohorte (annotieren, nicht filtern)
# =============================================================================
# GEBJAHR/PLZ aus VERS, Geschlecht aus VERSQ - je PSID. Alle Joins auf PSID.
# Ausgewertet werden nur Versicherte mit Geschlecht weiblich/maennlich UND
# bekanntem Alter; Divers/Unbestimmt/unbekannt und unbekanntes/negatives Alter
# werden bewusst ausgeschlossen (WHERE s.SEX IN Male/Female AND Alter >= 0).
# Alter = Alter im ERSTEN Versicherungsjahr (MIN(BJAHR) aus VERS) - so wie in
# der Nicht-OI-Kohorte, damit beide Gruppen gleich definiert sind.
# OUTPUT: V_META_RD (PSID, BJAHR, GEBJAHR, PLZ, AGE_GROUP, GESCHLECHT_LABEL)
# =============================================================================
create_psid_meta_rd <- function(CONN) {
  sql <- sprintf("
    CREATE OR REPLACE VIEW V_META_RD AS (
    WITH
    vers AS (
      SELECT v.PSID, MIN(v.BJAHR) AS BJAHR, MIN(v.GEBJAHR) AS GEBJAHR, MAX(v.PLZ) AS PLZ
      FROM DATRAV_VERS v
      INNER JOIN V_BASE_ICD b ON v.PSID = b.PSID
      GROUP BY v.PSID
    ),
    %s
    SELECT
      b.PSID,
      ve.BJAHR,
      ve.GEBJAHR,
      ve.PLZ,
      %s AS AGE_GROUP,
      s.SEX AS GESCHLECHT_LABEL
    FROM V_BASE_ICD b
    LEFT JOIN vers ve ON b.PSID = ve.PSID
    LEFT JOIN sex  s  ON b.PSID = s.PSID
    WHERE s.SEX IN ('Male', 'Female')          -- nur weiblich/maennlich
      AND (ve.BJAHR - ve.GEBJAHR) >= 0         -- unbekanntes/negatives Alter ausgeschlossen
    )",
    sex_cte("INNER JOIN V_BASE_ICD b ON vq.PSID = b.PSID"),
    age_case("ve.BJAHR - ve.GEBJAHR"))

  dbExecute(CONN, sql)
}


# =============================================================================
# FUNKTION 3 - Nicht-OI-Kohorte + Demografie
# =============================================================================
# Alle Versicherten OHNE OI-Diagnose. Basis ist DATRAV_VERS (Grundgesamtheit),
# Diagnosetabellen werden hier nicht angefasst. Wie in der OI-Kohorte nur
# weiblich/maennlich UND bekanntes Alter (Divers/Unbestimmt + unbekanntes Alter raus).
# OUTPUTS: V_REMAIN_PSIDS (PSID),
#          V_META_REMAIN  (PSID, BJAHR, GEBJAHR, PLZ, AGE_GROUP, GESCHLECHT_LABEL)
# =============================================================================
create_remaining_psids_and_meta <- function(CONN) {

  dbExecute(CONN, "
    CREATE OR REPLACE VIEW V_REMAIN_PSIDS AS (
      SELECT DISTINCT v.PSID
      FROM DATRAV_VERS v
      WHERE NOT EXISTS (SELECT 1 FROM V_BASE_ICD b WHERE b.PSID = v.PSID)
    )")

  sql <- sprintf("
    CREATE OR REPLACE VIEW V_META_REMAIN AS (
    WITH
    vers AS (
      SELECT v.PSID, MIN(v.BJAHR) AS BJAHR, MIN(v.GEBJAHR) AS GEBJAHR, MAX(v.PLZ) AS PLZ
      FROM DATRAV_VERS v
      INNER JOIN V_REMAIN_PSIDS r ON v.PSID = r.PSID
      GROUP BY v.PSID
    ),
    %s
    SELECT
      ve.PSID,
      ve.BJAHR,
      ve.GEBJAHR,
      ve.PLZ,
      %s AS AGE_GROUP,
      s.SEX AS GESCHLECHT_LABEL
    FROM vers ve
    LEFT JOIN sex s ON ve.PSID = s.PSID
    WHERE s.SEX IN ('Male', 'Female')          -- nur weiblich/maennlich
      AND (ve.BJAHR - ve.GEBJAHR) >= 0         -- unbekanntes/negatives Alter ausgeschlossen
    )",
    sex_cte("INNER JOIN V_REMAIN_PSIDS r ON vq.PSID = r.PSID"),
    age_case("ve.BJAHR - ve.GEBJAHR"))

  dbExecute(CONN, sql)
}


# =============================================================================
# FUNKTION 4 - Alters- x Geschlechtsverteilung beider Kohorten
# =============================================================================
# COUNT(DISTINCT PSID) direkt je (Alter x Geschlecht) - keine Summierung ueber
# Jahre, daher keine Doppelzaehlung. 'Unknown'/'Unbestimmt' bleiben erhalten.
# OUTPUT: V_AGE_SEX (ICD_CODE, ALTERSGRUPPE, GESCHLECHT_LABEL, CNT_D_PSID)
# =============================================================================
create_age_sex_distribution <- function(CONN) {
  dbExecute(CONN, sprintf("
    CREATE OR REPLACE VIEW V_AGE_SEX AS (
      SELECT '%s' AS ICD_CODE, AGE_GROUP AS ALTERSGRUPPE, GESCHLECHT_LABEL,
             COUNT(DISTINCT PSID) AS CNT_D_PSID
      FROM V_META_RD
      GROUP BY AGE_GROUP, GESCHLECHT_LABEL
      UNION ALL
      SELECT '!%s' AS ICD_CODE, AGE_GROUP AS ALTERSGRUPPE, GESCHLECHT_LABEL,
             COUNT(DISTINCT PSID) AS CNT_D_PSID
      FROM V_META_REMAIN
      GROUP BY AGE_GROUP, GESCHLECHT_LABEL
    )", OI_CODE, OI_CODE))
}


# =============================================================================
# FUNKTION 5 - Komorbiditaeten je Kohorte (generisch)
# =============================================================================
# Scannt AMBDIAG/KHDIAG einmal, gefiltert auf Kohorten-PSIDs UND Ziel-ICDs.
# Alter und Geschlecht kommen aus der Meta-View (ein Wert pro Person, Alter im
# ersten Versicherungsjahr) - dadurch steht jede Person je ICD_GROUP in genau
# EINER Altersgruppe. Mehrere Quellcodes werden ueber VT_ICD_GROUP zu einer
# Gruppe zusammengefasst; das Distinct im COUNT verhindert Doppelzaehlung, wenn
# eine Person mehrere Codes derselben Gruppe hat.
#   cohort_join : "INNER JOIN <PSID-Quelle> x ON a.PSID = x.PSID"
#   meta_view   : V_META_RD oder V_META_REMAIN (liefert AGE_GROUP, Geschlecht)
#   out_view    : Zielname der Ergebnis-View
# =============================================================================
create_icd_occurrence <- function(CONN, icd_list, exact_codes,
                                   cohort_join, meta_view, out_view) {
  cl <- build_icd_clauses(icd_list, exact_codes)

  # Staging: je Person die vorkommenden ICD_GROUP-Werte (3-Steller bzw. Code).
  stage_view <- paste0("V_STAGE_", out_view)
  dbExecute(CONN, sprintf("
    CREATE OR REPLACE VIEW %s AS (
      SELECT DISTINCT a.PSID, a.ICDAMB_CODE AS ICD_GROUP
      FROM DATRAV_AMBDIAG a %s
      WHERE (%s) AND a.DIAGSICH = 'G'
      UNION ALL
      SELECT DISTINCT a.PSID, a.ICDKH_CODE AS ICD_GROUP
      FROM DATRAV_KHDIAG a %s
      WHERE (%s)
      UNION ALL
      SELECT DISTINCT a.PSID, SUBSTRING(a.ICDAMB_CODE, 1, 3) AS ICD_GROUP
      FROM DATRAV_AMBDIAG a %s
      WHERE (%s) AND a.DIAGSICH = 'G'
      UNION ALL
      SELECT DISTINCT a.PSID, SUBSTRING(a.ICDKH_CODE, 1, 3) AS ICD_GROUP
      FROM DATRAV_KHDIAG a %s
      WHERE (%s)
    )",
    stage_view,
    cohort_join, cl$exact_amb,
    cohort_join, cl$exact_kh,
    cohort_join, cl$prefix_amb,
    cohort_join, cl$prefix_kh))

  # Aggregation: ICD_GROUP ggf. auf gemeinsame Gruppe abbilden (LEFT JOIN auf
  # VT_ICD_GROUP; ohne Treffer bleibt der Originalwert). Alter/Geschlecht aus
  # Meta -> jede Person je Gruppe genau einmal.
  dbExecute(CONN, sprintf("
    CREATE OR REPLACE VIEW %s AS (
      SELECT
        COALESCE(g.GRP, i.ICD_GROUP) AS ICD_GROUP,
        m.GESCHLECHT_LABEL,
        m.AGE_GROUP,
        COUNT(DISTINCT i.PSID) AS CNT_D_PSID
      FROM %s i
      INNER JOIN %s m ON i.PSID = m.PSID
      LEFT JOIN VT_ICD_GROUP g ON i.ICD_GROUP = g.SRC
      GROUP BY COALESCE(g.GRP, i.ICD_GROUP), m.GESCHLECHT_LABEL, m.AGE_GROUP
    )",
    out_view, stage_view, meta_view))
}


# =============================================================================
# FUNKTION 6 - PLZ- x Geschlechtsverteilung beider Kohorten
# =============================================================================
# Nur aus den Meta-Views (PLZ dort schon aufgeloest). Erste PLZ-Stelle als
# Region, getrennt nach Female/Male. KEINE Total-Randsumme (FDZ verbietet
# Randsummen in Ergebnismengen).
# OUTPUT: V_PLZ_SEX (PLZ, CNT_D_PSID_OI_F, CNT_D_PSID_OI_M,
#                    CNT_D_PSID_REMAIN_F, CNT_D_PSID_REMAIN_M)
# =============================================================================
create_plz_distribution <- function(CONN) {
  dbExecute(CONN, "
    CREATE OR REPLACE VIEW V_PLZ_SEX AS (
    WITH rd_base AS (
      SELECT SUBSTRING(PLZ, 1, 1) AS PLZ1,
        COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Female' THEN PSID END) AS OI_F,
        COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Male'   THEN PSID END) AS OI_M
      FROM V_META_RD WHERE PLZ IS NOT NULL AND PLZ != ''
      GROUP BY SUBSTRING(PLZ, 1, 1)
    ),
    remain_base AS (
      SELECT SUBSTRING(PLZ, 1, 1) AS PLZ1,
        COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Female' THEN PSID END) AS RE_F,
        COUNT(DISTINCT CASE WHEN GESCHLECHT_LABEL = 'Male'   THEN PSID END) AS RE_M
      FROM V_META_REMAIN WHERE PLZ IS NOT NULL AND PLZ != ''
      GROUP BY SUBSTRING(PLZ, 1, 1)
    )
    SELECT
      COALESCE(rd.PLZ1, rm.PLZ1) AS PLZ,
      COALESCE(rd.OI_F, 0) AS CNT_D_PSID_OI_F,
      COALESCE(rd.OI_M, 0) AS CNT_D_PSID_OI_M,
      COALESCE(rm.RE_F, 0) AS CNT_D_PSID_REMAIN_F,
      COALESCE(rm.RE_M, 0) AS CNT_D_PSID_REMAIN_M
    FROM rd_base rd
    FULL OUTER JOIN remain_base rm ON rd.PLZ1 = rm.PLZ1
    ORDER BY PLZ
    )")
}




# =============================================================================
# PIPELINE
# =============================================================================
dbExecute(CONN, paste("SET SCHEMA", SCHEMA))

# Gruppierungs-Lookup materialisieren (vor den Komorbiditaets-Views, die
# VT_ICD_GROUP referenzieren). Bei leerer Zuordnung ein neutraler Platzhalter,
# der nie matcht - Tabelle bleibt typstabil, Codes bleiben ungruppiert.
vt_map <- if (nrow(ICD_GROUP_MAP) > 0) ICD_GROUP_MAP else
  data.frame(SRC = "__NONE__", GRP = "__NONE__", stringsAsFactors = FALSE)
dbWriteTable(CONN, "VT_ICD_GROUP", vt_map, overwrite = TRUE)

create_base_rd_psids(CONN)
create_psid_meta_rd(CONN)
create_remaining_psids_and_meta(CONN)
create_age_sex_distribution(CONN)
create_icd_occurrence(CONN, ICD_LIST, EXACT_CODES,
                      "INNER JOIN V_BASE_ICD  b ON a.PSID = b.PSID",
                      "V_META_RD",     "V_COMO_OI_AGE_SEX")
create_icd_occurrence(CONN, ICD_LIST, EXACT_CODES,
                      "INNER JOIN V_REMAIN_PSIDS r ON a.PSID = r.PSID",
                      "V_META_REMAIN", "V_COMO_RE_AGE_SEX")
create_plz_distribution(CONN)


# =============================================================================
# ERGEBNISTABELLEN MATERIALISIEREN  (Spalten explizit - kein SELECT *)
# =============================================================================
TABELLE_ALTER_GESCHLECHT <- dbGetQuery(CONN,
  "SELECT ICD_CODE, ALTERSGRUPPE, GESCHLECHT_LABEL, CNT_D_PSID FROM V_AGE_SEX")
TABELLE_KOMORB_RD <- dbGetQuery(CONN,
  "SELECT ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP, CNT_D_PSID FROM V_COMO_OI_AGE_SEX")
TABELLE_KOMORB_RE <- dbGetQuery(CONN,
  "SELECT ICD_GROUP, GESCHLECHT_LABEL, AGE_GROUP, CNT_D_PSID FROM V_COMO_RE_AGE_SEX")
TABELLE_PLZ <- dbGetQuery(CONN,
  "SELECT PLZ, CNT_D_PSID_OI_F, CNT_D_PSID_OI_M,
          CNT_D_PSID_REMAIN_F, CNT_D_PSID_REMAIN_M FROM V_PLZ_SEX")


# =============================================================================
# KLEINZELLEN-BEHANDLUNG  (FDZ-Sekundaergeheimnis)
# =============================================================================

# Kleine Fallzahlen 1..(thr-1) werden NICHT entfernt, sondern durch das
# Bereichslabel "1-(thr-1)" ersetzt. 0 und >= thr bleiben unveraendert. Die
# Zaehlspalte wird dadurch zu Text (gemischt Zahl / Bereich).
mask_small <- function(x, thr)
  ifelse(x > 0 & x < thr, sprintf("1-%d", thr - 1L), as.character(x))

# --- TABELLE_ALTER_GESCHLECHT: kleine Fallzahlen maskieren (Zeile bleibt) -------------------
TABELLE_ALTER_GESCHLECHT$CNT_D_PSID <- mask_small(TABELLE_ALTER_GESCHLECHT$CNT_D_PSID, MIN_CELL)

# --- TABELLE_PLZ: kleine Fallzahlen je Zaehlspalte maskieren (Zeile bleibt) ----
# Keine TOTAL-Spalte (Randsumme entfernt) -> kein Rueckrechnen aus den 4 Zellen.
plz_cnt_cols <- c("CNT_D_PSID_OI_F", "CNT_D_PSID_OI_M",
                  "CNT_D_PSID_REMAIN_F", "CNT_D_PSID_REMAIN_M")
for (col in plz_cnt_cols) TABELLE_PLZ[[col]] <- mask_small(TABELLE_PLZ[[col]], MIN_CELL)

# --- RT_COMO_*: kaskadierte Kleinzellenbehandlung je ICD_GROUP ----------------
# Da das Alter jetzt EIN Wert pro Person ist (erstes Versicherungsjahr), ist
# jede Person je Gruppe in genau EINER Zelle -> Summen sind exakte Distinct-Zahlen.
# Divers/Unbestimmt/unbekannt sind schon in den Meta-Views ausgeschlossen.
# Kaskade je Gruppe:
#   1) SELEKTIV je Altersgruppe: sind beide Geschlechter >= MIN_CELL_COMO, bleibt
#      die Altersgruppe nach Geschlecht getrennt; sonst werden NUR in dieser
#      Altersgruppe die Geschlechter zu 'Female+Male' addiert. Das Ergebnis kann
#      gemischt sein (manche Altersgruppen getrennt, manche zusammengefasst).
#   2) Bleibt eine Altersgruppe auch zusammengefasst < MIN_CELL_COMO, werden ALLE
#      Altersgruppen (und Geschlechter) zu einer Zeile (AGE_ALL) zusammengelegt.
#      Ist selbst das < MIN_CELL_COMO, wird die Zeile als "1-(thr-1)" maskiert
#      (nicht entfernt).
SEX_MERGE_LABEL <- "Female+Male"
AGE_ALL         <- "0-18,19-49,50+"

collapse_como <- function(df) {
  out <- df[0, ]
  for (code in unique(df$ICD_GROUP)) {
    cells <- df[df$ICD_GROUP == code, ]

    # 1) selektiv je Altersgruppe: getrennt lassen oder nur hier zusammenfassen
    rows <- df[0, ]
    ok   <- TRUE
    for (ag in unique(cells$AGE_GROUP)) {
      ca <- cells[cells$AGE_GROUP == ag, ]
      if (all(ca$CNT_D_PSID >= MIN_CELL_COMO)) {
        rows <- rbind(rows, ca)                       # Geschlechter getrennt lassen
      } else {
        n <- sum(ca$CNT_D_PSID)                        # nur diese Altersgruppe mergen
        if (n >= MIN_CELL_COMO) {
          rows <- rbind(rows, data.frame(
            ICD_GROUP = code, GESCHLECHT_LABEL = SEX_MERGE_LABEL,
            AGE_GROUP = ag, CNT_D_PSID = n, stringsAsFactors = FALSE))
        } else { ok <- FALSE; break }                  # auch gemergt zu klein
      }
    }
    if (ok) { out <- rbind(out, rows); next }

    # 2) Restfall: alle Altersgruppen (und Geschlechter) zu einer Zeile. Wird
    #    unten ggf. als "1-(thr-1)" maskiert, wenn total < MIN_CELL_COMO.
    out <- rbind(out, data.frame(
      ICD_GROUP = code, GESCHLECHT_LABEL = SEX_MERGE_LABEL,
      AGE_GROUP = AGE_ALL, CNT_D_PSID = sum(cells$CNT_D_PSID), stringsAsFactors = FALSE))
  }
  rownames(out) <- NULL
  out
}
TABELLE_KOMORB_RD <- collapse_como(TABELLE_KOMORB_RD)
TABELLE_KOMORB_RE <- collapse_como(TABELLE_KOMORB_RE)
TABELLE_KOMORB_RD$CNT_D_PSID <- mask_small(TABELLE_KOMORB_RD$CNT_D_PSID, MIN_CELL_COMO)
TABELLE_KOMORB_RE$CNT_D_PSID <- mask_small(TABELLE_KOMORB_RE$CNT_D_PSID, MIN_CELL_COMO)

cat(sprintf("Kleinzellen als '1-%d' maskiert (statt entfernt): TABELLE_ALTER_GESCHLECHT & TABELLE_PLZ (Schwelle %d), RT_COMO_* nach Kaskade (Schwelle %d).\n",
            MIN_CELL - 1L, MIN_CELL, MIN_CELL_COMO))


# =============================================================================
# ERGEBNISBESCHREIBUNG  (DD_RESULT_TABLES / DD_RESULT_COLS - Anlage 2 §4.3.3)
# =============================================================================
DD_RESULT_TABLES <- data.frame(
  Tabellenname = c('TABELLE_ALTER_GESCHLECHT', 'TABELLE_KOMORB_RD', 'TABELLE_KOMORB_RE', 'TABELLE_PLZ'),
  Klassifikation_Versichertenbezug_Leistungstraegerbezug =
    rep('Versichertenbezug', 4),
  Klassifikation_statistisch = rep('Haeufigkeitstabelle', 4),
  Beschreibung = c(
    'Aggregierte Darstellung XXXX',
    'Aggregierte Darstellung XXXx',
    'Aggregierte Darstellung XXXx',
    'Aggregierte Darstellung XXX'
  ),
  Einschluss_Filterkriterien = c(
    'xxx',
    'xxxx',
    'xxxxx',
    'xxxx'
  ),
  stringsAsFactors = FALSE
)

DD_RESULT_COLS <- data.frame(
  Tabellenname = c(rep('TABELLE_ALTER_GESCHLECHT', 4),
                   rep('TABELLE_KOMORB_RD', 4),
                   rep('TABELLE_KOMORB_RE', 4),
                   rep('TABELLE_PLZ', 5)),
  Klassifikation_statistisch = c(
    'xx', 'xxx', 'xx', 'xx',
    'xx', 'xx', 'xx', 'xx',
    'xx', 'xx', 'xx', 'xx',),
  Spaltenname = c('xx', 'xxx', 'xx', 'xx',
    'xx', 'xx', 'xx', 'xx',
    'xx', 'xx', 'xx', 'xx',),
  Beschreibung = c('xx', 'xxx', 'xx', 'xx',
    'xx', 'xx', 'xx', 'xx',
    'xx', 'xx', 'xx', 'xx',),
  stringsAsFactors = FALSE
)


# =============================================================================
# EXPORT
# =============================================================================
dbWriteTable(CONN, "DD_RESULT_TABLES", DD_RESULT_TABLES, overwrite = TRUE)
dbWriteTable(CONN, "DD_RESULT_COLS",   DD_RESULT_COLS,   overwrite = TRUE)

dbWriteTable(CONN, "TABELLE_ALTER_GESCHLECHT",         TABELLE_ALTER_GESCHLECHT,         overwrite = TRUE)
dbWriteTable(CONN, "TABELLE_KOMORB_RD", TABELLE_KOMORB_RD, overwrite = TRUE)
dbWriteTable(CONN, "TABELLE_KOMORB_RE", TABELLE_KOMORB_RE, overwrite = TRUE)
dbWriteTable(CONN, "TABELLE_PLZ",         TABELLE_PLZ,         overwrite = TRUE)
