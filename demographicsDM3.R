#!/usr/bin/env Rscript


# Script for generating plots for the respective ICD-10 code input: 
# 1. Sex by Age 
# 2. Piechart of Sexes
# 3. Most frequent co-occurring ICD-10 Codes; ICD-10 Codes present at 10% of total patient number
# ===============================
# INPUT: Public Use File, Datenmodell 3; source: https://zenodo.org/records/15057924
# ===============================
# USAGE: Rscript demographicFDZ.R --code=<> --exact=FALSE
# ===============================
# DEBUG:
# with browser(); 
# R --args --code=<>; 
# args <- commandArgs(trailingOnly = TRUE)   # Grab "--code=I78"
# source("demographicFDZ.R", echo = TRUE)
# ===============================
# TODO; adjust plot titles when --exact=FALSE, with ".-", e.g. E84.- for searching all subcats
# TODO; add cmd for plot wish - sex, age, comorbidities?
# TODO; add multiple codes as args???
# TODO; translate script to English
# TODO; Consider that the original data contain all BJ
# ===============================

# ---- Load Packages ----
suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(tidyr)
  library(ICD10gm)
  library(optparse)
})

start_time=Sys.time()
# ---- Command Line Options ----
option_list = list(
  make_option(c("-c", "--code"), type = "character", default = NULL,
              help = "ICD-10 code prefix (e.g., 'E84' or 'I780')", metavar = "character"),
  make_option(c("-e", "--exact"), type = "logical", default = FALSE,
              help = "If TRUE, search for exact ICD code match; if FALSE, match all codes starting with prefix")
  )


opt_parser = OptionParser(option_list = option_list)
opt = parse_args(opt_parser)

if (is.null(opt$code)) {
  stop("Please provide an ICD code using --code, e.g.:
       Rscript demographicFDZ.R --code=E84", call. = FALSE)
}

code_input = toupper(opt$code)
exact_match = opt$exact
cat("Running analysis for ICD code:", code_input, "\n")
cat("Exact match:", exact_match, "\n")

# ---- Define Main Function ----
analyze_icd = function(code_prefix, exact=FALSE) {
  # Set up directories
  base_dir = getwd()
  out_dir = file.path(base_dir, paste0(code_prefix, "_results"))
  if (!dir.exists(out_dir)) dir.create(out_dir)
  
  # Load data
  ambdiag = read.csv("AMBDIAG.csv", sep = ",")
  vers = read.csv("VERS.csv", sep = ",")
  versq = read.csv("VERSQ.csv", sep = ",")
  print("Loaded: AMBDIAG, VERS, VERSQ")
  
  # Prepare demographic data
  vers = vers %>% select(PSID, VSID, GEBJAHR, PLZ)
  versq = versq %>% select(PSID, VSID, VERSQ, GESCHLECHT)
  demo = merge(vers, versq, by = "PSID") %>%
    distinct(PSID, .keep_all = TRUE)
  rm(vers, versq)
  
  # Subset for ICD code
  #TODO: add option of entire category or specific code
  #df_code = ambdiag[grepl(paste0("^", code_prefix), ambdiag$ICDAMB_CODE), ]
  if (exact) {
    # Match either the base code itself or the code + G
    df_code = ambdiag[grepl(paste0("^(", code_prefix, "|", code_prefix, "G)$"), ambdiag$ICDAMB_CODE), ]
  } else {
    # Match all codes starting with the prefix
    df_code = ambdiag[grepl(paste0("^", code_prefix), ambdiag$ICDAMB_CODE), ]
  }
  
  if (nrow(df_code) == 0) {
    stop(paste("No cases found for ICD code. Enter code without dots.", code_prefix))
  }
  
  df_code_demo = merge(df_code, demo, by = "PSID")
  
  df_code_demo$GESCHLECHT = factor(df_code_demo$GESCHLECHT,
                                    levels = c(1, 2),
                                    labels = c("Female", "Male"))
  df_code_demo$Alter = as.numeric(format(Sys.Date(), "%Y")) - df_code_demo$GEBJAHR
  cat("Saved sex and age data. Plotting ...\n")

  # ---- Plot 1: Age by Sex ----
  p1 = ggplot(df_code_demo, aes(x = Alter, fill = GESCHLECHT)) +
    geom_histogram(binwidth = 10, position = "dodge", color = "black") +
    labs(title = paste0("Distribution of Age by Sex for ", code_prefix),
         x = "Age", y = "PSID Count", fill = "Sex") +
    theme_minimal()
  ggsave(file.path(out_dir, paste0(code_prefix, "_age_sex_hist.pdf")), plot = p1, width = 7, height = 5)
  
  # ---- Plot 2: Sex Distribution ----
  sex_counts = df_code_demo %>%
    count(GESCHLECHT) %>%
    mutate(perc = n / sum(n) * 100,
           label = paste0(GESCHLECHT, "\n", n, " (", round(perc, 1), "%)"))
  p2 = ggplot(sex_counts, aes(x = "", y = n, fill = GESCHLECHT)) +
    geom_bar(stat = "identity", width = 1, color = "white") +
    coord_polar(theta = "y") +
    geom_text(aes(label = label), position = position_stack(vjust = 0.3)) +
    labs(title = paste0("Proportion of Males and Females with ", code_prefix)) +
    theme_void() +
    theme(legend.position = "none")
  ggsave(file.path(out_dir, paste0(code_prefix, "_sex_pie.pdf")), plot = p2, width = 6, height = 6)
  
  # ---- Comorbidity Analysis ----
  df_psids = ambdiag %>% filter(PSID %in% df_code_demo$PSID)
  
  df_code_counts = df_psids %>%
    group_by(PSID) %>%
    summarise(ICD_count = n_distinct(ICDAMB_CODE),
              ICD_codes = paste(unique(ICDAMB_CODE), collapse = ", ")) %>%
    ungroup()
  cat("Running comorbidity analysis ...\n")
  top_codes = df_code_counts %>%
    separate_rows(ICD_codes, sep = ",\\s*") %>%
    count(ICD_codes, sort = TRUE)
  # TODO; See the range first, then decide on the filtering? 
  min_patients = 0.1 * length(unique(df_code_demo$PSID))  # ≥10% of patients
  top_codes = top_codes[top_codes$n >= min_patients, ]
  # Remove the searched code from the table
  top_codes = top_codes[top_codes$ICD_codes != code_prefix, ]
  # Remove ICD Code UUU = Angabe einer ICD-10-GM-Schlüsselnummer nicht erforderlich
  top_codes = top_codes[top_codes$ICD_codes != "UUU", ]
  
  
  icds = icd_meta_codes %>%
    filter(year == 2019) %>%
    select(ICD_codes = icd_sub, label)

  merged = merge(top_codes, icds, by = "ICD_codes", all.x = TRUE)
  cat("Plotting ...\n")
  
  # ---- Plot 3: Top Comorbidities ----
  p3 = ggplot(merged %>% slice_max(n, n = 20),
               aes(x = reorder(label, n), y = n)) +
    geom_col(fill = "steelblue") +
    coord_flip() +
    labs(
      title = paste("Most Frequent Comorbidities for", code_prefix, "Patients"),
      x = "ICD-10 Code/Diagnosis",
      y = "Number of PSIDs"
    ) +
    theme_minimal(base_size = 12)
  
  ggsave(file.path(out_dir, paste0(code_prefix, "_top_comorbidities.pdf")), plot = p3, width = 12, height = 6)
  
  # ---- Save Summary Data ----
  write.csv(merged, file.path(out_dir, paste0(code_prefix, "_top_codes.csv")), row.names = FALSE)
  cat("Analysis complete. Results saved to:", out_dir, "\n")
}

# ---- Run Function ----
analyze_icd(code_input, exact=exact_match)
end_time = Sys.time()
cat("Execution time:", end_time-start_time, "\n")
