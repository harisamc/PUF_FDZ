#!/usr/bin/env Rscript


# Script for generating plots for the respective ICD-10 code input: 
# 1. Sex by Age 
# 2. Piechart of Sexes
# 3. Most frequent co-occurring ICD-10 Codes; ICD-10 Codes present at 10% of total patient number
# ===============================
# INPUT: Public Use File, Datenmodell 3; source: https://zenodo.org/records/15057924
# ===============================
# USAGE: Rscript demographicsDM3.R --code=<> --exact=FALSE
# ===============================
# DEBUG:
# with browser(); 
# R --args --code=<>; 
# args <- commandArgs(trailingOnly = TRUE)   # Grab "--code=I78"
# source("demographicsDM3.R", echo = TRUE)
# ===============================
# TODO; adjust plot titles when --exact=FALSE, with ".-", e.g. E84.- for searching all subcats
# TODO; add cmd for plot wish - sex, age, comorbidities?
# TODO; add multiple codes as args???
# TODO; translate script to English
# TODO; Consider that the original data contain all BJ
# ===============================

# Load required packages
suppressPackageStartupMessages({
  library(dplyr)
  library(ggplot2)
  library(tidyr)
  library(ICD10gm)
  library(optparse)
})

# Parse command line arguments
parse_cli_args <- function() {
  option_list <- list(
    make_option(c("-c", "--code"), type = "character", default = NULL, 
                help = "ICD-10 code prefix (e.g. 'E84' or 'I780')", metavar = "character"),
    make_option(c("-e", "--exact"), type = "logical", default = FALSE, 
                help = "Exact match against ICD code if TRUE; else prefix match")
  )
  parser <- OptionParser(option_list = option_list)
  opts <- parse_args(parser)
  if (is.null(opts$code) || nchar(opts$code) == 0) {
    stop("Argument --code for a valid ICD code is required. 
      Example usage: Rscript demographicsDM3.R --code=E84", call. = FALSE)
  }
  list(code = toupper(opts$code), exact = opts$exact)
}

# Create output directory if missing
ensure_output_dir <- function(prefix) {
  out_dir <- file.path(getwd(), paste0(prefix, "_results"))
  if (!dir.exists(out_dir)) {
    dir.create(out_dir)
  }
  out_dir
}

# Load required data
load_data <- function() {
  ambdiag <- read.csv("AMBDIAG.csv", stringsAsFactors = FALSE)
  vers <- read.csv("VERS.csv", stringsAsFactors = FALSE)
  versq <- read.csv("VERSQ.csv", stringsAsFactors = FALSE)
  list(ambdiag = ambdiag, vers = vers, versq = versq)
}

# Prepare demographic data by merging vers and versq
prepare_demographics <- function(vers, versq) {
  demog <- merge(
    vers %>% select(PSID, VSID, GEBJAHR, PLZ),
    versq %>% select(PSID, VSID, VERSQ, GESCHLECHT),
    by = "PSID"
  ) %>% 
    distinct(PSID, .keep_all = TRUE) %>%
    mutate(
      GESCHLECHT = factor(GESCHLECHT, levels = c(1, 2), labels = c("Female", "Male")),
      ALTER = as.numeric(format(Sys.Date(), "%Y")) - GEBJAHR
    )
  demog
}

# Filter ambdiag data for ICD-10 codes matching prefix or exact code
filter_icd_data <- function(ambdiag, code_prefix, exact = FALSE) {
  if (exact) {
    pattern <- paste0("^(", code_prefix, "|", code_prefix, "G)$")
    df_filtered <- ambdiag %>% filter(grepl(pattern, ICDAMB_CODE))
  } else {
    df_filtered <- ambdiag %>% filter(startsWith(ICDAMB_CODE, code_prefix))
  }
  if (nrow(df_filtered) == 0) {
    stop(paste0("No cases found for ICD code prefix: ", code_prefix))
  }
  df_filtered
}

# Plot age distribution by sex
plot_age_distribution <- function(df, code_prefix, out_dir) {
  p <- ggplot(df, aes(x = ALTER, fill = GESCHLECHT)) +
    geom_histogram(binwidth = 10, position = "dodge", color = "black") +
    labs(title = paste0("Age Distribution by Sex for ICD-10: ", code_prefix), 
         x = "Age", y = "Patient Count", fill = "Sex") +
    theme_minimal()
  ggsave(filename = file.path(out_dir, paste0(code_prefix, "_age_sex_hist.pdf")), plot = p, width = 7, height = 5)
}

# Plot sex distribution as a pie chart
plot_sex_piechart <- function(df, code_prefix, out_dir) {
  sex_counts <- df %>%
    count(GESCHLECHT) %>% 
    mutate(
      percent = n / sum(n) * 100,
      label = sprintf("%s\n%d (%.1f%%)", GESCHLECHT, n, percent)
    )
  p <- ggplot(sex_counts, aes(x = "", y = n, fill = GESCHLECHT)) +
    geom_col(width = 1, color = "white") +
    coord_polar(theta = "y") +
    geom_text(aes(label = label), position = position_stack(vjust = 0.5)) +
    labs(title = paste0("Sex Distribution for ICD-10: ", code_prefix)) +
    theme_void() +
    theme(legend.position = "none")
  ggsave(filename = file.path(out_dir, paste0(code_prefix, "_sex_dist_pie.pdf")), plot = p, width = 6, height = 6)
}

# Analyze and return top comorbid ICD codes (≥10% patients)
analyze_comorbidities <- function(df_main, ambdiag, code_prefix) {
  psids <- unique(df_main$PSID)
  df_sub <- ambdiag %>% filter(PSID %in% psids)
  code_counts <- df_sub %>% 
    count(ICDAMB_CODE) %>% 
    filter(n >= 0.1 * length(psids)) %>% 
    filter(ICDAMB_CODE != code_prefix, ICDAMB_CODE != "UUU") # Exclude main code and 'UUU'
  code_counts
}

# Plot top comorbidities as bar chart with ICD labels from icd_meta_codes
plot_comorbidities <- function(comorbid_df, icd_meta, code_prefix, out_dir) {
  merged <- comorbid_df %>%
    left_join(icd_meta %>% filter(year == 2019) %>% select(icd_sub, label), 
              by = c("ICDAMB_CODE" = "icd_sub")) %>%
    rename(label = label) %>%
    arrange(desc(n)) %>%
    slice_head(n = 20)
  p <- ggplot(merged, aes(x = reorder(label, n), y = n)) +
    geom_col(fill = "steelblue") +
    coord_flip() +
    labs(
      title = paste0("Top 20 Comorbidities for ICD-10: ", code_prefix),
      x = "ICD-10 Diagnosis",
      y = "Patient Count"
    ) +
    theme_minimal()
  ggsave(filename = file.path(out_dir, paste0(code_prefix, "_top_comorbidities.pdf")), plot = p, width = 12, height = 6)
}

# Main workflow
run_analysis <- function() {
  args <- parse_cli_args()
  cat("Starting analysis for ICD-10 code:", args$code, "(", ifelse(args$exact, "exact match", "prefix match"), ")...\n")
  out_dir <- ensure_output_dir(args$code)
  data <- load_data()
  cat("Data loaded successfully...\n")

  demog <- prepare_demographics(data$vers, data$versq)
  icd_filtered <- filter_icd_data(data$ambdiag, args$code, args$exact)
  df_icd_demog <- merge(icd_filtered, demog, by = "PSID")

  plot_age_distribution(df_icd_demog, args$code, out_dir)
  plot_sex_piechart(df_icd_demog, args$code, out_dir)

  comorbidities <- analyze_comorbidities(df_icd_demog, data$ambdiag, args$code)
  icd_meta <- ICD10gm::icd_meta_codes
  plot_comorbidities(comorbidities, icd_meta, args$code, out_dir)

  write.csv(comorbidities, file = file.path(out_dir, paste0(args$code, "_top_comorbidities.csv")), row.names = FALSE)
  cat("Analysis and plots completed. Outputs saved in:", out_dir, "\n")
}

# Run script timing
start_time <- Sys.time()
run_analysis()
cat("Script execution time:", Sys.time() - start_time, "\n")
