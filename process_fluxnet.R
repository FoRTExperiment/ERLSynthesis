# Prep the FLUXNET data
# Ben Bond-Lamberty January 2019
#
# Extract annual GPP, Re, and GPP (along with QC, temp/precip, etc)
# from zipped FLUXNET TIER1 files.

library(ggplot2)
theme_set(theme_bw())
library(dplyr)
library(readr)
library(tidyr)

SCRIPTNAME  	<- "process_fluxnet.R"
FLUXNET_DATA <- "~/Data/FLUXNET2015/"

# ==============================================================================
# Main 

cat("Welcome to", SCRIPTNAME, "\n")

# Extract the *_SUBSET_YY_* files (for GPP) from the FLUXNET zip files and save
# FLUXNET Tier 1 data downloaded 30 Jan 2017 from
# http://fluxnet.fluxdata.org (ftp.fluxdata.org/.fluxnet_downloads_86523/)
# (No update since then.)
td <- tempdir()
files <- list.files(FLUXNET_DATA, pattern = "zip$", full.names = TRUE)
stopifnot(length(files) > 0)

d <- list()
for(f in files) {
  cat("Unzipping", basename(f), "\n")
  zf <- utils::unzip(f, list = TRUE)
  annual_file <- utils::unzip(f, files = zf$Name[grep("SUBSET_YY", zf$Name)], exdir = td)

  # Read in the extracted annual file 
  stopifnot(length(annual_file) == 1)
  cat("Reading", basename(annual_file), "\n")
  readr::read_csv(annual_file, na = "-9999") %>%
    dplyr::select(TIMESTAMP, TA_F, P_F, NEE_VUT_REF_QC, GPP_DT_VUT_REF, GPP_NT_VUT_REF, RECO_DT_VUT_REF, RECO_NT_VUT_REF) %>%
    mutate(filename = annual_file) %>%
    rename(Year = TIMESTAMP) ->
    d[[f]]
  
  unlink(annual_file)
}


# Combine with site data (in particular lon/lat information)
cat("\n\nReading site data...\n")
sitedata <- read_csv("fluxnet/fluxdata_sites.csv", col_types = "ccdddcdi")

cat("Combining flux data and merging with site data...\n")
bind_rows(d) %>%
  separate(filename, into = c("FLX", "SITE_ID"), extra = "drop", sep = "_") %>%
  dplyr::select(-FLX) %>%
  left_join(sitedata, by = "SITE_ID") ->
  fluxnet

write_csv(fluxnet, "fluxnet/fluxnet_data.csv")

cat("All done with", SCRIPTNAME, "\n")
