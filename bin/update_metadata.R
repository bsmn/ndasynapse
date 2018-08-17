#!/usr/bin/env Rscript

library(tidyverse)
suppressPackageStartupMessages(library(synapseClient))
suppressMessages(synapseLogin())

fileViewId <- 'syn11298490'
labViewId <- 'syn10236893'
res <- suppressMessages(synTableQuery(sprintf("SELECT * FROM %s",
                                              labViewId))@values)

resgroups <- res %>%
  group_by(grant) %>%
  summarize(group=paste(name, collapse='-'))

tbl <- synTableQuery(sprintf("SELECT * from %s",
                             fileViewId), loadResult=FALSE) %>%
  getFileLocation() %>%
  read_csv() %>%
  filter(!(fileFormat %in% c("bed", "vcf")))

tbl <- tbl %>% filter(is.na(group))

tbl$grant <- ifelse(is.na(tbl$grant), tbl$site, tbl$grant)
tbl$group <- resgroups[match(tbl$grant, resgroups$grant), "group"]$group
tbl$individualId <- ifelse(is.na(tbl$individualId), tbl$subjectkey, tbl$individualId)
tbl$individualIdSource <- "NDA"
tbl$specimenId <- ifelse(is.na(tbl$specimenId), tbl$sample_id_original, tbl$specimenId)
tbl$specimenIdSource <- "NDA"
tbl$study <- "referenceBrain"
tbl$species <- "Human"

dataset_to_submission_id <- 'syn11299538'

dataset_to_submission <- synGet(dataset_to_submission_id) %>%
    getFileLocation() %>%
    read_csv() %>%
    rename(submission_id=SUBMISSION_ID, datasetid=DATASET_ID) %>%
    filter(SHORT_NAME == 'genomics_sample03')

tbl$submission_id <- dataset_to_submission[match(tbl$datasetid, dataset_to_submission$datasetid), ]$submission_id

tbl %>% write_csv(path="~/scratch/bsmn_upate.csv", na="")

