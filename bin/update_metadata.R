#!/usr/bin/env Rscript

library(tidyverse)
suppressPackageStartupMessages(library(synapseClient))
suppressMessages(synapseLogin())

res <- suppressMessages(synTableQuery("SELECT * FROM syn10236893")@values)

resgroups <- res %>% 
  group_by(grant) %>% 
  summarize(group=paste(name, collapse=','))

tbl <- synTableQuery("SELECT * from syn7871084", loadResult=FALSE) %>% 
  getFileLocation() %>% 
  read_csv()

tbl$grant <- ifelse(is.na(tbl$grant), tbl$site, tbl$grant)
tbl$group <- resgroups[match(tbl$grant, resgroups$grant), "group"]$group
tbl$individualId <- tbl$subjectkey
tbl$individualIdSource <- "NDA"
tbl$specimenId <- tbl$sample_id_original
tbl$specimenIdSource <- "NDA"
tbl$study <- "referenceBrain"

tbl %>% write_csv(path="~/scratch/bsmn_upate.csv", na="")

