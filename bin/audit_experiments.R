  library(tidyverse)

d <- synGet("syn11299538") %>% 
  getFileLocation() %>% 
  read_csv() %>% 
  mutate(SUBMISSION_ID=as.character(SUBMISSION_ID), 
         EXPERIMENT_ID=as.character(EXPERIMENT_ID),
         DATASET_ID=as.character(DATASET_ID)) %>% 
  rename(submission_id=SUBMISSION_ID, 
         experiment_id=EXPERIMENT_ID,
         datasetid=DATASET_ID) %>% 
  filter(!is.na(experiment_id))

refInfo <- synTableQuery('select * from syn10170802')@values

rtd <- synTableQuery("select * from syn7871084")@values

joined <- refInfo %>% left_join(d, by=c("datasetid", "experiment_id", "submission_id"))

joined %>% 
  select(experiment_id, submission_id, datasetid,
        grant, group, Data_Liason, Sample_Type, Technology)
