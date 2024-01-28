pacman::p_load(tidyverse, rio, lubridate, rtweet, DBI, RSQLite, bigrquery)

project_path <- Sys.getenv('PROJECT_BQ')
tw_data <- Sys.getenv("TW_DATASET")
br_trend <- rtweet::get_trends("brazil")

write_bq <- function(.data, table, type = "WRITE_TRUNCATE"){
  con <- dbConnect(bigquery(),
                   project = project_path,
                   dataset = tw_data)
  
  bq_df <- bigrquery::bq_table(project_path,
                               dataset = tw_data,
                               table = table)
  
  bigrquery::bq_table_upload(bq_df, .data,
                             write_disposition = type,
                             fields = bigrquery::as_bq_fields(.data))
  
  dbDisconnect(con)
}


write_bq(br_trend, "tw_trends", "WRITE_APPEND")


