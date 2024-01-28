pacman::p_load(tidyverse, rio, lubridate, bigrquery, DBI, RSQLite)

topic_path <- Sys.getenv('TOPIC_PATH')

con <- dbConnect(RSQLite::SQLite(), "workflow/yt/yt_all.db")


topics <- googlesheets4::read_sheet(topic_path) |>
  janitor::clean_names() |>
  mutate_at(vars(temas_de_analise, termos), tolower)

open_topic <- topics |>
  separate_rows(termos, sep = ", ")

to_find <- open_topic |>
  mutate(termos = ifelse(!str_detect(termos, " "), str_c("\\b", termos, "\\b"), termos)) |>
  pluck("termos")

regex_find <- str_c("(", paste0(to_find, collapse = "|"), ")")



all_videos <- dbReadTable(con, "yt_videos") |>
  tibble() |>
  select(id, title, publication_date, description, channel_id, channel_title, viewCount, likeCount, favoriteCount,
         commentCount, candidato, pull_date) |>
  mutate(publication_date = lubridate::as_datetime(publication_date),
         pull_date = lubridate::as_date(pull_date)) |>
  unique() |>
  arrange(desc(publication_date)) |>
  group_by(id, title, publication_date, description, channel_id, channel_title,
           candidato) |>
  summarise(viewCount = max(viewCount, na.rm = T),
            likeCount = max(likeCount, na.rm = T),
            favoriteCount = max(favoriteCount, na.rm = T),
            commentCount = max(commentCount, na.rm = T),
            pull_date = max(pull_date, na.rm = T)) |>
  mutate(pull_date = as_date(ifelse(pull_date == -Inf, Sys.Date(), pull_date)))




video_topic <- all_videos |>
  tibble() |>
  mutate(text = str_c(title, " ", description),
         topics = str_extract_all(tolower(text), regex(regex_find))) |>
  unnest(topics) |>
  left_join(select(open_topic, tema = temas_de_analise, topics = termos)) |>
  select(-topics, -text) |>
  unique() |>
  filter(!is.na(tema)) |>
  mutate(engajamento = likeCount + commentCount)


dbWriteTable(con, "yt_topics", video_topic, overwrite = T)

project_path <- Sys.getenv('PROJECT_BQ')
yt_data <- Sys.getenv("YT_DATASET")


write_bq <- function(.data, table, grouped, type = "WRITE_TRUNCATE"){
  con <- dbConnect(bigquery(),
                   project = project_path,
                   dataset = grouped)
  
  bq_df <- bigrquery::bq_table(project_path,
                               dataset = grouped,
                               table = table)
  
  bigrquery::bq_table_upload(bq_df, .data,
                             write_disposition = type,
                             fields = bigrquery::as_bq_fields(.data))
  
  dbDisconnect(con)
}

write_bq(all_videos, "all_videos", yt_data)
write_bq(video_topic, "topic_vid", "yt")


dbDisconnect(con)
