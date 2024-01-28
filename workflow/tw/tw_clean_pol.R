# Clean politician

pacman::p_load(tidyverse, rio, lubridate, RSQLite, DBI,
               bigrquery)

local_db <- dbConnect(RSQLite::SQLite(), "tw_posts.db")
history_tw <- dbReadTable(local_db, "tw_politicians") |>
  tibble() |>
  group_by(tweet_id) |>
  filter(pull_date == max(pull_date)) |>
  unique() |>
  ungroup() |>
  mutate(pull_date = lubridate::as_datetime(pull_date),
         created_at = as.Date(created_at),
         engagement = like_count + quote_count + retweet_count)

POLITICIANS_TABLE <- Sys.getenv('POLITICIANS_TABLE')

topics <- googlesheets4::read_sheet("POLITICIANS_TABLE") |>
  janitor::clean_names() |>
  mutate_at(vars(temas_de_analise, termos), tolower)

open_topic <- topics |>
  separate_rows(termos, sep = ", ")

to_find <- open_topic |>
  mutate(termos = ifelse(!str_detect(termos, " "), str_c("\\b", termos, "\\b"), termos)) |>
  pluck("termos")

regex_find <- str_c("(", paste0(to_find, collapse = "|"), ")")

# 
# tw_new <- tbl(local_db, "tw_posts") %>%
#   filter(pull_date == max(pull_date, na.rm = T)) %>%
#   show_query()
# 
# tw_all <- dbReadTable(local_db, "tw_politicians")

tw_topic <- history_tw |>
  tibble() |>
  mutate(topics = str_extract_all(tolower(text), regex(regex_find))) |>
  unnest(topics) |>
  left_join(select(open_topic, tema = temas_de_analise, topics = termos)) |>
  select(-topics) |>
  unique() |>
  filter(!is.na(tema)) |>
  select(tweet_id, user_username, text, created_at, author_id, user_url,
         user_name, user_description, retweet_count, like_count, quote_count,
         user_tweet_count, user_followers_count, user_following_count, pull_date,
         governador, estado, partido, tema) |>
  mutate(engajamento = retweet_count + like_count + quote_count,
         pull_date = lubridate::as_datetime(pull_date),
         created_at = as.Date(created_at))

dbWriteTable(local_db, "tw_topic_pol", tw_topic, overwrite = T)



write_bq <- function(.data, table, type = "WRITE_TRUNCATE"){
  con <- dbConnect(bigquery(),
                   project = "observatorio-eleicao",
                   dataset = "tw_posts")
  
  bq_df <- bigrquery::bq_table("observatorio-eleicao",
                               dataset = "tw_posts",
                               table = table)
  
  bigrquery::bq_table_upload(bq_df, .data,
                             write_disposition = type,
                             fields = bigrquery::as_bq_fields(.data))
  
  dbDisconnect(con)
}


write_bq(history_tw, "all_posts_pol")
# write_bq(updated_base, "all_posts_pol", "WRITE_APPEND")

write_bq(tw_topic, "tw_topic_pol")

beepr::beep()
