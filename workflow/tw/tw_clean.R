# Clean post ---------------------------------------------------------

pacman::p_load(tidyverse, rio, lubridate, RSQLite, DBI,
               bigrquery)

local_db <- dbConnect(RSQLite::SQLite(), "tw_posts.db")
history_tw <- dbReadTable(local_db, "tw_posts") |>
  tibble()

topics <- googlesheets4::read_sheet(Sys.getenv("TOPIC_LIST")) |>
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
tw_all <- dbReadTable(local_db, "tw_posts")

tw_topic <- tw_all |>
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
         candidato, tema) |>
  mutate(engajamento = retweet_count + like_count + quote_count,
         pull_date = lubridate::as_datetime(pull_date),
         created_at = as.Date(created_at))

dbWriteTable(local_db, "tw_topic", tw_topic, overwrite = T)

tw_new <- dbGetQuery(local_db, "SELECT `tweet_id`, `user_username`, `text`, `lang`, `created_at`, `source`, `conversation_id`, `possibly_sensitive`, `author_id`, `in_reply_to_user_id`, `user_url`, `user_created_at`, `user_name`, `user_pinned_tweet_id`, `user_description`, `user_location`, `user_verified`, `user_protected`, `user_profile_image_url`, `retweet_count`, `like_count`, `quote_count`, `user_tweet_count`, `user_list_count`, `user_followers_count`, `user_following_count`, `sourcetweet_type`, `sourcetweet_id`, `sourcetweet_text`, `sourcetweet_lang`, `sourcetweet_author_id`, `pull_date`, `candidato`
FROM (SELECT `tweet_id`, `user_username`, `text`, `lang`, `created_at`, `source`, `conversation_id`, `possibly_sensitive`, `author_id`, `in_reply_to_user_id`, `user_url`, `user_created_at`, `user_name`, `user_pinned_tweet_id`, `user_description`, `user_location`, `user_verified`, `user_protected`, `user_profile_image_url`, `retweet_count`, `like_count`, `quote_count`, `user_tweet_count`, `user_list_count`, `user_followers_count`, `user_following_count`, `sourcetweet_type`, `sourcetweet_id`, `sourcetweet_text`, `sourcetweet_lang`, `sourcetweet_author_id`, `pull_date`, `candidato`, MAX(`pull_date`) OVER () AS `q01`
FROM `tw_posts`)
WHERE (`pull_date` = `q01`)") |>
  tibble()

updated_base <- tw_new |>
  mutate(pull_date = lubridate::as_datetime(pull_date)) |>
  mutate(created_at = as.Date(created_at),
         user_created_at = as.Date(user_created_at),
         engagement = like_count + retweet_count)



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


write_bq(updated_base, "all_posts", "WRITE_APPEND")

write_bq(tw_topic, "tw_topic")


