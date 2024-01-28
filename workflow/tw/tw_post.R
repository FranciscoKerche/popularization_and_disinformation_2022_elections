# import packages

pacman::p_load(tidyverse, rio, lubridate, rtweet, 
               academictwitteR, RSQLite, DBI,
               bigrquery)

user_list <- Sys.getenv("USER_LIST")
controle <- Sys.getenv("CONTROL_TABLE")
current_users <- googlesheets4::read_sheet(user_list, sheet = "tw_user_list") |>
  filter(!is.na(screen_name) & screen_name != "NA")
# last_import <- googlesheets4::read_sheet(controle,
#                                          sheet = "last_import")



local_db <- dbConnect(RSQLite::SQLite(), "tw_posts.db")

# tw_new <- tbl(local_db, "tw_posts") %>%
#   filter(pull_date == max(pull_date, na.rm = T)) %>%
#   select(pull_date) %>%
#   show_query()


last_import <- dbGetQuery(local_db, "SELECT `pull_date`
FROM (SELECT `tweet_id`, `user_username`, `text`, `lang`, `created_at`, `source`, `conversation_id`, `possibly_sensitive`, `author_id`, `in_reply_to_user_id`, `user_url`, `user_created_at`, `user_name`, `user_pinned_tweet_id`, `user_description`, `user_location`, `user_verified`, `user_protected`, `user_profile_image_url`, `retweet_count`, `like_count`, `quote_count`, `user_tweet_count`, `user_list_count`, `user_followers_count`, `user_following_count`, `sourcetweet_type`, `sourcetweet_id`, `sourcetweet_text`, `sourcetweet_lang`, `sourcetweet_author_id`, `pull_date`, `candidato`
FROM (SELECT `tweet_id`, `user_username`, `text`, `lang`, `created_at`, `source`, `conversation_id`, `possibly_sensitive`, `author_id`, `in_reply_to_user_id`, `user_url`, `user_created_at`, `user_name`, `user_pinned_tweet_id`, `user_description`, `user_location`, `user_verified`, `user_protected`, `user_profile_image_url`, `retweet_count`, `like_count`, `quote_count`, `user_tweet_count`, `user_list_count`, `user_followers_count`, `user_following_count`, `sourcetweet_type`, `sourcetweet_id`, `sourcetweet_text`, `sourcetweet_lang`, `sourcetweet_author_id`, `pull_date`, `candidato`, MAX(`pull_date`) OVER () AS `q01`
FROM `tw_posts`)
WHERE (`pull_date` = `q01`))
                          LIMIT 1") |>
  tibble() |>
  mutate(pull_date = lubridate::as_datetime(pull_date)) |>
  pluck("pull_date")




fragment_by <- function(.x, groups_of){
  total_groups = nrow(.x) %/% groups_of + 1
  splited_data <- vector("list", total_groups)
  for(i in 1:total_groups){
    splited_data[[i]] <- .x |>
      slice(((i-1)*groups_of+1):(i*groups_of))
  }
  return(splited_data)
}

frag_user <- current_users |>
  fragment_by(5)


user_info <- frag_user |>
  map(pluck, "screen_name")

repeat_tweet <- slowly(academictwitteR::get_all_tweets, rate = rate_delay(5))

Sys.setenv('BEARER_TOKEN' = Sys.getenv('UFMG_BEARER'))

time_low <- lubridate::with_tz(last_import, tz = "Etc/UTC")
min_time <- paste0(stringr::str_replace_all(time_low, " ", "T"), "Z")
time_high <- lubridate::with_tz(Sys.time(), tz = "Etc/UTC")
max_time <- paste0(stringr::str_replace_all(time_high - lubridate::seconds(10), " ", "T"), "Z")




if(as.Date(min_time) == Sys.Date()){
  usethis::ui_warn("Ops, you have done this earlier today!")
} else{
  progressr::with_progress({
    p <- progressr::progressor(length(user_info))
    get_new_tweets <- walk(user_info, ~{
      repeat_tweet(users = .,
                   start_tweets = min_time,
                   end_tweets = max_time,
                   bearer_token = Sys.getenv('BEARER_TOKEN'),
                   data_path = "tw_info",
                   n = 5000)
      p()})
    
  })
  usethis::ui_done("Tweets were imported!")
}

file.remove("tw_info/data_.json")

# Import new tweets
all_tweets <- bind_tweets("tw_info", output_format = "tidy") |>
  mutate(pull_date = Sys.time()) |>
  left_join(select(current_users, user_username = screen_name, candidato))

if(ncol(all_tweets) > 0){
  
  usethis::ui_info(str_c("There are ", nrow(all_tweets), " new tweets!"))
  
  last_import <- tibble(last_import = Sys.time())
  googlesheets4::write_sheet(last_import, controle,
                             sheet = "last_import")
  
  
  # remove old files
  
  list.files("tw_info", full.names = T) |>
    walk(file.remove)
  
  # Update file locally
  
  # local_db <- dbConnect(RSQLite::SQLite(), "tw_posts.db")
  
  dbWriteTable(local_db, "tw_posts", all_tweets, append = T)
  dbDisconnect(local_db)
  
} else{
  usethis::ui_warn("nothing to add here.")
}


