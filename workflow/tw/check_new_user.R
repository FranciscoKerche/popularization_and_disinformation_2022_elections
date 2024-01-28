# Check for new users

pacman::p_load(tidyverse, rio, lubridate, rtweet)

user_list <- Sys.getenv('USER_LIST_PATH')
current_users <- googlesheets4::read_sheet(user_list, sheet = "tw_user_list") |>
  filter(!is.na(screen_name) & screen_name != "NA")

controle <- Sys.getenv('USER_CONTROL_PATH')
scraped_user <- googlesheets4::read_sheet(controle, sheet = "tw_page")

new_user <- current_users |>
  filter(!screen_name %in% scraped_user$screen_name)


repeat_tweet <- slowly(academictwitteR::get_all_tweets, rate = rate_delay(5))

Sys.setenv('BEARER_TOKEN' = Sys.getenv('UFMG_BEARER'))

min_time <- paste0('2022-08-01', "T00:00:00Z")
time <- lubridate::with_tz(Sys.time(), tz = "Etc/UTC")
max_time <- paste0(stringr::str_replace_all(time - lubridate::seconds(10), " ", "T"), "Z")


repeat_tweet(users = .,
             start_tweets = min_time,
             end_tweets = max_time,
             bearer_token = Sys.getenv('BEARER_TOKEN'),
             data_path = "tw_info",
             n = 5000)
usethis::ui_done("Tweets were imported!")
beepr::beep(2)
