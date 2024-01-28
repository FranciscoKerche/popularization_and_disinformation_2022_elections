# tw_politicians

pacman::p_load(tidyverse, rio, lubridate, rtweet, 
                       academictwitteR, RSQLite, DBI,
                       bigrquery)

candidatos <- Sys.getenv('CANDIDATOS_TABLE')
governadores <- googlesheets4::read_sheet(candidatos)

all_govs <- governadores |>
  filter(!is.na(Twitter))


local_db <- dbConnect(RSQLite::SQLite(), "tw_posts.db")

# tw_new <- tbl(local_db, "tw_posts") %>%
#   filter(pull_date == max(pull_date, na.rm = T)) %>%
#   select(pull_date) %>%
#   show_query()

last_import <- dbGetQuery(local_db, "SELECT MAX(pull_date) AS pull_date FROM tw_politicians") |>
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

frag_user <- all_govs |>
  arrange(ESTADO) |>
  fragment_by(5)


user_info <- frag_user |>
  map(pluck, "Twitter")

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
                   data_path = "tw_pol",
                   n = 5000)
      p()})
    
  })
  usethis::ui_done("Tweets were imported!")
}

file.remove("tw_pol/data_.json")

# Import new tweets
all_tweets <- bind_tweets("tw_pol", output_format = "tidy") |>
  mutate(pull_date = Sys.time()) |>
  left_join(select(all_govs, user_username = Twitter, governador = Candidato, estado = ESTADO, partido = PARTIDO)) |>
  unique()

  

if(nrow(all_tweets) > 0){
  
  usethis::ui_info(str_c("There are ", nrow(all_tweets), " new tweets!"))
  
  
  # remove old files
  
  list.files("tw_pol", full.names = T) |>
    walk(file.remove)
  
  # Update file locally
  
  # local_db <- dbConnect(RSQLite::SQLite(), "tw_posts.db")
  
  dbWriteTable(local_db, "tw_politicians", all_tweets, append = T)
  dbDisconnect(local_db)
  
} else{
  usethis::ui_warn("nothing to add here.")
}

