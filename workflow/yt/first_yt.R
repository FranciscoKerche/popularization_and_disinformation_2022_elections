# First yt

pacman::p_load(tidyverse, rio, lubridate, tuber)

all_seeds <- Sys.getenv('ALL_SEEDS')

yt_lula <- googlesheets4::read_sheet(all_seeds, sheet = "lula_yt") |>
  janitor::clean_names() |>
  filter(relevant == 'T')
yt_bozo <- googlesheets4::read_sheet(all_seeds, sheet = "bolsonaro_yt") |>
  janitor::clean_names() |>
  filter(relevant == 'T')



tuber::yt_oauth(app_id = Sys.getenv('OAUTH_ID'),
                app_secret = Sys.getenv('OAUTH_SECRET'))


get_video_name <- function(channel_id = NULL, mine = F, max_date = NULL){
  
  if (!is.character(channel_id) & !identical(tolower(mine),  "true")) {
    stop("Must specify a channel ID or specify mine = 'true'.")
  }
  
  a <- list_channel_resources(filter = c(channel_id = channel_id), part = "contentDetails")
  
  playlist_id <- a$items[[1]]$contentDetails$relatedPlaylists$uploads
  
  vids <- get_playlist_items(filter = c(playlist_id = playlist_id), max_results = 100) |>
    tibble() |>
    filter(contentDetails.videoPublishedAt > as.Date({{max_date}}))
  
  vid_ids <- as.vector(vids$contentDetails.videoId)
}

get_videos_info <- function(vid_ids){
  res <- lapply(vid_ids, get_stats)
  details <- lapply(vid_ids, get_video_details)
  res_df <- do.call(what = bind_rows, lapply(res, data.frame))
  
  details_tot <- data.frame(id = NA, title = NA,
                            publication_date = NA, description = NA,
                            channel_id = NA, channel_title = NA)
  
  for (p in seq_along(details)) {
    id <- details[[p]]$items[[1]]$id
    title <- details[[p]]$items[[1]]$snippet$title
    publication_date <- details[[p]]$items[[1]]$snippet$publishedAt
    description <- details[[p]]$items[[1]]$snippet$description
    channel_id <- details[[p]]$items[[1]]$snippet$channelId
    channel_title <- details[[p]]$items[[1]]$snippet$channelTitle
    
    detail <- data.frame(id = id, title = title,
                         publication_date = publication_date, description = description,
                         channel_id = channel_id, channel_title = channel_title)
    details_tot <- rbind(detail, details_tot)
  }
  
  res_df$url <- paste0("https://www.youtube.com/watch?v=", res_df$id)
  
  res_df <- merge(details_tot, res_df, by = "id")
  
  res_df
}


get_user_video <- function(.x, date = NULL){
  video_links <- get_video_name(.x, max_date = date)
  final_video <- get_videos_info(video_links)
  write_excel_csv(final_video, str_c("workflow/yt/user_video/", .x, "_", Sys.Date(), ".csv"))
}
try_user <- possibly(get_user_video, otherwise = NULL)

ids_lula <- pluck(yt_lula, "id")
# try_user(ids_lula[1], date = '2022-08-01')
progressr::with_progress({
  p <- progressr::progressor(length(ids_lula))
  all_lula <- map(ids_lula, ~{
    try_user(., date = '2022-08-01')
    p()
    })
})


ids_bolsonaro <- pluck(yt_bozo, "id")
# try_user(ids_lula[1], date = '2022-08-01')
progressr::with_progress({
  p <- progressr::progressor(length(ids_bolsonaro))
  all_lula <- map(ids_bolsonaro, ~{
    try_user(., date = '2022-08-01')
    p()
  })
})


yt_candidate <- yt_lula |>
  mutate(candidato = "lula") |>
  bind_rows(yt_bozo) |>
  mutate(candidato = if_else(is.na(candidato), "bolsonaro", candidato)) |>
  select(channel_id = id, candidato)

all_videos <- list.files("workflow/yt/user_video", full.names = T) |>
  map_df(import, setclass = "tibble") |>
  mutate(pull_date = Sys.Date()) |>
  select(id, title, publication_date, description, channel_id, channel_title, viewCount, likeCount, favoriteCount,
         commentCount) |>
  left_join(yt_candidate)


con <- dbConnect(RSQLite::SQLite(), "workflow/yt/yt_all.db")
dbWriteTable(con, "yt_videos", all_videos, overwrite = T)

RSQLite::dbDisconnect(con)



