# Packages ----------------------------------------------------------------

pacman::p_load(tidyverse, rio, lubridate, rtweet, academictwitteR)

# Tw page

current_tw <- googlesheets4::read_sheet(controle, sheet = "tw_page")


lula_tw <- googlesheets4::read_sheet(controle, sheet = "lula_basic_info")
bolso_tw <- googlesheets4::read_sheet(controle, sheet = "bolsonaro_basic_info")

lula_info <- rtweet::lookup_users(lula_tw$user_id)

simple_lula <- lula_info |>
  select(user_id, bio = description, created_at, screen_name, friends_count, followers_count)

bolso_info <- rtweet::lookup_users(bolso_tw$user_id)

simple_bolso <- bolso_info |>
  select(user_id, bio = description, created_at, screen_name, friends_count, followers_count)

walk2(list(simple_lula, simple_bolso), c("lula_basic_info", "bolsonaro_basic_info"),
      ~googlesheets4::write_sheet(.x, controle,
                                  sheet = .y))

# Daily extraction of followers and followees

tw_page <- simple_lula |>
  bind_rows(simple_bolso, .id = "candidato") |>
  mutate(candidato = case_when(candidato == 1 ~"lula",
                               candidato == 2 ~"bolsonaro")) |>
  mutate(pull_date = Sys.Date()) |>
  select(-created_at)


if(max(as.Date(current_tw$extraction_date)) != Sys.Date()){
  current_tw |>
    bind_rows(tw_page) |>
    googlesheets4::write_sheet(controle, sheet = "tw_page")
} else{
  usethis::ui_warn("ops, this data is already there")
}
