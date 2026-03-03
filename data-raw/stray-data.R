library(tidyverse)

hatchery_to_watershed_lookup <- c(
  coleman = "Battle Creek",
  feather = "Feather River",
  nimbus = "American River",
  mokelumne = "Mokelumne River",
  merced = "Merced River"
)

hatchery_oct_nov_flows <- vector(mode = "list")

hatchery_oct_nov_flows$biop_itp_2018_2019 <- flows_cfs$biop_itp_2018_2019 |>
  select(date, tidyselect::all_of(as.character(hatchery_to_watershed_lookup))) |>
  filter(month(date) %in% 10:11) |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow_cfs") |>
  group_by(year = year(date), watershed) |>
  summarise(avg_flow_cfs = mean(flow_cfs)) |>
  ungroup() |>
  filter(year %in% 1980:2002) |>
  pivot_wider(names_from = "watershed", values_from = "avg_flow_cfs") |>
  select(-year) |>
  as.matrix() |>
  `row.names<-`(1980:2002) |>
  t()

hatchery_oct_nov_flows$LTO_12a <- flows_cfs$LTO_12a |>
  select(date, tidyselect::all_of(as.character(hatchery_to_watershed_lookup))) |>
  filter(month(date) %in% 10:11) |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow_cfs") |>
  group_by(year = year(date), watershed) |>
  summarise(avg_flow_cfs = mean(flow_cfs)) |>
  ungroup() |>
  filter(year %in% 1980:2002) |>
  pivot_wider(names_from = "watershed", values_from = "avg_flow_cfs") |>
  select(-year) |>
  as.matrix() |>
  `row.names<-`(1980:2002) |>
  t()

hatchery_oct_nov_flows$action_5 <- flows_cfs$action_5 |>
  select(date, tidyselect::all_of(as.character(hatchery_to_watershed_lookup))) |>
  filter(month(date) %in% 10:11) |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow_cfs") |>
  group_by(year = year(date), watershed) |>
  summarise(avg_flow_cfs = mean(flow_cfs)) |>
  ungroup() |>
  filter(year %in% 1980:2002) |>
  pivot_wider(names_from = "watershed", values_from = "avg_flow_cfs") |>
  select(-year) |>
  as.matrix() |>
  `row.names<-`(1980:2000) |>
  t()

usethis::use_data(hatchery_oct_nov_flows, overwrite = TRUE)


hatchery_apr_may_flows <- vector(mode = "list")

hatchery_apr_may_flows$biop_itp_2018_2019 <- flows_cfs$biop_itp_2018_2019 |>
  select(date, tidyselect::all_of(as.character(hatchery_to_watershed_lookup))) |>
  filter(month(date) %in% 4:5) |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow_cfs") |>
  group_by(year = year(date), watershed) |>
  summarise(avg_flow_cfs = mean(flow_cfs)) |>
  ungroup() |>
  filter(year %in% 1980:2002) |>
  pivot_wider(names_from = "watershed", values_from = "avg_flow_cfs") |>
  select(-year) |>
  as.matrix() |>
  `row.names<-`(1980:2002) |>
  t()

hatchery_apr_may_flows$LTO_12a <- flows_cfs$LTO_12a |>
  select(date, tidyselect::all_of(as.character(hatchery_to_watershed_lookup))) |>
  filter(month(date) %in% 4:5) |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow_cfs") |>
  group_by(year = year(date), watershed) |>
  summarise(avg_flow_cfs = mean(flow_cfs)) |>
  ungroup() |>
  filter(year %in% 1980:2002) |>
  pivot_wider(names_from = "watershed", values_from = "avg_flow_cfs") |>
  select(-year) |>
  as.matrix() |>
  `row.names<-`(1980:2002) |>
  t()

hatchery_apr_may_flows$action_5 <- flows_cfs$action_5 |>
  select(date, tidyselect::all_of(as.character(hatchery_to_watershed_lookup))) |>
  filter(month(date) %in% 4:5) |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow_cfs") |>
  group_by(year = year(date), watershed) |>
  summarise(avg_flow_cfs = mean(flow_cfs)) |>
  ungroup() |>
  filter(year %in% 1980:2002) |>
  pivot_wider(names_from = "watershed", values_from = "avg_flow_cfs") |>
  select(-year) |>
  as.matrix() |>
  `row.names<-`(1980:2000) |>
  t()

usethis::use_data(hatchery_apr_may_flows, overwrite = TRUE)

