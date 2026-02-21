library(DSMflow)
library(tidyverse)

action_5 <- read_rds("data-raw/calsim_3_action_5/action_5.rds")

# --- helpers ------------------------------------------------------------------
mat_to_df <- function(mat, scenario_name) {
  tibble(
    month = rep(rownames(mat), ncol(mat)),
    year  = rep(as.integer(colnames(mat)), each = nrow(mat)),
    value = as.vector(mat),
    scenario = scenario_name
  ) |> mutate(date = as.Date(paste(year, match(month, month.abb), 1, sep = "-")))
}

arr_to_df <- function(arr, scenario_name) {
  df <- as.data.frame.table(arr, responseName = "value", stringsAsFactors = FALSE)
  names(df) <- c("watershed", "month", "year", "value")
  df$scenario <- scenario_name
  df$year  <- as.integer(df$year)
  df$date  <- as.Date(paste(df$year, match(df$month, month.abb), 1, sep = "-"))
  df
}

# --- 1. Flows CFS by watershed -----------------------------------------------
lto_flows <- DSMflow::flows_cfs$LTO_12a |>
  filter(date >= as.Date("1980-01-01"), date <= as.Date("2000-12-31")) |>
  mutate(date = as.Date(date)) |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow_cfs") |>
  mutate(scenario = "LTO_12a")

act_flows <- action_5$flows_cfs |>
  mutate(date = as.Date(date)) |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow_cfs") |>
  mutate(scenario = "action_5")

shared_ws <- intersect(unique(lto_flows$watershed), unique(act_flows$watershed))

flows_long <- bind_rows(lto_flows, act_flows) |> filter(watershed %in% shared_ws)

ggplot(flows_long, aes(date, flow_cfs, color = scenario)) +
  geom_line(alpha = 0.6) +
  facet_wrap(~watershed, scales = "free_y") +
  labs(title = "Flow (cfs) by Watershed", x = NULL, y = "Flow (cfs)") +
  theme_minimal() + theme(legend.position = "bottom")

# --- 2. Proportion Diverted --------------------------------------------------
prop_div_long <- bind_rows(
  arr_to_df(DSMflow::proportion_diverted$LTO_12a, "LTO_12a"),
  arr_to_df(action_5$proportion_diverted, "action_5")
)

ggplot(prop_div_long, aes(date, value, color = scenario)) +
  geom_line(alpha = 0.6) +
  facet_wrap(~watershed, scales = "free_y") +
  labs(title = "Proportion Diverted by Watershed", x = NULL, y = "Proportion Diverted") +
  theme_minimal() + theme(legend.position = "bottom")

# --- 3. Total Diverted -------------------------------------------------------
tot_div_long <- bind_rows(
  arr_to_df(DSMflow::total_diverted$LTO_12a, "LTO_12a"),
  arr_to_df(action_5$total_diverted, "action_5")
)

ggplot(tot_div_long, aes(date, value, color = scenario)) +
  geom_line(alpha = 0.6) +
  facet_wrap(~watershed, scales = "free_y") +
  labs(title = "Total Diverted (cms) by Watershed", x = NULL, y = "Total Diverted (cms)") +
  theme_minimal() + theme(legend.position = "bottom")

# --- 4. Delta Flows -----------------------------------------------------------
delta_long <- bind_rows(
  DSMflow::delta_flows$LTO_12a |> mutate(date = as.Date(date), scenario = "LTO_12a"),
  action_5$delta_flows |> mutate(date = as.Date(date), scenario = "action_5")
) |>
  pivot_longer(c(n_dlt_inflow_cfs, s_dlt_inflow_cfs, n_dlt_div_cfs,
                 s_dlt_div_cfs, n_dlt_prop_div, s_dlt_prop_div),
               names_to = "variable", values_to = "value")

ggplot(delta_long, aes(date, value, color = scenario)) +
  geom_line(alpha = 0.6) +
  facet_wrap(~variable, scales = "free_y") +
  labs(title = "Delta Flows & Diversions", x = NULL, y = NULL) +
  theme_minimal() + theme(legend.position = "bottom")

# --- 5. Single-node flows & exports (month x year matrices) -------------------
single_node_long <- bind_rows(
  mat_to_df(DSMflow::upper_sacramento_flows$LTO_12a, "LTO_12a") |> mutate(variable = "Upper Sac Flow"),
  mat_to_df(action_5$upper_sacramento_flows, "action_5") |> mutate(variable = "Upper Sac Flow"),
  mat_to_df(DSMflow::freeport_flow$LTO_12a, "LTO_12a") |> mutate(variable = "Freeport Flow"),
  mat_to_df(action_5$freeport_flow, "action_5") |> mutate(variable = "Freeport Flow"),
  mat_to_df(DSMflow::vernalis_flow$LTO_12a, "LTO_12a") |> mutate(variable = "Vernalis Flow"),
  mat_to_df(action_5$vernalis_flow, "action_5") |> mutate(variable = "Vernalis Flow"),
  mat_to_df(DSMflow::stockton_flow$LTO_12a, "LTO_12a") |> mutate(variable = "Stockton Flow"),
  mat_to_df(action_5$stockton_flow, "action_5") |> mutate(variable = "Stockton Flow"),
  mat_to_df(DSMflow::cvp_exports$LTO_12a, "LTO_12a") |> mutate(variable = "CVP Exports"),
  mat_to_df(action_5$cvp_exports, "action_5") |> mutate(variable = "CVP Exports"),
  mat_to_df(DSMflow::swp_exports$LTO_12a, "LTO_12a") |> mutate(variable = "SWP Exports"),
  mat_to_df(action_5$swp_exports, "action_5") |> mutate(variable = "SWP Exports")
)

ggplot(single_node_long, aes(date, value, color = scenario)) +
  geom_line(alpha = 0.6) +
  facet_wrap(~variable, scales = "free_y") +
  labs(title = "Single-Node Flows & Exports (cms)", x = NULL, y = "cms") +
  theme_minimal() + theme(legend.position = "bottom")

# --- 6. Bypass Proportion Flow ------------------------------------------------
bypass_to_df <- function(arr, scenario_name) {
  df <- as.data.frame.table(arr, responseName = "value", stringsAsFactors = FALSE)
  names(df) <- c("month", "year", "bypass", "value")
  df$scenario <- scenario_name
  df$year  <- as.integer(df$year)
  df$date  <- as.Date(paste(df$year, match(df$month, month.abb), 1, sep = "-"))
  df
}

bypass_long <- bind_rows(
  bypass_to_df(DSMflow::proportion_flow_bypasses$LTO_12a, "LTO_12a"),
  bypass_to_df(action_5$proportion_flow_bypasses, "action_5")
)

ggplot(bypass_long, aes(date, value, color = scenario)) +
  geom_line(alpha = 0.6) +
  facet_wrap(~bypass, scales = "free_y") +
  labs(title = "Proportion Flow Bypasses", x = NULL, y = "Proportion") +
  theme_minimal() + theme(legend.position = "bottom")
