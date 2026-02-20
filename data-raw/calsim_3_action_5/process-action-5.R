library(tidyverse)

action_5 <- list()
watershed_lookup <- c(
  "lower_mid_sacramento_river" = "Lower-mid Sacramento River",
  "american_river" = "American River",
  "antelope_creek" = "Antelope Creek",
  "battle_creek" = "Battle Creek",
  "bear_creek" = "Bear Creek",
  "bear_river" = "Bear River",
  "big_chico_creek" = "Big Chico Creek",
  "butte_creek" = "Butte Creek",
  "calaveras_river" = "Calaveras River",
  "clear_creek" = "Clear Creek",
  "cosumnes_river" = "Cosumnes River",
  "cottonwood_creek" = "Cottonwood Creek",
  "cow_creek" = "Cow Creek",
  "deer_creek" = "Deer Creek",
  "elder_creek" = "Elder Creek",
  "feather_river" = "Feather River",
  "lower_sacramento_river" = "Lower Sacramento River",
  "merced_river" = "Merced River",
  "mill_creek" = "Mill Creek",
  "mokelumne_river" = "Mokelumne River",
  "paynes_creek" = "Paynes Creek",
  "san_joaquin_river" = "San Joaquin River",
  "stanislaus_river" = "Stanislaus River",
  "stony_creek" = "Stony Creek",
  "thomes_creek" = "Thomes Creek",
  "tuolumne_river" = "Tuolumne River",
  "upper_mid_sacramento_river" = "Upper-mid Sacramento River",
  "upper_sacramento_river" = "Upper Sacramento River",
  "yuba_river" = "Yuba River"
)

calsim_data <- readr::read_csv("data-raw/calsim_3_action_5/calsim3-processed-all.csv")
raw_data <- readr::read_csv("data-raw/calsim_3_action_5/sdm_action_5_vason.csv")

raw_data |>
  filter(node %in% c("C_CSL004B", "DD_SAC017_SACS"), year(datetime) %in% 1980:2000) |>
  group_by(datetime) |>
  summarise(values = sum(values))


# delta diversions total --------------------------------------
delta_diverseion_total <- raw_data |> filter(year(datetime) %in% 1980:2000, dataset == "delta_diversions_total") |>
  group_by(region, datetime) |>
  summarise(values = sum(values)) |>
  ungroup() |>
  mutate(values = DSMflow::cfs_to_cms(values),
         month = month(datetime),
         year = year(datetime)) |>
  select(region, month, year, values)

north_delta_diversions <- delta_diverseion_total |> filter(region == "north_delta") |>
  pivot_wider(names_from=year, values_from = values) |>
  select(-region, -month) |>
  as.matrix()
row.names(north_delta_diversions) <- month.abb

south_delta_diversions <- delta_diverseion_total |> filter(region == "south_delta") |>
  pivot_wider(names_from=year, values_from = values) |>
  select(-region, -month) |>
  as.matrix()
row.names(south_delta_diversions) <- month.abb


delta_total_diverted <- matrix()
delta_total_diverted[,,"North Delta"] <- north_delta_diversions
delta_total_diverted[,,"South Delta"] <- south_delta_diversions


# delta inflows --------------------------------------------
delta_inflows <- raw_data |>
  filter(dataset == "delta_inflows",
         year(datetime) %in% 1980:2000) |>
  group_by(region, datetime) |>
  summarise(
    values = sum(values)
  ) |>
  ungroup() |>
  mutate(values = DSMflow::cfs_to_cms(values))

north_delta_inflows <- delta_inflows |> filter(region == "north_delta") |>
  mutate(year = year(datetime), month = month(datetime)) |>
  select(-region, -datetime) |>
  pivot_wider(names_from = "year", values_from = "values") |>
  select(-month) |>
  as.matrix()

row.names(north_delta_inflows) <- month.abb

south_delta_inflows <- delta_inflows |> filter(region == "south_delta") |>
  mutate(year = year(datetime), month = month(datetime)) |>
  select(-region, -datetime) |>
  pivot_wider(names_from = "year", values_from = "values") |>
  select(-month) |>
  as.matrix()

row.names(south_delta_inflows) <- month.abb


# avg flow -----------------------------------------------------
# for most of these we can just do a sum which is just one node anyway
# lower_mid_sac we need to do C_SAC093 * 35.6/58 + C_SAC048 * 22.4/58
most_avg_flows <- raw_data |> filter(dataset == "avg_flows", year(datetime) %in% 1980:2000,
                   region != "lower_mid_sacramento_river") |>
  group_by(datetime, region) |>
  summarise(values = sum(values)) |>
  ungroup()

lower_mid_sacramento_river_avg_flows <- raw_data |> filter(region == "lower_mid_sacramento_river", dataset == "avg_flows") |>
  pivot_wider(names_from = "node", values_from = values) |>
  mutate(values = (C_SAC093 * 35.6/58) + (C_SAC048 * 22.4/58)) |>
  select(datetime, region, values )

avg_flows <- bind_rows(most_avg_flows, lower_mid_sacramento_river_avg_flows) |>
  filter(year(datetime) %in% 1980:2000) |>
  arrange(datetime, region) |>
  mutate(watershed = watershed_lookup[region]) |>
  select(date = datetime, watershed, values) |>
  pivot_wider(names_from = "watershed", values_from = "values")


# Upper Sacramento Flows ------------------------------------------
upper_sacramento_flows <- raw_data |> filter(dataset == "upsac_flow", year(datetime) %in% 1980:2000) |>
  transmute(year = year(datetime), month = month(datetime), values = DSMflow::cfs_to_cms(values)) |>
  pivot_wider(names_from = "year", values_from = "values") |>
  select(-month) |>
  as.matrix()
row.names(upper_sacramento_flows) <- month.abb

# Freeport Flows ------------------------------------------
freeport_flows <- raw_data |> filter(dataset == "freeport_flow", year(datetime) %in% 1980:2000) |>
  transmute(year = year(datetime), month = month(datetime), values = DSMflow::cfs_to_cms(values)) |>
  pivot_wider(names_from = "year", values_from = "values") |>
  select(-month) |>
  as.matrix()
row.names(freeport_flows) <- month.abb

# Vernalis Flows ------------------------------------------
vernalis_flows <- raw_data |> filter(dataset == "vernalis_flow", year(datetime) %in% 1980:2000) |>
  transmute(year = year(datetime), month = month(datetime), values = DSMflow::cfs_to_cms(values)) |>
  pivot_wider(names_from = "year", values_from = "values") |>
  select(-month) |>
  as.matrix()
row.names(vernalis_flows) <- month.abb

# Stockton Flows ------------------------------------------
stockton_flows <- raw_data |> filter(dataset == "stockton_flow", year(datetime) %in% 1980:2000) |>
  transmute(year = year(datetime), month = month(datetime), values = DSMflow::cfs_to_cms(values)) |>
  pivot_wider(names_from = "year", values_from = "values") |>
  select(-month) |>
  as.matrix()
row.names(stockton_flows) <- month.abb

# CVP Exports Realized (Jones Pumping) ------------------------------------------
# these use the updated nodes recommended in the LTO report
cvp_realized <- raw_data |> filter(dataset == "cvp_exports_realized", year(datetime) %in% 1980:2000) |>
  transmute(year = year(datetime), month = month(datetime), values = DSMflow::cfs_to_cms(values)) |>
  pivot_wider(names_from = "year", values_from = "values") |>
  select(-month) |>
  as.matrix()
row.names(cvp_realized) <- month.abb

# SWP Exports Realized (Banks Pumping) ------------------------------------------
swp_realized <- raw_data |> filter(dataset == "swp_exports_realized", year(datetime) %in% 1980:2000) |>
  transmute(year = year(datetime), month = month(datetime), values = DSMflow::cfs_to_cms(values)) |>
  pivot_wider(names_from = "year", values_from = "values") |>
  select(-month) |>
  as.matrix()
row.names(swp_realized) <- month.abb


# Proportion Diverted --------------------------------------------------------
prop_div_wide <- raw_data |>
  filter(dataset == "prop_diversion", year(datetime) %in% 1980:2000) |>
  select(-dataset, -region, -units) |>
  pivot_wider(names_from = node, values_from = values)

proportion_diverted <- prop_div_wide |>
  transmute(
    date = datetime,
    `Upper Sacramento River` = pmin((D_SAC296_WTPFTH + D_SAC296_02_SA + D_SAC294_WTPBLV + D_SAC294_03_PA + D_SAC281_02_NA + D_SAC273_03_NA) / C_SAC273, 1),
    `Antelope Creek` = pmin(D_ANT010_05_NA / C_ANT010, 1),
    `Battle Creek` = 0,
    `Bear Creek` = 0,
    `Big Chico Creek` = 0,
    `Butte Creek` = pmin((D_BTC045_ESL008 + D_BTC043_10_NA + D_BTC036_10_NA + D_BTC012_CRK005) / (D_BTC045_ESL008 + D_BTC043_10_NA + D_BTC036_10_NA + D_BTC012_CRK005 + C_BTC012), 1),
    `Clear Creek` = 0,
    `Cottonwood Creek` = 0,
    `Cow Creek` = 0,
    `Deer Creek` = pmin((D_DRC010_05_NA + D_DRC005_05_NA) / C_DRC005, 1),
    `Elder Creek` = pmin(D_ELD012_04_NA / C_ELD005, 1),
    `Mill Creek` = pmin(D_MLC006_05_NA / C_MLC004, 1),
    `Paynes Creek` = 0,
    `Stony Creek` = pmin(D_STN021_06_PA / C_STN026, 1),
    `Thomes Creek` = pmin(D_THM012_04_NA / C_THM005, 1),
    `Upper-mid Sacramento River` = pmin((D_SAC240_TCC001 + D_SAC240_05_NA + D_SAC224_04_NA + D_SAC196_MTC000 + D_SAC185_08N_NA + D_SAC185_09_NA + D_SAC178_08N_SA1 + D_SAC162_09_SA2 + D_SAC159_08S_SA1 + D_SAC159_08N_SA1 + D_SAC146_08S_NA1 + D_SAC136_18_NA + D_SAC136_18_SA + D_SAC129_08S_NA2 + D_SAC122_19_SA) / C_SAC247, 1),
    `Sutter Bypass` = 0,
    `Bear River` = pmin(D_BRR017_23_NA / (D_BRR017_23_NA + C_CMPFW), 1),
    `Feather River` = pmin((D_THRMF_12_NU1 + D_THRMF_11_NU1 + D_THRMA_WEC000 + D_THRMA_RVC000 + D_THRMA_JBC000) / C_OROVL, 1),
    `Yuba River` = pmin(D_YUB011_15S_NA2 / (D_YUB011_15S_NA2 + C_YUB002), 1),
    `Lower-mid Sacramento River` = pmin((D_SAC121_08S_SA3 + D_SAC115_19_SA + D_SAC109_08S_SA3 + D_SAC109_19_SA + D_SAC099_19_SA + D_SAC091_19_SA + D_SAC083_21_SA + D_SAC082_22_SA1 + D_SAC081_21_NA + D_SAC078_22_SA1 + D_SAC075_22_NA + D_SAC074_21_SA + D_SAC065_WTPBTB) / C_SAC120, 1),
    `Yolo Bypass` = 0,
    `American River` = pmin(D_AMR007_WTPFBN / C_NTOMA, 1),
    `Lower Sacramento River` = pmin((D_SAC050_FPT013 + D_SAC062_WTPSAC) / C_SAC120, 1),
    `Calaveras River` = pmin((D_LJC022_60S_PA1 + D_CLV037_CACWD + D_CLV026_60S_PA1 + D_CLV026_WTPWDH) / C_NHGAN, 1),
    `Cosumnes River` = 0,
    `Mokelumne River` = pmin((D_MOK050_60N_NA3 + D_MOK050_60N_NA5 + D_MOK039_60N_NA5 + D_MOK035_60N_NA4 + D_MOK035_60N_NU1 + D_MOK035_WTPDWS + D_MOK033_60N_NA5) / C_CMCHE, 1),
    `Merced River` = pmin(D_MCD021_63_NA4 / C_MCD050, 1),
    `Stanislaus River` = pmin((D_STS030_61_NA4 + D_STS004_61_NA6) / C_STS059, 1),
    `Tuolumne River` = pmin((D_TUO047_61_NA3 + D_TUO047_62_NA4 + D_TUO015_61_NA3 + D_TUO015_62_NA4) / C_TUO054, 1),
    `San Joaquin River` = pmin((D_SJR062_50_PA1 + D_SJR090_71_NA2 + D_SJR081_61_NA5 + D_SJR116_72_NA1) / (D_SJR062_50_PA1 + D_SJR090_71_NA2 + D_SJR081_61_NA5 + D_SJR116_72_NA1 + C_SJR072), 1)
  ) |>
  mutate(across(-date, ~case_when(
    is.infinite(.) ~ 0,
    is.nan(.) ~ 0,
    TRUE ~ .
  ))) |>
  pivot_longer(-date, names_to = "watershed", values_to = "prop_div") |>
  pivot_wider(names_from = date, values_from = prop_div) |>
  left_join(DSMflow::watershed_ordering) |>
  select(-watershed) |>
  mutate(across(everything(), ~replace_na(., 0))) |>
  arrange(order) |>
  select(-order) |>
  DSMflow::create_model_array()

dimnames(proportion_diverted) <- list(DSMflow::watershed_ordering$watershed,
                                      month.abb[1:12],
                                      1980:2000)


# Bypass Proportion flows ----------------------------------------------------


# Bypass overtopped ----------------------------------------------------------




