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


delta_total_diverted <- array(dim=c(12, 21, 2), dimnames = list(month.abb, 1980:2000, c("North Delta", "South Delta")))
delta_total_diverted[,,1] <- north_delta_diversions
delta_total_diverted[,,2] <- south_delta_diversions


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
  distinct(datetime, node, values) |>
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


# Total Diverted -------------------------------------------------------------
total_diverted <- prop_div_wide |>
  transmute(
    date = datetime,
    `Upper Sacramento River` = (D_SAC296_WTPFTH + D_SAC296_02_SA + D_SAC294_WTPBLV + D_SAC294_03_PA + D_SAC281_02_NA + D_SAC273_03_NA),
    `Antelope Creek` = D_ANT010_05_NA,
    `Battle Creek` = 0,
    `Bear Creek` = 0,
    `Big Chico Creek` = 0,
    `Butte Creek` = (D_BTC045_ESL008 + D_BTC043_10_NA + D_BTC036_10_NA + D_BTC012_CRK005),
    `Clear Creek` = 0,
    `Cottonwood Creek` = 0,
    `Cow Creek` = 0,
    `Deer Creek` = (D_DRC010_05_NA + D_DRC005_05_NA),
    `Elder Creek` = D_ELD012_04_NA,
    `Mill Creek` = D_MLC006_05_NA,
    `Paynes Creek` = 0,
    `Stony Creek` = D_STN021_06_PA,
    `Thomes Creek` = D_THM012_04_NA,
    `Upper-mid Sacramento River` = (D_SAC240_TCC001 + D_SAC240_05_NA + D_SAC224_04_NA + D_SAC196_MTC000 + D_SAC185_08N_NA + D_SAC185_09_NA + D_SAC178_08N_SA1 + D_SAC162_09_SA2 + D_SAC159_08S_SA1 + D_SAC159_08N_SA1 + D_SAC146_08S_NA1 + D_SAC136_18_NA + D_SAC136_18_SA + D_SAC129_08S_NA2 + D_SAC122_19_SA),
    `Sutter Bypass` = 0,
    `Bear River` = D_BRR017_23_NA,
    `Feather River` = (D_THRMF_12_NU1 + D_THRMF_11_NU1 + D_THRMA_WEC000 + D_THRMA_RVC000 + D_THRMA_JBC000),
    `Yuba River` = D_YUB011_15S_NA2,
    `Lower-mid Sacramento River` = (D_SAC121_08S_SA3 + D_SAC115_19_SA + D_SAC109_08S_SA3 + D_SAC109_19_SA + D_SAC099_19_SA + D_SAC091_19_SA + D_SAC083_21_SA + D_SAC082_22_SA1 + D_SAC081_21_NA + D_SAC078_22_SA1 + D_SAC075_22_NA + D_SAC074_21_SA + D_SAC065_WTPBTB),
    `Yolo Bypass` = 0,
    `American River` = D_AMR007_WTPFBN,
    `Lower Sacramento River` = (D_SAC050_FPT013 + D_SAC062_WTPSAC),
    `Calaveras River` = (D_LJC022_60S_PA1 + D_CLV037_CACWD + D_CLV026_60S_PA1 + D_CLV026_WTPWDH),
    `Cosumnes River` = 0,
    `Mokelumne River` = (D_MOK050_60N_NA3 + D_MOK050_60N_NA5 + D_MOK039_60N_NA5 + D_MOK035_60N_NA4 + D_MOK035_60N_NU1 + D_MOK035_WTPDWS + D_MOK033_60N_NA5),
    `Merced River` = D_MCD021_63_NA4,
    `Stanislaus River` = (D_STS030_61_NA4 + D_STS004_61_NA6),
    `Tuolumne River` = (D_TUO047_61_NA3 + D_TUO047_62_NA4 + D_TUO015_61_NA3 + D_TUO015_62_NA4),
    `San Joaquin River` = (D_SJR062_50_PA1 + D_SJR090_71_NA2 + D_SJR081_61_NA5 + D_SJR116_72_NA1)
  ) |>
  pivot_longer(-date, names_to = "watershed", values_to = "tot_diver") |>
  mutate(tot_diver = DSMflow::cfs_to_cms(tot_diver)) |>
  pivot_wider(names_from = date, values_from = tot_diver) |>
  left_join(DSMflow::watershed_ordering) |>
  select(-watershed) |>
  mutate(across(everything(), ~replace_na(., 0))) |>
  arrange(order) |>
  select(-order) |>
  DSMflow::create_model_array()

dimnames(total_diverted) <- list(DSMflow::watershed_ordering$watershed,
                                 month.abb[1:12],
                                 1980:2000)


# Mean Flow ------------------------------------------------------------------


# San Joaquin Flows ----------------------------------------------------------
san_joaquin_flows <- avg_flows |>
  filter(year(date) %in% 1980:2000) |>
  transmute(year = year(date), month = month(date),
            sjQcms = DSMflow::cfs_to_cms(`San Joaquin River`)) |>
  pivot_wider(names_from = year, values_from = sjQcms) |>
  select(-month) |>
  as.matrix()
row.names(san_joaquin_flows) <- month.abb


# Proportion Flow Natal ------------------------------------------------------
# proportion of flow at tributary junction from natal watershed, October only
watersheds <- DSMflow::watershed_ordering$watershed

tributary_junctions <- c(rep(watersheds[16], 16), NA, watersheds[19], watersheds[21], watersheds[19],
                         watersheds[21], NA, rep(watersheds[24], 2), watersheds[25:27], rep(watersheds[31], 4))
names(tributary_junctions) <- watersheds

denominator <- avg_flows |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow") |>
  filter(month(date) == 10, watershed %in% unique(tributary_junctions)) |>
  rename(denominator = watershed, junction_flow = flow)

proportion_flow_natal <- avg_flows |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow") |>
  filter(month(date) == 10) |>
  mutate(denominator = tributary_junctions[watershed]) |>
  left_join(denominator) |>
  mutate(retQ = ifelse(flow / junction_flow > 1, 1, flow / junction_flow),
         retQ = replace(retQ, watershed %in% c('Calaveras River', 'Cosumnes River', 'Mokelumne River'), 1)) |>
  select(watershed, date, retQ) |>
  mutate(year = year(date)) |>
  filter(year >= 1979, year <= 2000) |>
  select(watershed, year, retQ) |>
  bind_rows(tibble(
    year = 1979,
    watershed = c('Yolo Bypass', 'Sutter Bypass'),
    retQ = 0
  )) |>
  left_join(DSMflow::watershed_ordering) |>
  arrange(order) |>
  pivot_wider(names_from = year, values_from = retQ) |>
  select(-watershed, -order) |>
  mutate(across(everything(), ~replace_na(., 0))) |>
  as.matrix()

rownames(proportion_flow_natal) <- watersheds


# Proportion Pulse Flows -----------------------------------------------------
proportion_pulse_flows <- avg_flows |>
  filter(between(year(date), 1980, 1999)) |>
  pivot_longer(-date, names_to = "watershed", values_to = "flow") |>
  group_by(month = month(date), watershed) |>
  summarise(prop_pulse = sd(flow) / median(flow) / 100) |>
  mutate(prop_pulse = replace(prop_pulse, is.infinite(prop_pulse), 0)) |>
  bind_rows(tibble(
    month = rep(1:12, 2),
    watershed = rep(c('Yolo Bypass', 'Sutter Bypass'), each = 12),
    prop_pulse = 0
  )) |>
  pivot_wider(names_from = month, values_from = prop_pulse) |>
  left_join(DSMflow::watershed_ordering) |>
  arrange(order) |>
  select(-order, -watershed) |>
  as.matrix()

proportion_pulse_flows[is.na(proportion_pulse_flows)] <- 0
colnames(proportion_pulse_flows) <- month.abb[1:12]
rownames(proportion_pulse_flows) <- DSMflow::watershed_ordering$watershed


# Delta Cross Channel Closed -------------------------------------------------
# TODO: need policy guidance for action 5 DCC operations

# Delta Flows ----------------------------------------------------------------
delta_inflows_cfs <- raw_data |>
  filter(dataset == "delta_inflows", year(datetime) %in% 1980:2000) |>
  group_by(region, datetime) |>
  summarise(values = sum(values)) |>
  ungroup() |>
  pivot_wider(names_from = region, values_from = values) |>
  rename(date = datetime, n_dlt_inflow_cfs = north_delta, s_dlt_inflow_cfs = south_delta)

delta_div_cfs <- raw_data |>
  filter(dataset == "delta_diversions_total", year(datetime) %in% 1980:2000) |>
  group_by(region, datetime) |>
  summarise(values = sum(values)) |>
  ungroup() |>
  pivot_wider(names_from = region, values_from = values) |>
  rename(date = datetime, n_dlt_div_cfs = north_delta, s_dlt_div_cfs = south_delta)

delta_flows <- delta_inflows_cfs |>
  left_join(delta_div_cfs) |>
  mutate(
    n_dlt_prop_div = n_dlt_div_cfs / n_dlt_inflow_cfs,
    s_dlt_prop_div = pmin(s_dlt_div_cfs / s_dlt_inflow_cfs, 1)
  )

# Delta Proportion Diverted --------------------------------------------------
delta_prop_div <- delta_flows |>
  filter(year(date) %in% 1980:2000) |>
  select(date, n_dlt_prop_div, s_dlt_prop_div) |>
  pivot_longer(n_dlt_prop_div:s_dlt_prop_div,
               names_to = "delta", values_to = "prop_div") |>
  pivot_wider(names_from = date, values_from = prop_div) |>
  select(-delta)

delta_proportion_diverted <- array(NA, dim = c(12, 21, 2))
delta_proportion_diverted[, , 1] <- as.matrix(delta_prop_div[1, ], nrow = 12, ncol = 21)
delta_proportion_diverted[, , 2] <- as.matrix(delta_prop_div[2, ], nrow = 12, ncol = 21)

dimnames(delta_proportion_diverted) <- list(month.abb[1:12],
                                            1980:2000,
                                            c('North Delta', 'South Delta'))


# Bypass Flows ---------------------------------------------------------------
# TODO: skipped for now, need C_BTC003 and C_SBP024 nodes for full sutter1-4


# Proportion Flow Bypasses ---------------------------------------------------
# Sutter: (SP_SAC193_BTC003 + SP_SAC188_BTC003 + SP_SAC178_BTC003) / C_SAC195
# Yolo: SP_SAC083_YBP037 / (SP_SAC083_YBP037 + C_SAC048)
propq_nodes <- c("SP_SAC193_BTC003", "SP_SAC188_BTC003", "SP_SAC178_BTC003",
                 "C_SAC195", "SP_SAC083_YBP037", "C_SAC048")

propq_wide <- raw_data |>
  filter(node %in% propq_nodes, year(datetime) %in% 1980:2000) |>
  select(datetime, node, values) |>
  distinct(datetime, node, values) |>
  pivot_wider(names_from = node, values_from = values)

bypass_prop_flow <- propq_wide |>
  transmute(
    year = year(datetime),
    month = month(datetime),
    sutter = pmin((SP_SAC193_BTC003 + SP_SAC188_BTC003 + SP_SAC178_BTC003) / C_SAC195, 1),
    yolo = pmin(SP_SAC083_YBP037 / (SP_SAC083_YBP037 + C_SAC048), 1)
  ) |>
  pivot_longer(sutter:yolo, names_to = "bypass", values_to = "prop_flow") |>
  pivot_wider(names_from = year, values_from = prop_flow) |>
  arrange(bypass, month) |>
  select(-month, -bypass) |>
  as.matrix()

proportion_flow_bypasses <- array(NA, dim = c(12, 21, 2))
proportion_flow_bypasses[, , 1] <- bypass_prop_flow[1:12, ]
proportion_flow_bypasses[, , 2] <- bypass_prop_flow[13:24, ]

dimnames(proportion_flow_bypasses) <- list(month.abb[1:12],
                                           1980:2000,
                                           c('Sutter Bypass', 'Yolo Bypass'))


# Gates Overtopped -----------------------------------------------------------
# Sutter: TRUE if (SP_SAC193 + SP_SAC188 + SP_SAC178 + SP_SAC159 + SP_SAC148 + SP_SAC122 + C_SSL001) >= 100
# Yolo: TRUE if (SP_SAC083_YBP037 + C_CSL005) >= 100
overtopped_nodes <- c("SP_SAC193_BTC003", "SP_SAC188_BTC003", "SP_SAC178_BTC003",
                      "SP_SAC159_BTC003", "SP_SAC148_BTC003", "SP_SAC122_SBP021",
                      "C_SSL001", "SP_SAC083_YBP037", "C_CSL005")

overtopped_wide <- raw_data |>
  filter(node %in% overtopped_nodes, year(datetime) %in% 1980:2000) |>
  select(datetime, node, values) |>
  distinct(datetime, node, values) |>
  pivot_wider(names_from = node, values_from = values)

bypass_overtopped <- overtopped_wide |>
  transmute(
    year = year(datetime),
    month = month(datetime),
    sutter = (SP_SAC193_BTC003 + SP_SAC188_BTC003 + SP_SAC178_BTC003 +
                SP_SAC159_BTC003 + SP_SAC148_BTC003 + SP_SAC122_SBP021 + C_SSL001) >= 100,
    yolo = (SP_SAC083_YBP037 + C_CSL005) >= 100
  ) |>
  pivot_longer(sutter:yolo, names_to = "bypass", values_to = "overtopped") |>
  pivot_wider(names_from = year, values_from = overtopped) |>
  arrange(bypass, month) |>
  select(-month, -bypass) |>
  as.matrix()

gates_overtopped <- array(NA, dim = c(12, 21, 2))
gates_overtopped[, , 1] <- bypass_overtopped[1:12, ]
gates_overtopped[, , 2] <- bypass_overtopped[13:24, ]

dimnames(gates_overtopped) <- list(month.abb[1:12],
                                   1980:2000,
                                   c('Sutter Bypass', 'Yolo Bypass'))


# Wilkins Flow ---------------------------------------------------------------
# TODO: needs C_SAC129 node added to CSV


# Pack into action_5 list ----------------------------------------------------
action_5$flows_cfs <- avg_flows
action_5$upper_sacramento_flows <- upper_sacramento_flows
action_5$freeport_flow <- freeport_flows
action_5$vernalis_flow <- vernalis_flows
action_5$stockton_flow <- stockton_flows
action_5$cvp_exports <- cvp_realized
action_5$swp_exports <- swp_realized
action_5$proportion_diverted <- proportion_diverted
action_5$total_diverted <- total_diverted
action_5$san_joaquin_flows <- san_joaquin_flows
action_5$proportion_flow_natal <- proportion_flow_natal
action_5$proportion_pulse_flows <- proportion_pulse_flows
action_5$delta_flows <- delta_flows
action_5$delta_proportion_diverted <- delta_proportion_diverted
action_5$delta_inflow <- list(north = north_delta_inflows, south = south_delta_inflows)
action_5$delta_total_diverted <- list(north = north_delta_diversions, south = south_delta_diversions)
action_5$proportion_flow_bypasses <- proportion_flow_bypasses
action_5$gates_overtopped <- gates_overtopped
# action_5$delta_cross_channel_closed <- TODO
# action_5$bypass_flows <- TODO
# action_5$wilkins_flow <- TODO
# action_5$mean_flow <- TODO

readr::write_rds(action_5, "data-raw/calsim_3_action_5/action_5.rds")


