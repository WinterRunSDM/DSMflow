library(dplyr)
library(tidyr)
library(readr)
library(lubridate)
library(stringr)
library(readxl)
library(purrr)
library(DSMflow)
library(tibble)

source('R/utils.R')

# watershed ordering --------
watershed_ordering <- read_csv('data-raw/watershed_ordering.csv')
usethis::use_data(watershed_ordering, overwrite = TRUE)


# dry years for model timeframe -------------------------------------------
dry_years <- waterYearType::water_year_indices |>
  filter(location == "Sacramento Valley") |>
  rename(water_year = WY) |>
  mutate(year_type = ifelse(Yr_type %in% c("Wet", "Above Normal"), "wet", "dry")) |>
  filter(#water_year %in% 1980:2000,
         water_year %in% 1921:2002,
         year_type == "dry") |>
  pull(water_year)

# calsim prep (all versions) ---------------------------------------------------
cvpia_nodes <- read_csv('data-raw/calsim_2008_2009/MikeWrightCalSimOct2017/cvpia_calsim_nodes.csv', skip = 1)
watersheds <- cvpia_nodes$watershed

process_dss_output <- function(file) {
  raw_data <- readxl::read_excel(file)
  raw_data |>
    pivot_longer(-c(B, date), values_to = "flow_cfs", names_to = "node") |>
    mutate(
      date = date - years(
        case_when(
          year(date) >= 2022 ~ 100,
          year(date) == 2021 & month(date) %in% 10:12 ~ 100,
          TRUE ~ 0
        )
      )
    ) |>
    filter(!is.na(flow_cfs))
}

calsim3_data <- map_df(list.files("data-raw/calsim3/", pattern = ".xlsx", full.names = TRUE), process_dss_output)

# LTO action 5 scenario
action_5 <- readr::read_rds("data-raw/calsim_3_action_5/action_5.rds")


need_split_habitat <- cvpia_nodes$calsim_habitat_flow |> str_detect(', ')
habitat_split <- cvpia_nodes$calsim_habitat_flow[need_split_habitat] |> str_split(', ') |> flatten_chr()
habitat_node <- c(cvpia_nodes$calsim_habitat_flow[!need_split_habitat], habitat_split, 'C134', 'C160')[-20]

# Flow Function ----------------------------------------------------------------
generate_flow_cfs <- function(calsim_data, nodes){
  node_columns <- names(calsim_data) %in% c(nodes, 'date')

  flow_calsim <- calsim_data[, node_columns]

  flow <- flow_calsim |>
    mutate(`Upper Sacramento River` = C104,
           `Antelope Creek` = C11307,
           `Battle Creek` = C10803,
           `Bear Creek` = C11001,
           `Big Chico Creek` = C11501,
           `Butte Creek` = C217A,
           `Clear Creek` = C3,
           `Cottonwood Creek` = C10802,
           `Cow Creek` = C10801,
           `Deer Creek` = C11309,
           `Elder Creek` = C11303,
           `Mill Creek` = C11308,
           `Paynes Creek` = C11001,
           `Stony Creek` = C142A,
           `Thomes Creek` = C11304,
           `Upper-mid Sacramento River` = C115,
           `Bear River` = C285,
           `Feather River` = C203,
           `Yuba River` = C230,
           `Lower-mid Sacramento River1` = C134, # low-mid habitat = 35.6/58*habitat(C134) + 22.4/58*habitat(C160),
           `Lower-mid Sacramento River2` = C160,
           `American River` = C9,
           `Lower Sacramento River` = C166,
           `Calaveras River` = C92,
           `Cosumnes River` = C501,
           `Mokelumne River` = NA,
           `Merced River` = C561,
           `Stanislaus River` = C520,
           `Tuolumne River` = C540,
           `San Joaquin River` = C630) |>
    select(date, `Upper Sacramento River`:`San Joaquin River`)
  return(flow)
}

# Original (2008 - 2009 BiOp) ------------------------------------------------
calsim_2008_2009 <- read_rds('data-raw/calsim_2008_2009/MikeWrightCalSimOct2017/cvpia_calsim.rds') |>
  rename(D403D = D403D.x) |>
  select(-D403D.y)


# Generate 2008 2009 flow_cfs --------------------------------------------------
flow_2008_2009 <- generate_flow_cfs(calsim_data = calsim_2008_2009, nodes = habitat_node)

# bring in Moke flow from other model run
moke <- readxl::read_excel('data-raw/calsim_2008_2009/EBMUDSIM/CVPIA_SIT_Data_RequestEBMUDSIMOutput_ExCond.xlsx',
                   sheet = 'Tableau Clean-up') |>
  mutate(date = as_date(Date), `Mokelumne River` = C91) |>
  select(date, `Mokelumne River`)

# join moke to flows
flows_cfs_2008_2009 <- flow_2008_2009 |>
  select(-`Mokelumne River`) |> # get rid of old Moke column
  left_join(moke) |> # add in Moke
  select(date:`Cosumnes River`, `Mokelumne River`, `Merced River`:`San Joaquin River`) # reorder

# Add in new calsim run (2018 - 2019 Biop/Itp) data-----------------------------
calsim_2019_biop_itp <- read_rds('data-raw/calsim_2019_BiOp_ITP/biop_cvpia_calsim.rds')

moke_2019 <- readxl::read_excel('data-raw/calsim_2019_BiOp_ITP/EBMUDSIM/CVPIA_SIT_Data_RequestUpdated2022.xlsx',
                   sheet = 'Tableau Clean-up') |>
  mutate(date = as_date(Date), `Mokelumne River` = C91) |>
  select(date, `Mokelumne River`)

flow_cfs_2019_biop_itp <- generate_flow_cfs(calsim_data = calsim_2019_biop_itp, nodes = habitat_node)

flow_cfs_2019_biop_itp <- flow_cfs_2019_biop_itp |>
  select(-`Mokelumne River`) |> # get rid of old Moke column
  left_join(moke_2019) |> # add in Moke
  select(date:`Cosumnes River`, `Mokelumne River`, `Merced River`:`San Joaquin River`) # reorder

# EFF run
# Sacramento
# Sometime flow change to eff on upper sac leads to negative flows downstream, set these to bottom out at 100 CFS
# includes San Joaquin now
load("data/synthetic_eff_sac.rda")
load("data/synthetic_eff_sj.rda")
eff_2019_biop_elsewhere <- flow_cfs_2019_biop_itp |>
  left_join(synthetic_eff_sac) |>
  left_join(synthetic_eff_sj) |>
  # sacramento
  mutate(flow_change = `Upper Sacramento River EFF` - `Upper Sacramento River`,
         `Upper Sacramento River` = `Upper Sacramento River EFF`,
         `Upper-mid Sacramento River` = ifelse(`Upper-mid Sacramento River` + flow_change < 0, 100, `Upper-mid Sacramento River` + flow_change),
         `Lower-mid Sacramento River1` = ifelse(`Lower-mid Sacramento River1` + flow_change < 0, 100, `Lower-mid Sacramento River1` + flow_change),
         `Lower-mid Sacramento River2` = ifelse(`Lower-mid Sacramento River2` + flow_change < 0, 100, `Lower-mid Sacramento River2` + flow_change),
         `Lower Sacramento River` = ifelse(`Lower Sacramento River` + flow_change < 0, 100, `Lower Sacramento River` + flow_change)) |>
  mutate(`San Joaquin River` = `Lower San Joaquin EFF`) |>
  select(-c(`Upper Sacramento River EFF`, flow_change, `Lower San Joaquin EFF`))

View(eff_2019_biop_elsewhere)

# Add run of river
calsim_run_of_river <- read_rds('data-raw/calsim_run_of_river/run_of_river_r2r_calsim.rds')

# TODO: temporarily just use 2019 data for Moke in run of river
moke_2019 <- read_excel('data-raw/calsim_2019_BiOp_ITP/EBMUDSIM/CVPIA_SIT_Data_RequestUpdated2022.xlsx',
                        sheet = 'Tableau Clean-up') |>
  mutate(date = as_date(Date), `Mokelumne River` = C91) |>
  select(date, `Mokelumne River`)

flow_cfs_run_of_river <- generate_flow_cfs(calsim_data = calsim_run_of_river, nodes = habitat_node)

flow_cfs_run_of_river <- flow_cfs_run_of_river |>
  select(-`Mokelumne River`) |> # get rid of old Moke column
  left_join(moke_2019) |> # #TODO: update with updated Moke data
  select(date:`Cosumnes River`, `Mokelumne River`, `Merced River`:`San Joaquin River`) # reorder

# Calsim 3
watershed_to_nodes <- c(`Upper Sacramento River` = "C_SAC273", `Antelope Creek` = "C_ANT010",
                        `Battle Creek` = "C_BTL006", `Bear Creek` = "C_BCN005", `Big Chico Creek` = "C_BCC004",
                        `Butte Creek` = "C_BTC012", `Clear Creek` = "C_CLR009", `Cottonwood Creek` = "C_CWD003",
                        `Cow Creek` = "C_COW003", `Deer Creek` = "C_DRC005", `Elder Creek` = "C_ELD005",
                        `Mill Creek` = "C_MLC004", `Paynes Creek` = "C_PYN001", `Stony Creek` = "C_STN004",
                        `Thomes Creek` = "C_THM005", `Upper-mid Sacramento River` = "C_SAC193",
                        `Bear River` = "C_CMPFW", `Feather River` = "C_FTR059", `Yuba River` = "C_YUB002",
                        `Lower-mid Sacramento River1` = "C_SAC093", `Lower-mid Sacramento River2` = "C_SAC048",
                        `American River` = "C_NTOMA", `Lower Sacramento River` = "C_SAC063",
                        `Calaveras River` = "C_NHGAN", `Cosumnes River` = "C_CSM005",
                        `Mokelumne River` = "C_CMCHE", `Merced River` = "C_MCD050", `Stanislaus River` = "C_STS059",
                        `Tuolumne River` = "C_TUO054", `San Joaquin River` = "C_SJR081"
)

nodes_to_watershed <- names(watershed_to_nodes)
names(nodes_to_watershed) <- as.character(watershed_to_nodes)


lto_calsim3_flows <- calsim3_data |> filter(node %in% watershed_to_nodes) |>
  mutate(watershed = nodes_to_watershed[node]) |>
  select(-B, -node) |>
  pivot_wider(names_from = "watershed", values_from = "flow_cfs") |>
  mutate(date = as_date(date))

# combine eff in dry years to HRL (lto) flows
LTO_12a_eff_dy <- bind_rows(eff_2019_biop_elsewhere |>
                              filter(year(date) %in% dry_years),
                            lto_calsim3_flows |>
                              filter(!year(date) %in% dry_years)) |>
  arrange(date)

# create flow_cfs with both 2008-2009 biop and 2018-2019 biop/itp and run of river ---------------
flows_cfs <- list(biop_2008_2009 = flows_cfs_2008_2009,
                  biop_itp_2018_2019 = flow_cfs_2019_biop_itp,
                  run_of_river = flow_cfs_run_of_river,
                  eff = eff_2019_biop_elsewhere,
                  LTO_12a = lto_calsim3_flows,
                  LTO_12a_eff_dy = LTO_12a_eff_dy
)
# LTO action 5 scenario
flows_cfs$action_5 <- action_5$flows_cfs

# Write flow cfs data object
usethis::use_data(flows_cfs, overwrite = TRUE)

# bypasses habitat flow --------------------------------------------------------
generate_bypass_flows <- function(calsim_run) {
  bypass_flows <- calsim_run |>
    select(date,
           sutter1 = D117,
           sutter2 = C135,
           sutter3 = C136A,
           sutter4 = C137,
           yolo1 = D160,
           yolo2 = C157) |>
    mutate(sutter2 = sutter2 + sutter1,
           sutter3 = sutter3 + sutter2,
           sutter4 = sutter4 + sutter3,
           yolo2 = yolo2 + yolo1)
  return(bypass_flows)
}

bypass_2008_2009 <-  generate_bypass_flows(calsim_run = calsim_2008_2009)
bypass_2019_biop_itp <- generate_bypass_flows(calsim_run = calsim_2019_biop_itp)
run_of_river <-  generate_bypass_flows(calsim_run = calsim_run_of_river)

# calsim 3
calsim3_bypass_nodes <- data.frame(inputs=c("sutter1",
                                "sutter1",
                                "sutter1",
                                "sutter2",
                                "sutter3",
                                "sutter4",
                                "yolo1",
                                "yolo2"),
                       nodes=c("SP_SAC193_BTC003",
                               "SP_SAC188_BTC003",
                               "SP_SAC178_BTC003",
                               "C_BTC003",
                               "C_SBP024",
                               "C_SSL001",
                               "SP_SAC083_YBP037",
                               "C_CSL005"),
                       type=c(rep("RIVER-SPILLS",3),rep("CHANNEL",3),
                              "RIVER-SPILLS","CHANNEL"))

bypass_nodes <- calsim3_bypass_nodes$inputs
names(bypass_nodes) <- calsim3_bypass_nodes$nodes

lto_calsim3_bypass_flows <- calsim3_data |> filter(node %in% names(bypass_nodes)) |>
  mutate(bypass = bypass_nodes[node]) |>
  select(-B, -node) |>
  group_by(date, bypass) |>
  summarise(
    flow_cfs = sum(flow_cfs)
  ) |>
  ungroup() |>
  pivot_wider(names_from = bypass, values_from = flow_cfs) |>
  mutate(date = as_date(date))


# create bypass flows with both 2008-2009 biop and 2018-2019 biop/itp
bypass_flows <- list(biop_2008_2009 = bypass_2008_2009,
                     biop_itp_2018_2019 = bypass_2019_biop_itp,
                     run_of_river = run_of_river,
                     LTO_12a = lto_calsim3_bypass_flows
)

assertthat::are_equal(names(run_of_river), names(lto_calsim3_bypass_flows))
# LTO action 5 scenario
bypass_flows$action_5 <- action_5$bypass_flows

usethis::use_data(bypass_flows, overwrite = TRUE)

# diversions -------------------------------------------------------------------
need_split <- cvpia_nodes$cal_sim_flow_nodes |> str_detect(', ')
div_split <- cvpia_nodes$cal_sim_flow_nodes[need_split] |> str_split(', ') |> flatten_chr()
div_flow_nodes <- c(cvpia_nodes$cal_sim_flow_nodes[!need_split], div_split)

need_split <- cvpia_nodes$cal_sim_diversion_nodes |> str_detect(', ')
div_split <- cvpia_nodes$cal_sim_diversion_nodes[need_split] |> str_split(', ') |> flatten_chr()
div_nodes <- c(cvpia_nodes$cal_sim_diversion_nodes[!need_split], div_split)
diversion_nodes <- div_nodes[!is.na(div_nodes)] |> str_trim('both') |> str_replace(',', '')

combined_flow_nodes <- c('C11305', 'C11301')

all_div_nodes <- c(div_flow_nodes, diversion_nodes, combined_flow_nodes, 'date') |> unique()
all_div_nodes


# diversion function ------------------------------------------------------
generate_diversion_total <- function(calsim_data, nodes){

  node_columns <- names(calsim_data) %in% c(nodes, 'date')

  div_calsim <- calsim_data[, node_columns]

  div_tot <- div_calsim |>
    mutate(`Upper Sacramento River` = D104,
           `Antelope Creek` = ifelse(C11307 == 0, 0, (C11307 / (C11307 + C11308 + C11309) * D11305)),
           `Battle Creek` = NA,
           `Bear Creek` = NA,
           `Big Chico Creek` = NA,
           `Butte Creek` = (C217B + D217),
           `Clear Creek` = NA,
           `Cottonwood Creek` = NA,
           `Cow Creek` = NA,
           `Deer Creek` = ifelse(C11309 == 0 ,0, (C11309 / (C11307 + C11308 + C11309) * D11305)),
           `Elder Creek` = ifelse(C11303 == 0, 0, (C11303 / (C11303 + C11304) * D11301)),
           `Mill Creek` = ifelse(C11308 == 0, 0, (C11308 / (C11307 + C11308 + C11309) * D11305)),
           `Paynes Creek` = NA,
           `Stony Creek` = D17301,
           `Thomes Creek` = ifelse(C11304 == 0, 0, (C11304 / (C11303 + C11304) * D11301)),
           `Upper-mid Sacramento River` = (D109 + D112 + D113A + D113B + D114 + D118 + D122A + D122B
                                           + D123 + D124A + D128_WTS + D128),
           `Sutter Bypass` = NA,
           `Bear River` = D285,
           `Feather River` = (D201 + D202 + D7A + D7B),
           `Yuba River` = D230,
           `Lower-mid Sacramento River` = (D129A + D134 + D162 + D165),
           `Yolo Bypass` = NA,
           `American River` = D302,
           `Lower Sacramento River` = (D167 + D168 + D168A_WTS),
           `Calaveras River` = (D506A + D506B + D506C + D507),
           `Cosumnes River` = NA,
           `Mokelumne River` = NA, # other run from mike U ebmud
           `Merced River` = (D562 + D566),
           `Stanislaus River` = D528,
           `Tuolumne River` = D545,
           `San Joaquin River` = (D637 + D630B + D630A + D620B)) |>
    select(date, `Upper Sacramento River`:`San Joaquin River`)

  # turn into array for total_diversion
  total_diverted <- div_tot |>
    filter(year(date) >= 1980, year(date) <= 2000) |>
    pivot_longer(`Upper Sacramento River`:`San Joaquin River`,
                 names_to = "watershed",
                 values_to = "tot_diver") |>
    mutate(tot_diver = DSMflow::cfs_to_cms(tot_diver)) |>
    pivot_wider(names_from = date,
                values_from = tot_diver) |>
    left_join(DSMflow::watershed_ordering) |>
    select(-watershed) |>
    mutate_all( ~ replace_na(., 0)) |>
    arrange(order) |>
    select(-order) |>
    create_model_array()

  dimnames(total_diverted) <- list(watershed_ordering$watershed,
                                   month.abb[1:12],
                                   1980:2000)

  return(total_diverted)
}

# generate diversions for 2008 2009 --------------------------------------------
diversion_2008_2009 <-  generate_diversion_total(calsim_data = calsim_2008_2009,
                                                 nodes = all_div_nodes)
#bring in Moke diversions for 2008_2009 run from other model run
moke <- readxl::read_excel('data-raw/calsim_2008_2009/EBMUDSIM/CVPIA_SIT_Data_RequestEBMUDSIMOutput_ExCond.xlsx',
                   sheet = 'Tableau Clean-up') |>
  mutate(date = as_date(Date),
         `Mokelumne River` = (D503A + D503B + D503C + D502A + D502B)) |>
  select(date, `Mokelumne River`) |>
  filter(year(date) <= 2000 & year(date) >= 1980) |># subset to 20 years
  group_by(year(date), month(date)) |> # summarize by month
  summarize(monthly_flow = sum(`Mokelumne River`)) |>
  rename(year = `year(date)`,
         month = `month(date)`) |>
  mutate(monthly_flow = DSMflow::cfs_to_cms(monthly_flow)) |>
  pivot_wider(names_from = year, values_from = monthly_flow) |> # format to fit into model array
  select(-month)

diversion_2008_2009["Mokelumne River",,] <- as.matrix(moke) # add to 2008_2009 matrix

# generate diversions for 2019 biop --------------------------------------------
diversion_2019_biop_itp <- generate_diversion_total(calsim_data = calsim_2019_biop_itp,
                                                    nodes = all_div_nodes)

#bring in Moke diversions for 20019 run from other model run
moke_2019 <- readxl::read_excel('data-raw/calsim_2019_BiOp_ITP/EBMUDSIM/CVPIA_SIT_Data_RequestUpdated2022.xlsx',
                   sheet = 'Tableau Clean-up') |>
  mutate(date = as_date(Date),
         `Mokelumne River` = (D503A + D503B + D503C + D502A + D502B)) |>
  select(date, `Mokelumne River`) |>
  filter(year(date) <= 2000 & year(date) >= 1980) |># subset to 20 years
  group_by(year(date), month(date)) |> # summarize by month
  summarize(monthly_flow = sum(`Mokelumne River`)) |>
  rename(year = `year(date)`,
         month = `month(date)`) |>
  mutate(monthly_flow = DSMflow::cfs_to_cms(monthly_flow)) |>
  pivot_wider(names_from = year, values_from = monthly_flow) |> # format to fit into model array
  select(-month)

diversion_2019_biop_itp["Mokelumne River",,] <- as.matrix(moke_2019) # add to 2008_2009 matrix


# generate diversions for run of river ------------------------------------
diversion_run_of_river <- generate_diversion_total(calsim_data = calsim_run_of_river,
                                                   nodes = all_div_nodes)

# TODO: update when we have moke for run of river
# bring in Moke diversions for 20019 run from other model run
moke_2019_run_of_river <- readxl::read_excel('data-raw/calsim_2019_BiOp_ITP/EBMUDSIM/CVPIA_SIT_Data_RequestUpdated2022.xlsx',
                                sheet = 'Tableau Clean-up') |>
  mutate(date = as_date(Date),
         `Mokelumne River` = (D503A + D503B + D503C + D502A + D502B)) |>
  select(date, `Mokelumne River`) |>
  filter(year(date) <= 2000 & year(date) >= 1980) |># subset to 20 years
  group_by(year(date), month(date)) |> # summarize by month
  summarize(monthly_flow = sum(`Mokelumne River`)) |>
  rename(year = `year(date)`,
         month = `month(date)`) |>
  mutate(monthly_flow = DSMflow::cfs_to_cms(monthly_flow)) |>
  pivot_wider(names_from = year, values_from = monthly_flow) |> # format to fit into model array
  select(-month)

diversion_run_of_river["Mokelumne River",,] <- as.matrix(moke_2019_run_of_river)

# total diverted LTO12a
calsim3_diversion_nodes <- c("D_SAC296_WTPFTH", "D_SAC296_02_SA", "D_SAC294_WTPBLV", "D_SAC294_03_PA",
                             "D_SAC289_03_SA", "D_SAC281_02_NA", "D_SAC273_03_NA", "D_ANT010_05_NA",
                             "D_BTC045_ESL008", "D_BTC043_10_NA", "D_BTC036_10_NA", "D_BTC012_09_SA2",
                             "D_BTC012_CRK005", "D_DRC010_05_NA", "D_DRC005_05_NA", "D_ELD012_04_NA",
                             "D_MLC006_05_NA", "D_STN021_06_PA", "D_THM012_04_NA", "D_SAC240_TCC001",
                             "D_SAC240_05_NA", "D_SAC224_04_NA", "D_SAC207_GCC007", "D_SAC196_MTC000",
                             "D_SAC185_08N_NA", "D_SAC185_09_NA", "D_SAC178_08N_SA1", "D_SAC162_09_SA2",
                             "D_SAC159_08S_SA1", "D_SAC159_08N_SA1", "D_SAC146_08S_NA1", "D_SAC136_18_NA",
                             "D_SAC136_18_SA", "D_SAC129_08S_NA2", "D_SAC122_19_SA", "D_BRR017_23_NA",
                             "D_THRMF_12_NU1", "D_THRMF_11_NU1", "D_THRMA_WEC000", "D_THRMA_RVC000",
                             "D_THRMA_JBC000", "D_YUB011_15S_NA2", "D_SAC121_08S_SA3", "D_SAC115_19_SA",
                             "D_SAC109_08S_SA3", "D_SAC109_19_SA", "D_SAC099_19_SA", "D_SAC091_19_SA",
                             "D_SAC083_21_SA", "D_SAC082_22_SA1", "D_SAC081_21_NA", "D_SAC078_22_SA1",
                             "D_SAC075_22_NA", "D_SAC074_21_SA", "D_SAC065_WTPBTB", "D_AMR007_WTPFBN",
                             "D_SAC050_FPT013", "D_SAC062_WTPSAC", "D_LJC022_60S_PA1", "D_CLV037_CACWD",
                             "D_CLV026_60S_PA1", "D_CLV026_WTPWDH", "D_MOK050_60N_NA3", "D_MOK050_60N_NA5",
                             "D_MOK039_60N_NA5", "D_MOK035_60N_NA4", "D_MOK035_60N_NU1", "D_MOK035_WTPDWS",
                             "D_MOK033_60N_NA5", "D_MCD042_63_NA2", "D_MCD021_63_NA4", "D_STS030_61_NA4",
                             "D_STS004_61_NA6", "D_TUO047_61_NA3", "D_TUO047_62_NA4", "D_TUO015_61_NA3",
                             "D_TUO015_62_NA4", "D_SJR062_50_PA1", "D_SJR090_71_NA2", "D_SJR081_61_NA5",
                             "D_SJR116_72_NA1", "C_SAC273", "C_ANT010", "D_BTC045_ESL008",
                             "D_BTC043_10_NA", "D_BTC036_10_NA", "D_BTC012_09_SA2", "D_BTC012_CRK005",
                             "C_BTC012", "C_DRC005", "C_ELD005", "C_MLC004", "C_STN026", "C_THM005",
                             "C_SAC247", "C_CMPFW", "D_BRR017_23_NA", "C_OROVL", "C_YUB002",
                             "D_YUB011_15S_NA2", "C_SAC120", "C_NTOMA", "C_SAC063", "C_NHGAN",
                             "C_CMCHE", "C_MCD050", "C_STS059", "C_TUO054", "D_SJR062_50_PA1",
                             "D_SJR090_71_NA2", "D_SJR081_61_NA5", "D_SJR116_72_NA1", "C_SJR072"
)


calsim_diversions_wide <- calsim3_data |> filter(node %in% calsim3_diversion_nodes) |>
  select(-B) |>
  pivot_wider(names_from = node, values_from = flow_cfs)


lto_total_diverted <-
  calsim_diversions_wide |>
  mutate(
    `div_final_Upper Sacramento River` = sum(D_SAC296_WTPFTH, D_SAC296_02_SA, D_SAC294_WTPBLV, D_SAC294_03_PA, D_SAC289_03_SA, D_SAC281_02_NA, D_SAC273_03_NA),
    `div_final_Antelope Creek` = D_ANT010_05_NA,
    `div_final_Battle Creek` = 0,
    `div_final_Bear Creek` = 0,
    `div_final_Big Chico Creek` = 0,
    `div_final_Butte Creek` = (D_BTC045_ESL008 + D_BTC043_10_NA + D_BTC036_10_NA + D_BTC012_09_SA2 + D_BTC012_CRK005),
    `div_final_Clear Creek` = 0,
    `div_final_Cottonwood Creek` = 0,
    `div_final_Cow Creek` = 0,
    `div_final_Deer Creek` = (D_DRC010_05_NA + D_DRC005_05_NA),
    `div_final_Elder Creek` = D_ELD012_04_NA,
    `div_final_Mill Creek` = D_MLC006_05_NA,
    `div_final_Paynes Creek` = 0,
    `div_final_Stony Creek` = D_STN021_06_PA,
    `div_final_Thomes Creek` = D_THM012_04_NA,
    `div_final_Upper-mid Sacramento River` = (D_SAC240_TCC001 + D_SAC240_05_NA + D_SAC224_04_NA + D_SAC196_MTC000 + D_SAC185_08N_NA + D_SAC185_09_NA + D_SAC178_08N_SA1 + D_SAC162_09_SA2 + D_SAC159_08S_SA1 + D_SAC159_08N_SA1 + D_SAC146_08S_NA1 + D_SAC136_18_NA + D_SAC136_18_SA + D_SAC129_08S_NA2 + D_SAC122_19_SA),
    `div_final_Sutter Bypass` = NA_real_,
    `div_final_Bear River` = D_BRR017_23_NA,
    `div_final_Feather River` = (D_THRMF_12_NU1 + D_THRMF_11_NU1 + D_THRMA_WEC000 + D_THRMA_RVC000 + D_THRMA_JBC000),
    `div_final_Yuba River` = D_YUB011_15S_NA2,
    `div_final_Lower-mid Sacramento River` = (D_SAC121_08S_SA3 + D_SAC115_19_SA + D_SAC109_08S_SA3 + D_SAC109_19_SA + D_SAC099_19_SA + D_SAC091_19_SA + D_SAC083_21_SA + D_SAC082_22_SA1 + D_SAC081_21_NA + D_SAC078_22_SA1 + D_SAC075_22_NA + D_SAC074_21_SA + D_SAC065_WTPBTB),
    `div_final_Yolo Bypass` = NA_real_,
    `div_final_American River` = D_AMR007_WTPFBN,
    `div_final_Lower Sacramento River` = (D_SAC050_FPT013 + D_SAC062_WTPSAC),
    `div_final_Calaveras River` = (D_LJC022_60S_PA1 + D_CLV037_CACWD + D_CLV026_60S_PA1 + D_CLV026_WTPWDH),
    `div_final_Cosumnes River` = 0,
    `div_final_Mokelumne River` = (D_MOK050_60N_NA3 + D_MOK050_60N_NA5 + D_MOK039_60N_NA5 + D_MOK035_60N_NA4 + D_MOK035_60N_NU1 + D_MOK035_WTPDWS + D_MOK033_60N_NA5),
    `div_final_Merced River` = (D_MCD042_63_NA2 + D_MCD021_63_NA4),
    `div_final_Stanislaus River` = (D_STS030_61_NA4 + D_STS004_61_NA6),
    `div_final_Tuolumne River` = (D_TUO047_61_NA3 + D_TUO047_62_NA4 + D_TUO015_61_NA3 + D_TUO015_62_NA4),
    `div_final_San Joaquin River` = (D_SJR062_50_PA1 + D_SJR090_71_NA2 + D_SJR081_61_NA5 + D_SJR116_72_NA1)
  )


lto_total_diverted_final <- lto_total_diverted |>
  select(date, starts_with("div_final")) |>
  rename_with(\(x) str_replace(x, "div_final_", "")) |>
# turn into array for total_diversion
  filter(year(date) >= 1980, year(date) <= 2000) |>
  pivot_longer(`Upper Sacramento River`:`San Joaquin River`,
               names_to = "watershed",
               values_to = "tot_diver") |>
  mutate(tot_diver = DSMflow::cfs_to_cms(tot_diver)) |>
  pivot_wider(names_from = date,
              values_from = tot_diver) |>
  left_join(DSMflow::watershed_ordering) |>
  select(-watershed) |>
  mutate_all( ~ replace_na(., 0)) |>
  arrange(order) |>
  select(-order) |>
  create_model_array()

dimnames(lto_total_diverted_final) <- list(watershed_ordering$watershed,
                                           month.abb[1:12],
                                           1980:2000)



# create diversion flows with both 2008-2009 biop and 2018-2019 biop/itp
total_diverted <- list(biop_2008_2009 = diversion_2008_2009, # has moke
                     biop_itp_2018_2019 = diversion_2019_biop_itp, # missing moke
                     run_of_river = diversion_run_of_river,
                     LTO_12a = lto_total_diverted_final
)

assertthat::are_equal(dimnames(diversion_run_of_river), dimnames(lto_total_diverted_final))
# LTO action 5 scenario
total_diverted$action_5 <- action_5$total_diverted

usethis::use_data(total_diverted, overwrite = TRUE)


# proportion_diverted ----------------------------------------------------------
generate_proportion_diverted <- function(calsim_data, nodes) {

  node_columns <- names(calsim_data) %in% c(nodes, 'date')

  div_calsim <- calsim_data[, node_columns]

  temp_prop_diverted <- div_calsim |>
    mutate(`Upper Sacramento River` = D104 / C104,
           `Antelope Creek` = (C11307 / (C11307 + C11308 + C11309) * D11305) / C11307,
           `Battle Creek` = NA,
           `Bear Creek` = NA,
           `Big Chico Creek` = NA,
           `Butte Creek` = (C217B + D217) / (C217B + D217 + C217A),
           `Clear Creek` = NA,
           `Cottonwood Creek` = NA,
           `Cow Creek` = NA,
           `Deer Creek` = (C11309 / (C11307 + C11308 + C11309) * D11305) / C11309,
           `Elder Creek` = (C11303 / (C11303 + C11304) * D11301) / C11303,
           `Mill Creek` = (C11308 / (C11307 + C11308 + C11309) * D11305) / C11308,
           `Paynes Creek` = NA,
           `Stony Creek` = D17301 / C42,
           `Thomes Creek` = (C11304 / (C11303 + C11304) * D11301) / C11304,
           `Upper-mid Sacramento River` = (D109 + D112 + D113A + D113B + D114 + D118 + D122A + D122B
                                           # + D122_EWA  #not in baseline calsim run
                                           # + D122_WTS  #not in baseline calsim run
                                           # + D128_EWA  #not in baseline calsim run
                                           + D123 + D124A + D128_WTS + D128) / C110,
           `Sutter Bypass` = NA,
           `Bear River` = D285 / (C285 + D285),
           `Feather River` = (D201 + D202 + D7A + D7B) / C6,
           `Yuba River` = D230 / (C230 + D230),
           `Lower-mid Sacramento River` = (D129A + D134 + D162 + D165) / C128, # D165A does not exist
           `Yolo Bypass` = NA,
           `American River` = D302 / C9,
           `Lower Sacramento River` = (D167 + D168 + D168A_WTS) / C166,
           `Calaveras River` = (D506A + D506B + D506C + D507) / C92,
           `Cosumnes River` = NA,
           `Mokelumne River` = NA, # external model
           `Merced River` = (D562 + D566) / C561,
           `Stanislaus River` = D528 / C520,
           `Tuolumne River` = D545 / C540,
           `San Joaquin River` = (D637 + D630B + D630A + D620B) / (D637 + D630B + D630A + D620B + C637)) |>
    select(`Upper Sacramento River`:`San Joaquin River`, date) |>
    pivot_longer(`Upper Sacramento River`:`San Joaquin River`,
                 names_to = "watershed",
                 values_to = "prop_diver") |>
    mutate(prop_diver = round(prop_diver, 6),
           prop_diver = case_when(
             is.infinite(prop_diver) ~ 0,
             is.nan(prop_diver) ~ 0,
             prop_diver > 1 ~ 1,
             TRUE ~ prop_diver
           ))


  # create array
  proportion_diverted <- temp_prop_diverted |>
    filter(year(date) >= 1980, year(date) <= 2000) |>
    pivot_wider(names_from = date,
                values_from = prop_diver) |>
    left_join(DSMflow::watershed_ordering) |>
    select(-watershed) |>
    mutate_all(~replace_na(., 0)) |>
    arrange(order) |>
    select(-order) |>
    create_model_array()

  dimnames(proportion_diverted) <- list(watershed_ordering$watershed,
                                        month.abb[1:12], 1980:2000)
  return(proportion_diverted)
}

# Generate proportion diverted for 2008 and 2009 -------------------------------
prop_diverted_2008_2009 <- generate_proportion_diverted(calsim_data = calsim_2008_2009,
                                                        nodes = all_div_nodes)
# bring in Moke
moke <- read_excel('data-raw/calsim_2008_2009/EBMUDSIM/CVPIA_SIT_Data_RequestEBMUDSIMOutput_ExCond.xlsx',
                   sheet = 'Tableau Clean-up') |>
  mutate(date = as_date(Date), `Mokelumne River` = (D503A + D503B + D503C + D502A + D502B) / C91) |>
  select(date, `Mokelumne River`) |>
  filter(year(date) <= 2000 & year(date) >= 1980) |> # 1980-2000
  group_by(year(date), month(date)) |>
  summarize(monthly_flow = sum(`Mokelumne River`)) |> # summarize by month and year to fit into model array
  pivot_wider(names_from = `year(date)`, values_from = monthly_flow) |>
  select(-`month(date)`)

prop_diverted_2008_2009["Mokelumne River", , ] <- as.matrix(moke) # add to 2008_2009 matrix

# Generate proportion diverted for 2019 ----------------------------------------
prop_diverted_2019_biop_itp <- generate_proportion_diverted(calsim_data = calsim_2019_biop_itp,
                                                           nodes = all_div_nodes)
# bring in Moke for 2019
moke_2019 <- read_excel('data-raw/calsim_2019_BiOp_ITP/EBMUDSIM/CVPIA_SIT_Data_RequestUpdated2022.xlsx',
                   sheet = 'Tableau Clean-up') |>
  mutate(date = as_date(Date), `Mokelumne River` = (D503A + D503B + D503C + D502A + D502B) / C91) |>
  select(date, `Mokelumne River`) |>
  filter(year(date) <= 2000 & year(date) >= 1980) |> # 1980-2000
  group_by(year(date), month(date)) |>
  summarize(monthly_flow = sum(`Mokelumne River`)) |> # summarize by month and year to fit into model array
  pivot_wider(names_from = `year(date)`, values_from = monthly_flow) |>
  select(-`month(date)`)

prop_diverted_2019_biop_itp["Mokelumne River",,] <- as.matrix(moke_2019) # add to 2008_2009 matrix

# generate proportion diverted for run of river ---------------------------

# Generate proportion diverted for 2019 ----------------------------------------
prop_diverted_run_of_river <- generate_proportion_diverted(calsim_data = calsim_run_of_river,
                                                            nodes = all_div_nodes)
# TODO: update if we get run of river for Moke
# bring in Moke for 2019
moke_2019 <- read_excel('data-raw/calsim_2019_BiOp_ITP/EBMUDSIM/CVPIA_SIT_Data_RequestUpdated2022.xlsx',
                        sheet = 'Tableau Clean-up') |>
  mutate(date = as_date(Date), `Mokelumne River` = (D503A + D503B + D503C + D502A + D502B) / C91) |>
  select(date, `Mokelumne River`) |>
  filter(year(date) <= 2000 & year(date) >= 1980) |> # 1980-2000
  group_by(year(date), month(date)) |>
  summarize(monthly_flow = sum(`Mokelumne River`)) |> # summarize by month and year to fit into model array
  pivot_wider(names_from = `year(date)`, values_from = monthly_flow) |>
  select(-`month(date)`)

prop_diverted_run_of_river["Mokelumne River",,] <- as.matrix(moke_2019)

# LTO calsim 3 prop diverted
lto_proportion_diverted <-
  lto_total_diverted |>
  transmute(
    date,
    `Upper Sacramento River` = pmin(`div_final_Upper Sacramento River`/C_SAC273, 1),
    `Antelope Creek` = pmin(`div_final_Antelope Creek`/C_ANT010, 1),
    `Battle Creek` = 0,
    `Bear Creek` = 0,
    `Big Chico Creek` = 0,
    `Butte Creek` = pmin(`div_final_Butte Creek`/(`div_final_Butte Creek` + C_BTC012), 1),
    `Clear Creek` = 0,
    `Cottonwood Creek` = 0,
    `Cow Creek` = 0,
    `Deer Creek` = pmin(`div_final_Deer Creek`/C_DRC005, 1),
    `Elder Creek` = pmin(`div_final_Elder Creek`/C_ELD005, 1),
    `Mill Creek` = pmin(`div_final_Mill Creek`/C_MLC004, 1),
    `Paynes Creek` = 0,
    `Stony Creek` = pmin(`div_final_Stony Creek`/C_STN026, 1),
    `Thomes Creek` = pmin(`div_final_Thomes Creek`/C_THM005, 1),
    `Upper-mid Sacramento River` = pmin(`div_final_Upper-mid Sacramento River`/C_SAC247, 1),
    `Sutter Bypass` = 0,
    `Bear River` = pmin(`div_final_Bear River`/(`div_final_Bear River` + C_CMPFW), 1),
    `Feather River` = pmin(`div_final_Feather River`/C_OROVL, 1),
    `Yuba River` = pmin(`div_final_Yuba River`/(`div_final_Yuba River` + D_YUB011_15S_NA2)),
    `Lower-mid Sacramento River` = pmin(`div_final_Lower-mid Sacramento River`/(C_SAC120), 1),
    `Yolo Bypass` = 0,
    `American River` = pmin(`div_final_American River`/(C_NTOMA), 1),
    `Lower Sacramento River` = pmin(`div_final_Lower Sacramento River`/(C_SAC063), 1),
    `Calaveras River` = pmin(`div_final_Calaveras River`/C_NHGAN, 1),
    `Cosumnes River` = 0,
    `Mokelumne River` = pmin(`div_final_Mokelumne River`/C_CMCHE, 1),
    `Merced River` = pmin(`div_final_Merced River`/(C_MCD050), 1),
    `Stanislaus River` = pmin(`div_final_Stanislaus River`/(C_STS059)),
    `Tuolumne River` = pmin(`div_final_Tuolumne River`/C_TUO054, 1),
    `San Joaquin River` = pmin(`div_final_San Joaquin River`/(`div_final_San Joaquin River` + C_SJR072), 1)
  )

# convert to array
lto_proportion_diverted_final <- lto_proportion_diverted |>
  # turn into array for total_diversion
  filter(year(date) >= 1980, year(date) <= 2000) |>
  pivot_longer(`Upper Sacramento River`:`San Joaquin River`,
               names_to = "watershed",
               values_to = "prop_diver") |>
  mutate(prop_diver = DSMflow::cfs_to_cms(prop_diver)) |>
  pivot_wider(names_from = date,
              values_from = prop_diver) |>
  left_join(DSMflow::watershed_ordering) |>
  select(-watershed) |>
  mutate_all( ~ replace_na(., 0)) |>
  arrange(order) |>
  select(-order) |>
  create_model_array()

dimnames(lto_proportion_diverted_final) <- list(watershed_ordering$watershed,
                                           month.abb[1:12],
                                           1980:2000)

# create proportion diversion flows with both 2008-2009 biop and 2018-2019 biop/itp and run of river
proportion_diverted <- list(biop_2008_2009 = prop_diverted_2008_2009,
                            biop_itp_2018_2019 = prop_diverted_2019_biop_itp,
                            run_of_river = prop_diverted_run_of_river,
                            LTO_12a = lto_proportion_diverted_final
)

assertthat::are_equal(dimnames(prop_diverted_run_of_river), dimnames(lto_proportion_diverted_final))
# LTO action 5 scenario
proportion_diverted$action_5 <- action_5$proportion_diverted

usethis::use_data(proportion_diverted, overwrite = TRUE)

# tributary --------------
# Mean flows -------------------------------------------------------------------
generate_mean_flow <- function(bypass_flow, flow_cfs) {
  bypass <- bypass_flow |>
    select(date, `Sutter Bypass` = sutter4, `Yolo Bypass` = yolo2)

  mean_flow <- flow_cfs |>
    left_join(bypass) |>
    filter(between(year(date), 1980, 2000)) |>
    # gather(watershed, flow_cfs, -date)
    pivot_longer(-date,
                 names_to = "watershed",
                 values_to = "flow_cfs") |>
    filter(watershed != "Lower-mid Sacramento River1") |>
    mutate(flow_cms = DSMflow::cfs_to_cms(flow_cfs),
           watershed = ifelse(watershed == "Lower-mid Sacramento River2",
                              "Lower-mid Sacramento River", watershed)) |>
    select(-flow_cfs) |>
    left_join(DSMflow::watershed_ordering) |>
    pivot_wider(names_from = date,
                values_from = flow_cms) |>
    arrange(order) |>
    select(-order, -watershed) |>
    create_model_array()

    dimnames(mean_flow) <- list(DSMflow::watershed_ordering$watershed,
                               month.abb[1:12],
                               1980:2000)
    return(mean_flow)
}

# create mean flows with both 2008-2009 biop and 2018-2019 biop/itp and run of river
# using bypass nodes that are activated the most for meanQ
mean_flow_2008_2009 <- generate_mean_flow(bypass_flows$biop_2008_2009, flows_cfs$biop_2008_2009)
mean_flow_2019_biop_itp <- generate_mean_flow(bypass_flows$biop_itp_2018_2019, flows_cfs$biop_itp_2018_2019) # missing moke
mean_flow_run_of_river <- generate_mean_flow(bypass_flows$run_of_river, flows_cfs$run_of_river) # missing moke
mean_flow_eff <- generate_mean_flow(bypass_flows$biop_itp_2018_2019, flows_cfs$eff)
mean_flow_lto12a <- generate_mean_flow(bypass_flows$LTO_12a, flows_cfs$LTO_12a)
mean_flow_LTO_12a_eff_dy <- generate_mean_flow(bypass_flows$biop_itp_2018_2019, flows_cfs$LTO_12a_eff_dy)

mean_flow <- list(biop_2008_2009 = mean_flow_2008_2009,
                  biop_itp_2018_2019 = mean_flow_2019_biop_itp,
                  run_of_river = mean_flow_run_of_river,
                  eff = mean_flow_eff,
                  LTO_12a = mean_flow_lto12a,
                  LTO_12a_eff_dy = mean_flow_LTO_12a_eff_dy)

assertthat::are_equal(dimnames(mean_flow_eff), dimnames(mean_flow_lto12a))
# LTO action 5 scenario
# mean_flow$action_5 <- action_5$mean_flow # TODO: not yet available

usethis::use_data(mean_flow, overwrite = TRUE)

# misc flow nodes ----
# prepare misc flow nodes to be used to generate additional data objects
generate_misc_flow_nodes <- function(cs, ds) {
  misc_flows <- left_join(cs, ds) |>
    pivot_longer(C134:D126,
                 names_to = "node",
                 values_to = "flow") |>
    arrange(node) |>
    filter(!is.na(flow)) |>
    mutate(flow = as.numeric(flow)) |>
    pivot_wider(names_from = node,
                 values_from = flow)

  return(misc_flows)
}

# 2008-2009 biop
cs <- read_csv('data-raw/calsim_2008_2009/MikeWrightCalSimOct2017/C1_C169.csv', skip = 1) |>
  select(date = "...2", C134, C165, C116, C123, C124, C125, C109) |>
  filter(!is.na(date)) |>
  mutate(date = dmy(date))

ds <- read_csv('data-raw/calsim_2008_2009/MikeWrightCalSimOct2017/D100_D403.csv', skip = 1) |>
  select(date = "...2", D160, D166A, D117, D124, D125, D126) |>
  filter(!is.na(date)) |>
  mutate(date = dmy(date))

misc_flows_2008_2009 <- generate_misc_flow_nodes(cs, ds)

# 2018, 2019 biop
cs <- read_csv('data-raw/calsim_2019_BiOp_ITP/C1_C169.csv', skip = 1) |>
  select(date = "...2", C134, C165, C116, C123, C124, C125, C109) |>
  filter(!is.na(date)) |>
  mutate(date = dmy(date))

ds <- read_csv('data-raw/calsim_2019_BiOp_ITP/D100_D403.csv', skip = 1) |>
  select(date = "...2", D160, D166A, D117, D124, D125, D126) |>
  filter(!is.na(date)) |>
  mutate(date = dmy(date))

misc_flows_2019_biop_itp <- generate_misc_flow_nodes(cs, ds)

# run of river
# Note: must fix dates which are imported and off by 100 years, except for years 2000-2003
cs_filter <- read_excel('data-raw/calsim_run_of_river/max_flow_data/CalSim/C1_C169.xlsx', skip = 1) |>
  select(date = "...2", C134, C165, C116, C123, C124, C125, C109) |>
  filter(!is.na(date)) |>
  mutate(date = as.Date(date)) |>
  filter(year(date) <= 2003)

cs <- read_excel('data-raw/calsim_run_of_river/max_flow_data/CalSim/C1_C169.xlsx', skip = 1) |>
  select(date = "...2", C134, C165, C116, C123, C124, C125, C109) |>
  filter(!is.na(date)) |>
  mutate(date = as.Date(date)) |>
  filter(year(date) > 2003) |>
  mutate(date = as.Date(date)-lubridate::years(100)) |>
  bind_rows(cs_filter) |>
  filter(year(date) >= 1922, year(date) <= 2002)

ds_filter <- read_excel('data-raw/calsim_run_of_river/max_flow_data/CalSim/D100_D403.xlsx', skip = 1) |>
  select(date = "...2", D160, D166A, D117, D124, D125, D126) |>
  filter(!is.na(date)) |>
  mutate(date = as.Date(date)) |>
  filter(year(date) <= 2003)

ds <- read_excel('data-raw/calsim_run_of_river/max_flow_data/CalSim/D100_D403.xlsx', skip = 1) |>
  select(date = "...2", D160, D166A, D117, D124, D125, D126) |>
  filter(!is.na(date)) |>
  mutate(date = as.Date(date)) |>
  filter(year(date) > 2003) |>
  mutate(date = as.Date(date)-lubridate::years(100)) |>
  bind_rows(ds_filter) |>
  filter(year(date) >= 1922, year(date) <= 2002)

misc_flows_run_of_river <- generate_misc_flow_nodes(cs, ds)

misc_flows <- list(biop_2008_2009 = misc_flows_2008_2009,
                   biop_itp_2018_2019 = misc_flows_2019_biop_itp,
                   run_of_river = misc_flows_run_of_river)

# flow at Bend C109, CALSIMII units cfs, sit-model units cms
# replaces upsacQ

generate_upper_sacramento_flows <- function(misc_flows) {

  upper_sacramento_flows <- misc_flows |>
    select(date, upsacQcfs = C109) |>
    mutate(upsacQcms = DSMflow::cfs_to_cms(upsacQcfs),
           year = year(date),
           month = month(date)) |>
    filter(year >= 1980, year <= 2000) |>
    select(-date, -upsacQcfs) |>
    pivot_wider(names_from = year,
                values_from = upsacQcms) |>
    select(-month) |>
    as.matrix()

  rownames(upper_sacramento_flows) <- month.abb[1:12]
  return(upper_sacramento_flows)
}

upper_sacramento_flows_2008_2009 <- generate_upper_sacramento_flows(misc_flows$biop_2008_2009)
upper_sacramento_flows_2019_biop_itp <- generate_upper_sacramento_flows(misc_flows$biop_itp_2018_2019)
upper_sacramento_flows_run_of_river <- generate_upper_sacramento_flows(misc_flows$run_of_river)

up_sac_flows_eff_as_matrix <- synthetic_eff_sac |>
  mutate(upsacQcms = DSMflow::cfs_to_cms(`Upper Sacramento River EFF`),
         year = year(date),
         month = month(date)) |>
  filter(year >= 1980, year <= 2000) |>
  arrange(date, ascending = TRUE) |>
  select(-date, -`Upper Sacramento River EFF`) |>
  pivot_wider(names_from = year,
              values_from = upsacQcms) |>
  select(-month) |>
  as.matrix()
rownames(up_sac_flows_eff_as_matrix) <- month.abb[1:12]

upper_sac_flows_eff <- up_sac_flows_eff_as_matrix # copied code from EFF vignette above

# calsim 3
lto_12a_upper_sacramento_flows <- calsim3_data |>
  filter(node == "C_SAC257") |>
  select(-B, -node, upsacQcfs=flow_cfs) |>
  mutate(upsacQcms = DSMflow::cfs_to_cms(upsacQcfs),
         year = year(date),
         month = month(date)) |>
  filter(year >= 1980, year <= 2000) |>
  select(-date, -upsacQcfs) |>
  pivot_wider(names_from = year,
              values_from = upsacQcms) |>
  select(-month) |>
  as.matrix()

rownames(lto_12a_upper_sacramento_flows) <- month.abb

# add in EFF flows for dry years to HRL
dry_years_model <- dry_years[dry_years %in% 1980:2000]
dry_years_index <- which(1980:2000 %in%dry_years_model)
upper_sacramento_flows_LTO_12a_eff_dy <- lto_12a_upper_sacramento_flows
upper_sacramento_flows_LTO_12a_eff_dy[, dry_years_index] <- upper_sac_flows_eff[, dry_years_index]

upper_sacramento_flows <- list(biop_2008_2009 = upper_sacramento_flows_2008_2009,
                               biop_itp_2018_2019 = upper_sacramento_flows_2019_biop_itp,
                               run_of_river = upper_sacramento_flows_run_of_river,
                               eff = upper_sac_flows_eff,
                               LTO_12a = lto_12a_upper_sacramento_flows,
                               LTO_12a_eff_dy = upper_sacramento_flows_LTO_12a_eff_dy)


assertthat::are_equal(dimnames(lto_12a_upper_sacramento_flows), dimnames(upper_sac_flows_eff))
# LTO action 5 scenario
upper_sacramento_flows$action_5 <- action_5$upper_sacramento_flows

usethis::use_data(upper_sacramento_flows, overwrite = TRUE)

# create SJ object - quick and dirty approach, just only available for use in EFF scenario
sj_flows_eff_as_matrix <- synthetic_eff_sj |>
  mutate(sjQcms = DSMflow::cfs_to_cms(`Lower San Joaquin EFF`),
         year = year(date),
         month = month(date)) |>
  filter(year >= 1980, year <= 2000) |>
  arrange(date, ascending = TRUE) |>
  select(-date, -`Lower San Joaquin EFF`) |>
  pivot_wider(names_from = year,
              values_from = sjQcms) |>
  select(-month) |>
  as.matrix()
rownames(sj_flows_eff_as_matrix) <- month.abb[1:12]

san_joaquin_flows_eff <- sj_flows_eff_as_matrix # copied code from EFF vignette above

# create HRL/EFF combo for dry years
san_joaquin_flows_LTO_12a <- flows_cfs$LTO_12a |>
  select(date, `San Joaquin River`) |>
  mutate(sjQcms = DSMflow::cfs_to_cms(`San Joaquin River`),
         year = year(date),
         month = month(date)) |>
  filter(year >= 1980, year <= 2000) |>
  arrange(date, ascending = TRUE) |>
  select(-date, -`San Joaquin River`) |>
  pivot_wider(names_from = year,
              values_from = sjQcms) |>
  select(-month) |>
  as.matrix()

san_joaquin_flows_LTO_12a_eff_dy <- san_joaquin_flows_LTO_12a
san_joaquin_flows_LTO_12a_eff_dy[, dry_years_index] <- san_joaquin_flows_eff[, dry_years_index]
rownames(san_joaquin_flows_LTO_12a_eff_dy) <- month.abb

san_joaquin_flows <- list(eff = san_joaquin_flows_eff,
                          LTO_12a_eff_dy = san_joaquin_flows_LTO_12a_eff_dy)
# LTO action 5 scenario
san_joaquin_flows$action_5 <- action_5$san_joaquin_flows

usethis::use_data(san_joaquin_flows, overwrite = TRUE)

# prop flow natal --------------------------------------------------------------
# Replaces retQ
# proportion flows at tributary junction coming from natal watershed using october average flow
# create lookup vector for retQ denominators based on Jim's previous input
tributary_junctions <- c(rep(watersheds[16], 16), NA, watersheds[19], watersheds[21], watersheds[19],
                         watersheds[21], NA, rep(watersheds[24],2), watersheds[25:27], rep(watersheds[31],4))

names(tributary_junctions) <- watersheds

generate_proportion_flow_natal <- function(flow_cfs, tributary_junctions){
  denominator <- flow_cfs |>
    select(-`Lower-mid Sacramento River1`) |> #Feather river comes in below Fremont Weir use River2 for Lower-mid Sac
    rename(`Lower-mid Sacramento River` = `Lower-mid Sacramento River2`) |>
    pivot_longer(-date,
                 names_to = "watershed",
                 values_to = "flow") |>
    filter(month(date) == 10, watershed %in% unique(tributary_junctions)) |>
    rename(denominator = watershed, junction_flow = flow)

  proportion_flow_natal <- flow_cfs |>
    select(-`Lower-mid Sacramento River1`) |> #Feather river comes in below Fremont Weir use River2 for Lower-mid Sac
    rename(`Lower-mid Sacramento River` = `Lower-mid Sacramento River2`) |>
    pivot_longer(-date,
                 names_to = "watershed",
                 values_to = "flow") |>
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
    pivot_wider(names_from = year,
                values_from = retQ) |>
    select(-watershed, -order) |>
    mutate_all(~replace_na(., 0)) |>
    as.matrix()

  rownames(proportion_flow_natal) <- watersheds
  return(proportion_flow_natal)
}

proportion_flow_natal_2008_2009 <- generate_proportion_flow_natal(flows_cfs$biop_2008_2009, tributary_junctions)
proportion_flow_natal_2019_biop_itp <- generate_proportion_flow_natal(flows_cfs$biop_itp_2018_2019, tributary_junctions)
proportion_flow_natal_run_of_river <- generate_proportion_flow_natal(flows_cfs$run_of_river, tributary_junctions)
proportion_flow_natal_eff <- generate_proportion_flow_natal(flows_cfs$eff, tributary_junctions)
proportion_flow_natal_lto12a <- generate_proportion_flow_natal(lto_calsim3_flows, tributary_junctions)
proportion_flow_natal_lto12a_eff_dy <- generate_proportion_flow_natal(flows_cfs$LTO_12a_eff_dy, tributary_junctions)

proportion_flow_natal <- list(biop_2008_2009 = proportion_flow_natal_2008_2009,
                              biop_itp_2018_2019 = proportion_flow_natal_2019_biop_itp,
                              run_of_river = proportion_flow_natal_run_of_river,
                              eff = proportion_flow_natal_eff,
                              LTO12a = proportion_flow_natal_lto12a)

assertthat::are_equal(dimnames(proportion_flow_natal_2008_2009), dimnames(proportion_flow_natal_eff))
# LTO action 5 scenario
proportion_flow_natal$action_5 <- action_5$proportion_flow_natal

usethis::use_data(proportion_flow_natal, overwrite = TRUE)

# proportion pulse flows -------------------------------------------------------
# Replaces prop.pulse
generate_proportion_pulse_flows <- function(flow_cfs) {
  proportion_pulse_flows <- flow_cfs |>
    filter(between(year(date), 1980, 1999)) |>
    mutate(`Lower-mid Sacramento River` = 35.6/58 * `Lower-mid Sacramento River1` + 22.4/58 *`Lower-mid Sacramento River2`) |>
    select(-`Lower-mid Sacramento River1`, -`Lower-mid Sacramento River2`) |>
    pivot_longer(-date,
                 names_to = "watershed",
                 values_to = "flow") |>
    group_by(month = month(date), watershed) |>
    summarise(prop_pulse = sd(flow)/median(flow)/100) |>
    mutate(prop_pulse = replace(prop_pulse, is.infinite(prop_pulse), 0)) |>
    bind_rows(tibble(
      month = rep(1:12, 2),
      watershed = rep(c('Yolo Bypass', 'Sutter Bypass'), each = 12),
      prop_pulse = 0
    )) |>
    pivot_wider(names_from = month,
                 values_from = prop_pulse) |>
    left_join(DSMflow::watershed_ordering) |>
    arrange(order) |>
    select(-order, -watershed) |>
    as.matrix()

  colnames(proportion_pulse_flows) <- month.abb[1:12]
  rownames(proportion_pulse_flows) <- DSMflow::watershed_ordering$watershed

  return(proportion_pulse_flows)
}

proportion_pulse_flows_2008_2009 <- generate_proportion_pulse_flows(flows_cfs$biop_2008_2009)
proportion_pulse_flows_2019_biop_itp <- generate_proportion_pulse_flows(flows_cfs$biop_itp_2018_2019)
proportion_pulse_flows_run_of_river <- generate_proportion_pulse_flows(flows_cfs$run_of_river)
proportion_pulse_flows_lto_12a <- generate_proportion_pulse_flows(flows_cfs$LTO_12a)
proportion_pulse_flows_eff <- generate_proportion_pulse_flows(flows_cfs$eff)
proportion_pulse_flows_lto_12a_eff_dy <- generate_proportion_pulse_flows(flows_cfs$LTO_12a_eff_dy)

proportion_pulse_flows_run_of_river[is.nan(proportion_pulse_flows_run_of_river)] <- 0
proportion_pulse_flows_eff[is.na(proportion_pulse_flows_eff)] <- 0
proportion_pulse_flows_lto_12a[is.na(proportion_pulse_flows_lto_12a)] <- 0
proportion_pulse_flows_lto_12a_eff_dy[is.na(proportion_pulse_flows_lto_12a_eff_dy)] <- 0

proportion_pulse_flows <- list(biop_2008_2009 = proportion_pulse_flows_2008_2009,
                              biop_itp_2018_2019 = proportion_pulse_flows_2019_biop_itp,
                              run_of_river = proportion_pulse_flows_run_of_river,
                              eff = proportion_pulse_flows_eff,
                              LTO_12a = proportion_pulse_flows_lto_12a,
                              LTO_12a_eff_dy = proportion_pulse_flows_lto_12a_eff_dy)

assertthat::are_equal(dimnames(proportion_pulse_flows_2008_2009), dimnames(proportion_pulse_flows_lto_12a))
# LTO action 5 scenario
proportion_pulse_flows$action_5 <- action_5$proportion_pulse_flows

usethis::use_data(proportion_pulse_flows, overwrite = TRUE)

# delta flow objects -----------------------------------------------------------
# Cross channel gates
#  C400 flow at freeport
# 1) daily discharge of the Sacramento River at Freeport
# 2) an indicator variable for whether the DCC is open (1) or closed (0).
# Replaces dlt.gates
delta_cross_channel_closed_2008_2009 <- read_csv('data-raw/delta_cross_channel_gates/DeltaCrossChannelTypicalOperations.csv', skip = 2) |>
  mutate(Month = which(month.name == Month), prop_days_closed = `Days Closed` / days_in_month(Month)) |>
  select(month = Month, days_closed = `Days Closed`, prop_days_closed) |>
  pivot_longer(days_closed,
               names_to = "metric",
               values_to = "value") |>
  pivot_wider(names_from = metric,
              values_from = value) |>
  select(-month) |>
  select(days_closed, prop_days_closed) |>
  as.matrix() |>
  t()

colnames(delta_cross_channel_closed_2008_2009) <- month.abb[1:12]
rownames(delta_cross_channel_closed_2008_2009) <- c('count', 'proportion')

# Generate for 2019
# The followung guidance was provided to use for the update:
# Per guidance from Reclamation’s CALSIM lead modeler (Derya Sumer), the same logic
# used to represent days closed for the 2009 CALSIM model should be used for the 2019
# CALSIM model. Specifically, there should be 28 days closed in May and 8 days closed
# in June. It is important to note that DSM calibration would still require synthetic
# monthly flows based on the 2009 CALSIM model ouputs that are representative of the
# escapement period of record use for calibration.

delta_cross_channel_closed_2018_2019 <- read_csv('data-raw/delta_cross_channel_gates/DeltaCrossChannelTypicalOperations.csv', skip = 2) |>
  mutate(Month = which(month.name == Month), prop_days_closed = `Days Closed` / days_in_month(Month)) |>
  select(month = Month, days_closed = `Days Closed`, prop_days_closed) |>
  pivot_longer(days_closed,
               names_to = "metric",
               values_to = "value") |>
  pivot_wider(names_from = metric,
              values_from = value) |>
  select(-month) |>
  select(days_closed, prop_days_closed) |>
  as.matrix() |>
  t()

colnames(delta_cross_channel_closed_2018_2019) <- month.abb[1:12]
rownames(delta_cross_channel_closed_2018_2019) <- c('count', 'proportion')

# replace May and June values according to text above (from word doc proposal)
delta_cross_channel_closed_2018_2019[ , "May"] <- c(28, 28/31)
delta_cross_channel_closed_2018_2019[ , "Jun"] <- c(8, 8/30)

# Generate for run of river
# Based on discussion from SAT team should be closed 100% of time under Run of River
days <- lubridate::days_in_month(c(1:12))

delta_cross_channel_closed_run_of_river <- dplyr::tibble(days_closed = days,
                                                         prop_days_closed = c(rep(1, 12))) |>
  as.matrix() |> t()

colnames(delta_cross_channel_closed_run_of_river) <- month.abb[1:12]
rownames(delta_cross_channel_closed_run_of_river) <- c('count', 'proportion')

# lto12a
# based on their script no changes required
# TODO: review if we want to pick option 3 instead.
# Multiple options
# 1. Use existing values, based on 2009 BiOp
# 2. Use expected values from 2019 BiOp
# 3. Use updated CalSim outputs, either averaged across years (A) or year-specific (B)


# Combine into named list
delta_cross_channel_closed <- list(biop_2008_2009 = delta_cross_channel_closed_2008_2009,
                                   biop_itp_2018_2019 = delta_cross_channel_closed_2018_2019,
                                   run_of_river = delta_cross_channel_closed_run_of_river,
                                   LTO_12a = delta_cross_channel_closed_2018_2019)

# LTO action 5 scenario
# delta_cross_channel_closed$action_5 <- action_5$delta_cross_channel_closed # TODO: not yet available

usethis::use_data(delta_cross_channel_closed, overwrite = TRUE)

# delta flows and diversions ---------------------------------------------------
# North Delta inflows: C400 + C157
# South Delta inflow: C401B + C504 + C508 + C644
# North Delta diversions: D403A + D403B + D403C + D403D + D404
# South Delta diversions: D418 + D419 + D412 + D410 + D413 + D409B + D416 + D408_OR + D408_VC

generate_delta_flows <- function(calsim_data) {
  delta_flows <- calsim_data |>
    select(date, C400, C157, C401B, C504, C508, C644, D403A, D403B, D403C, D403D,
           D404, D418, D419, D412, D410, D413, D409B, D416, D408_OR, D408_VC) |>
    mutate(n_dlt_inflow_cfs = C400 + C157,
           s_dlt_inflow_cfs = C401B + C504 + C508 + C644,
           n_dlt_div_cfs =  D403A + D403B + D403C + D403D + D404,
           s_dlt_div_cfs = D418 + D419 + D412 + D410 + D413 + D409B + D416 + D408_OR + D408_VC,
           n_dlt_prop_div = n_dlt_div_cfs / n_dlt_inflow_cfs,
           s_dlt_prop_div = s_dlt_div_cfs / s_dlt_inflow_cfs,
           s_dlt_prop_div = ifelse(s_dlt_prop_div > 1, 1, s_dlt_prop_div)) |>
    select(date,
           n_dlt_inflow_cfs,
           s_dlt_inflow_cfs,
           n_dlt_div_cfs,
           s_dlt_div_cfs,
           n_dlt_prop_div,
           s_dlt_prop_div)

  return(delta_flows)
}

delta_flows_2008_2009 <- generate_delta_flows(calsim_2008_2009)
delta_flows_2019_biop_itp <- generate_delta_flows(calsim_2019_biop_itp)
delta_flows_run_of_river <- generate_delta_flows(calsim_run_of_river)

#calsim 3
n_dlt_inflow_cfs_nodes <- c("C_SAC041", "C_CSL005")
s_dlt_inflow_cfs_nodes <- c("C_SAC029B", "D_SAC030_MOK014",
                            "C_MOK022", "C_CLV004", "C_SJR056")
n_dlt_div_cfs_nodes <- c("C_CSL004B", "DD_SAC017_SACS")
s_dlt_div_cfs_nodes <- c("D_OMR028_DMC000", "D_OMR027_CAA000",
                         "DD_SJR026_SJRE", "DD_SJR013_SJRW",
                         "DD_MOK004_MOK", "DD_OMR027_OMR",
                         "D_RSL004_CCC004", "D_OMR021_ORP000",
                         "D_VCT002_ORP000")

north_delta_inflow <- calsim3_data |>
  filter(node %in% n_dlt_inflow_cfs_nodes) |>
  group_by(date = as_date(date)) |>
  summarise(
    flow_cfs = sum(flow_cfs)
  )

south_delta_inflow <- calsim3_data |>
  filter(node %in% s_dlt_inflow_cfs_nodes)|>
  group_by(date = as_date(date)) |>
  summarise(
    flow_cfs = sum(flow_cfs)
  )

north_delta_diversions <- calsim3_data |>
  filter(node %in% n_dlt_div_cfs_nodes)|>
  group_by(date = as_date(date)) |>
  summarise(
    flow_cfs = sum(flow_cfs)
  )

south_delta_diversions <- calsim3_data |>
  filter(node %in% s_dlt_div_cfs_nodes)|>
  group_by(date = as_date(date)) |>
  summarise(
    flow_cfs = sum(flow_cfs)
  )

lto_12a_delta_flows <- tibble(
  date = north_delta_inflow$date,
  n_dlt_inflow_cfs = north_delta_inflow$flow_cfs,
  s_dlt_inflow_cfs = south_delta_inflow$flow_cfs,
  n_dlt_div_cfs = north_delta_diversions$flow_cfs,
  s_dlt_div_cfs = south_delta_diversions$flow_cfs,
  n_dlt_prop_div = n_dlt_div_cfs / n_dlt_inflow_cfs,
  s_dlt_prop_div = s_dlt_div_cfs / s_dlt_inflow_cfs
) |>
  mutate(
    s_dlt_prop_div = ifelse(s_dlt_prop_div > 1, 1, s_dlt_prop_div)
  ) |>
  filter(between(date,
                 range(delta_flows_run_of_river$date)[1],
                 range(delta_flows_run_of_river$date)[2]))


delta_flows <- list(biop_2008_2009 = delta_flows_2008_2009,
                    biop_itp_2018_2019 = delta_flows_2019_biop_itp,
                    run_of_river = delta_flows_run_of_river,
                    LTO_12a = lto_12a_delta_flows)

assertthat::are_equal(dimnames(lto_12a_delta_flows), dimnames(delta_flows_run_of_river))
# LTO action 5 scenario
delta_flows$action_5 <- action_5$delta_flows

usethis::use_data(delta_flows, overwrite = TRUE)

# delta inflows ----------------------------------------------------------------
generate_delta_inflows <- function(delta_flows) {
  inflow <- delta_flows |>
    filter(year(date) >= 1980, year(date) <= 2000) |>
    mutate(n_dlt_inflow_cms = DSMflow::cfs_to_cms(n_dlt_inflow_cfs),
           s_dlt_inflow_cms = DSMflow::cfs_to_cms(s_dlt_inflow_cfs)) |>
    select(date, n_dlt_inflow_cms, s_dlt_inflow_cms) |>
    pivot_longer(n_dlt_inflow_cms:s_dlt_inflow_cms,
                 names_to = "delta",
                 values_to = "inflow") |>
    pivot_wider(names_from = date,
                values_from = inflow) |>
    select(-delta)

  delta_inflow <- array(NA, dim = c(12, 21, 2))
  delta_inflow[ , , 1] <- as.matrix(inflow[1, ], nrow = 12, ncol = 21)
  delta_inflow[ , , 2] <- as.matrix(inflow[2, ], nrow = 12, ncol = 21)

  dimnames(delta_inflow) <- list(month.abb[1:12],
                                 1980:2000,
                                 c('North Delta', 'South Delta'))
  return(delta_inflow)
}

delta_inflows_2008_2009 <- generate_delta_inflows(delta_flows$biop_2008_2009)
delta_inflows_2019_biop_itp <- generate_delta_inflows(delta_flows$biop_itp_2018_2019)
delta_inflows_run_of_river <- generate_delta_inflows(delta_flows$run_of_river)
delta_inflows_lto_12a <- generate_delta_inflows(delta_flows$LTO_12a)


delta_inflow <- list(biop_2008_2009 = delta_inflows_2008_2009,
                    biop_itp_2018_2019 = delta_inflows_2019_biop_itp,
                    run_of_river = delta_inflows_run_of_river,
                    LTO_12a = delta_inflows_lto_12a)

assertthat::are_equal(dimnames(delta_inflows_lto_12a), dimnames(delta_inflows_run_of_river))
# LTO action 5 scenario
delta_inflow$action_5 <- action_5$delta_inflow

usethis::use_data(delta_inflow, overwrite = TRUE)

# delta prop diverted ----------------------------------------------------------
generate_delta_proportion_diverted <- function(delta_flows) {
  dl_prop_div <- delta_flows |>
    filter(year(date) >= 1980, year(date) <= 2000) |>
    select(date, n_dlt_prop_div, s_dlt_prop_div) |>
    pivot_longer(n_dlt_prop_div:s_dlt_prop_div,
                 names_to = "delta",
                 values_to = "prop_div") |>
    pivot_wider(names_from = date,
                values_from = prop_div) |>
    select(-delta)

  delta_proportion_diverted <- array(NA, dim = c(12, 21, 2))
  delta_proportion_diverted[ , , 1] <- as.matrix(dl_prop_div[1, ], nrow = 12, ncol = 21)
  delta_proportion_diverted[ , , 2] <- as.matrix(dl_prop_div[2, ], nrow = 12, ncol = 21)

  dimnames(delta_proportion_diverted) <- list(month.abb[1:12],
                                              1980:2000,
                                              c('North Delta', 'South Delta'))

  return(delta_proportion_diverted)
}

delta_proportion_diverted_2008_2009 <- generate_delta_proportion_diverted(delta_flows$biop_2008_2009)
delta_proportion_diverted_2019_biop_itp <- generate_delta_proportion_diverted(delta_flows$biop_itp_2018_2019)
delta_proportion_diverted_run_of_river <- generate_delta_proportion_diverted(delta_flows$run_of_river)
delta_proportion_diverted_lto_12a <- generate_delta_proportion_diverted(delta_flows$LTO_12a)


delta_proportion_diverted <- list(biop_2008_2009 = delta_proportion_diverted_2008_2009,
                                  biop_itp_2018_2019 = delta_proportion_diverted_2019_biop_itp,
                                  run_of_river = delta_proportion_diverted_run_of_river,
                                  LTO_12a = delta_proportion_diverted_lto_12a)
# LTO action 5 scenario
delta_proportion_diverted$action_5 <- action_5$delta_proportion_diverted

usethis::use_data(delta_proportion_diverted, overwrite = TRUE)

# delta total diversions -------------------------------------------------------
generate_delta_total_diverted <- function(delta_flows) {
  dl_tot_div <- delta_flows |>
    filter(year(date) >= 1980, year(date) <= 2000) |>
    mutate(n_dlt_div_cms = DSMflow::cfs_to_cms(n_dlt_div_cfs),
           s_dlt_div_cms = DSMflow::cfs_to_cms(s_dlt_div_cfs)) |>
    select(date, n_dlt_div_cms, s_dlt_div_cms) |>
    pivot_longer(n_dlt_div_cms:s_dlt_div_cms,
                 names_to = "delta",
                 values_to = "tot_div") |>
    pivot_wider(names_from = date,
                values_from = tot_div) |>
    select(-delta)

  delta_total_diverted <- array(NA, dim = c(12, 21, 2))
  delta_total_diverted[ , , 1] <- as.matrix(dl_tot_div[1, ], nrow = 12, ncol = 21)
  delta_total_diverted[ , , 2] <- as.matrix(dl_tot_div[2, ], nrow = 12, ncol = 21)

  dimnames(delta_total_diverted) <- list(month.abb[1:12],
                                         1980:2000,
                                         c('North Delta', 'South Delta'))
  return(delta_total_diverted)

}

delta_total_diverted_2008_2009 <- generate_delta_total_diverted(delta_flows$biop_2008_2009)
delta_total_diverted_2019_biop_itp <- generate_delta_total_diverted(delta_flows$biop_itp_2018_2019)
delta_total_diverted_run_of_river <- generate_delta_total_diverted(delta_flows$run_of_river)
delta_total_diverted_lto_12a <- generate_delta_total_diverted(delta_flows$LTO_12a)


delta_total_diverted <- list(biop_2008_2009 = delta_total_diverted_2008_2009,
                             biop_itp_2018_2019 = delta_total_diverted_2019_biop_itp,
                             run_of_river = delta_total_diverted_run_of_river,
                             LTO_12a = delta_total_diverted_lto_12a)
# LTO action 5 scenario
delta_total_diverted$action_5 <- action_5$delta_total_diverted

usethis::use_data(delta_total_diverted, overwrite = TRUE)

# bypasses ---------------------------------------------------------------------
# cap values greater than 1 at 1
generate_proportion_flow_bypasses <- function(misc_flows) {
  bypass_prop_flow <- misc_flows |>
    mutate(yolo = pmin(D160/C134, 1),
           sutter = (D117 + D124 + D125 + D126)/C116,
           year = year(date), month = month(date)) |>
    select(month, year, yolo, sutter) |>
    filter(between(year, 1980, 2000)) |>
    pivot_longer(yolo:sutter,
                 names_to = "bypass",
                 values_to = "prop_flow") |>
    pivot_wider(names_from = year,
                values_from = prop_flow) |>
    arrange(bypass, month) |>
    select(-month, -bypass) |>
    as.matrix()

  proportion_flow_bypasses <- array(NA, dim = c(12, 21, 2))
  proportion_flow_bypasses[ , , 1] <- bypass_prop_flow[1:12, ]
  proportion_flow_bypasses[ , , 2] <- bypass_prop_flow[13:24, ]

  dimnames(proportion_flow_bypasses) <- list(month.abb[1:12],
                                             1980:2000,
                                             c('Sutter Bypass', 'Yolo Bypass'))

  return(proportion_flow_bypasses)

}

proportion_flow_bypasses_2008_2009 <- generate_proportion_flow_bypasses(misc_flows$biop_2008_2009)
proportion_flow_bypasses_2019_biop_itp <- generate_proportion_flow_bypasses(misc_flows$biop_itp_2018_2019)
proportion_flow_bypasses_run_of_river <- generate_proportion_flow_bypasses(misc_flows$run_of_river)

# lto 12a
calsim3_prop_q_nodes <- data.frame(inputs=c("D117", "D117", "D117",
                                            "D124", "D125", "D126",
                                            "D160",
                                            "C116", "C134", "C137",
                                            "C160", "C157"),
                                   nodes=c("SP_SAC193_BTC003", "SP_SAC188_BTC003", "SP_SAC178_BTC003",
                                           "SP_SAC159_BTC003", "SP_SAC148_BTC003", "SP_SAC122_SBP021",
                                           "SP_SAC083_YBP037",
                                           "C_SAC195", "C_SAC093", "C_SSL001",
                                           "C_SAC048", "C_CSL005"),
                                   type=c(rep("RIVER-SPILLS",7),rep("CHANNEL",5)))

lto_propq_data <- calsim3_data |> filter(node %in% calsim3_prop_q_nodes$nodes) |>
  left_join(select(calsim3_prop_q_nodes, inputs, nodes), by=c("node" = "nodes")) |>
  group_by(date = as_date(date), inputs) |>
  summarise(
    flow_cfs = sum(flow_cfs)
  ) |> ungroup() |>
  pivot_wider(names_from = "inputs", values_from = "flow_cfs")

lto_12a_proportion_flow_bypass <- generate_proportion_flow_bypasses(lto_propq_data)

proportion_flow_bypasses <- list(biop_2008_2009 = proportion_flow_bypasses_2008_2009,
                                 biop_itp_2018_2019 = proportion_flow_bypasses_2019_biop_itp,
                                 run_of_river = proportion_flow_bypasses_run_of_river,
                                 LTO_12a = lto_12a_proportion_flow_bypass)
# LTO action 5 scenario
proportion_flow_bypasses$action_5 <- action_5$proportion_flow_bypasses

usethis::use_data(proportion_flow_bypasses, overwrite = TRUE)

# Adds gates_overtopped --------------------------------------------------------
# overtopped is > 100 cfs

generate_gates_overtopped <- function(calsim_data) {
  bypass_overtopped <- calsim_data %>%
    mutate(sutter = D117 + D124 + D125 + D126 + C137,
           yolo = D160 + C157) %>%
    select(date, sutter, yolo) %>%
    filter(between(year(date), 1980, 2000)) %>%
    gather(bypass, flow, - date) %>%
    mutate(overtopped = flow >= 100) %>%
    select(-flow) %>%
    spread(bypass, overtopped) |>
    mutate(year = year(date),
           month = month(date)) |>
    # filter(between(year, 1980, 2000)) |>
    select(-date) |>
    gather(bypass, overtopped, -month, -year) |>
    spread(year, overtopped) |>
    arrange(bypass, month)  |>
    select(-month, -bypass) |>
    as.matrix()

  gates_overtopped <- array(NA, dim = c(12, 21, 2))
  dimnames(gates_overtopped) <- list(month.abb[1:12], 1980:2000, c('Sutter Bypass', 'Yolo Bypass'))
  gates_overtopped[ , , 1] <- bypass_overtopped[1:12, ]
  gates_overtopped[ , , 2] <- bypass_overtopped[13:24, ]

  return(gates_overtopped)
}

gates_overtopped_2008_2009 <- generate_gates_overtopped(calsim_2008_2009)
gates_overtopped_2019_biop_itp <- generate_gates_overtopped(calsim_2019_biop_itp)
gates_overtopped_run_of_river <- generate_gates_overtopped(calsim_run_of_river)
gates_overtopped_lto_12a <- generate_gates_overtopped(lto_propq_data)

gates_overtopped <- list(biop_2008_2009 = gates_overtopped_2008_2009,
                         biop_itp_2018_2019 = gates_overtopped_2019_biop_itp,
                         run_of_river = gates_overtopped_run_of_river,
                         LTO_12a = gates_overtopped_lto_12a)
# LTO action 5 scenario
gates_overtopped$action_5 <- action_5$gates_overtopped

usethis::use_data(gates_overtopped, overwrite = TRUE)

# Delta Routing Flows ----------------------------------------------------------

# calsim3 nodes
calsim3_delta_routing <- data.frame(input=c("freeport_flows", "vernalis_flows", "stockton_flows",
                   rep("CVP_exports", 2), rep("SWP_exports",3)),
           nodes=c("C_SAC041", "C_SJR070", "C_SJR053A",
                   "DEL_CVP_TOTAL_N", "DEL_CVP_TOTAL_S",
                   "DEL_SWP_PMI", "DEL_SWP_PAG", "DEL_SWP_PIN"),
           type=c(rep("CHANNEL",3), rep("DELIVERY-CVP",2),
                  rep("DELIVERY-SWP",3)))

# wilkins flow -----------------------------------------------------------------
# Adds wilkins flow node to replace freeport flow
# I used node C129 for wilkins(Cyril recommended C129)
wilkins_node <- "C129"

generate_wilkins_flow <- function(calsim_data, wilkins_node) {
  wilkins_flow <- calsim_data |>
    select(date, wilkins_node) |>
    filter(year(date) >= 1980, year(date) <= 2000) |>
    transmute(
      year = year(date),
      month = month(date),
      wilklinsQcfs = C129,
      wilkinsQcms = cfs_to_cms(C129))  |>
    select(year, month, wilkinsQcms) |>
    pivot_wider(names_from = year,
                values_from = wilkinsQcms) |>
    select(-month) |>
    as.matrix()

  rownames(wilkins_flow) <- month.abb

  return(wilkins_flow)
}

wilkins_flow_2008_2009 <- generate_wilkins_flow(calsim_2008_2009, wilkins_node)
wilkins_flow_2019_biop_itp <- generate_wilkins_flow(calsim_2019_biop_itp, wilkins_node)
wilkins_flow_run_of_river <- generate_wilkins_flow(calsim_run_of_river, wilkins_node)

# calsim 3
wilkins_flow_lto_12a <- calsim3_data |> filter(node == "C_SAC129", year(date) >= 1980, year(date) <= 2000) |>
  transmute(
    year = year(date),
    month = month(date),
    wilkinsQcms = cfs_to_cms(flow_cfs)
  ) |>
  pivot_wider(names_from = year,
              values_from = wilkinsQcms) |>
  select(-month) |>
  as.matrix()


rownames(wilkins_flow_lto_12a) <- month.abb


wilkins_flow <- list(biop_2008_2009 = wilkins_flow_2008_2009,
                     biop_itp_2018_2019 = wilkins_flow_2019_biop_itp,
                     run_of_river = wilkins_flow_run_of_river,
                     LTO_12a = wilkins_flow_lto_12a)
# LTO action 5 scenario
# wilkins_flow$action_5 <- action_5$wilkins_flow # TODO: not yet available

usethis::use_data(wilkins_flow, overwrite = TRUE)

# freeport flow ----------------------------------------------------------------
freeport_node <- "C400"

generate_freeport_flow <- function(calsim_data, freeport_node) {
  freeport_flow <- calsim_data |>
    select(date, freeport_node) |>
    filter(
      year(date) >= 1980, year(date) <= 2000) |>
    transmute(
      year = year(date),
      month = month(date),
      freeportQcfs = C400,
      freeportQcms = cfs_to_cms(C400)
    ) |>
    select(year, month, freeportQcms) |>
    pivot_wider(names_from = year,
                values_from = freeportQcms) |>
    select(-month) |>
    as.matrix()

  rownames(freeport_flow) <- month.abb

  return(freeport_flow)
}

freeport_flow_2008_2009 <- generate_freeport_flow(calsim_2008_2009, freeport_node)
freeport_flow_2019_biop_itp <- generate_freeport_flow(calsim_2019_biop_itp, freeport_node)
freeport_flow_run_of_river <- generate_freeport_flow(calsim_run_of_river, freeport_node)

freeport_flow_lto_12a <- calsim3_data |> filter(node == "C_SAC041", year(date) >= 1980, year(date) <= 2000) |>
  transmute(
    year = year(date),
    month = month(date),
    wilkinsQcms = cfs_to_cms(flow_cfs)
  ) |>
  pivot_wider(names_from = year,
              values_from = wilkinsQcms) |>
  select(-month) |>
  as.matrix()

rownames(freeport_flow_lto_12a) <- month.abb

freeport_flow <- list(biop_2008_2009 = freeport_flow_2008_2009,
                      biop_itp_2018_2019 = freeport_flow_2019_biop_itp,
                      run_of_river = freeport_flow_run_of_river,
                      LTO_12a = freeport_flow_lto_12a)
# LTO action 5 scenario
freeport_flow$action_5 <- action_5$freeport_flow

usethis::use_data(freeport_flow, overwrite = TRUE)

# vernalis flow ----------------------------------------------------------------
vernalis_node <- "C639"

generate_vernalis_flow <- function(calsim_data, vernalis_node) {
  vernalis_flow <- calsim_data  |>
    select(date, vernalis_node) |>
    filter(
      year(date) >= 1980, year(date) <= 2000) |>
    transmute(
      year = year(date),
      month = month(date),
      vernalisQcfs = C639,
      vernalisQcms = cfs_to_cms(C639)
    ) |>
    select(year, month, vernalisQcms) |>
    pivot_wider(names_from = year,
                values_from = vernalisQcms) |>
    select(-month) |>
    as.matrix()

  rownames(vernalis_flow) <- month.abb

  return(vernalis_flow)
}

vernalis_flow_2008_2009 <- generate_vernalis_flow(calsim_2008_2009, vernalis_node)
vernalis_flow_2019_biop_itp <- generate_vernalis_flow(calsim_2019_biop_itp, vernalis_node)
vernalis_flow_run_of_river <- generate_vernalis_flow(calsim_run_of_river, vernalis_node)

vernalis_flow_lto_12a <- calsim3_data |> filter(node == "C_SJR070", year(date) >= 1980, year(date) <= 2000) |>
  transmute(
    year = year(date),
    month = month(date),
    wilkinsQcms = cfs_to_cms(flow_cfs)
  ) |>
  pivot_wider(names_from = year,
              values_from = wilkinsQcms) |>
  select(-month) |>
  as.matrix()

rownames(vernalis_flow_lto_12a) <- month.abb


vernalis_flow <- list(biop_2008_2009 = vernalis_flow_2008_2009,
                      biop_itp_2018_2019 = vernalis_flow_2019_biop_itp,
                      run_of_river = vernalis_flow_run_of_river,
                      LTO_12a = vernalis_flow_lto_12a)
# LTO action 5 scenario
vernalis_flow$action_5 <- action_5$vernalis_flow

usethis::use_data(vernalis_flow, overwrite = TRUE)

# stockton flow ----------------------------------------------------------------
stockton_node <- "C417A"

generate_stockton_flow <- function(calsim_data, stockton_node) {
  stockton_flow <- calsim_data |>
    select(date, stockton_node) |>
    filter(
      year(date) >= 1980, year(date) <= 2000) |>
    transmute(
      year = year(date),
      month = month(date),
      stocktonQcfs = C417A,
      stocktonQcms = cfs_to_cms(C417A)
    ) |>
    select(year, month, stocktonQcms) |>
    pivot_wider(names_from = year,
                 values_from = stocktonQcms) |>
    select(-month) |>
    as.matrix()

  rownames(stockton_flow) <- month.abb

  return(stockton_flow)
}

stockton_flow_2008_2009 <- generate_stockton_flow(calsim_2008_2009, stockton_node)
stockton_flow_2019_biop_itp <- generate_stockton_flow(calsim_2019_biop_itp, stockton_node)
stockton_flow_run_of_river <- generate_stockton_flow(calsim_run_of_river, stockton_node)

stockton_flow_lto_12a <- calsim3_data |> filter(node == "C_SJR053A", year(date) >= 1980, year(date) <= 2000) |>
  transmute(
    year = year(date),
    month = month(date),
    wilkinsQcms = cfs_to_cms(flow_cfs)
  ) |>
  pivot_wider(names_from = year,
              values_from = wilkinsQcms) |>
  select(-month) |>
  as.matrix()

rownames(stockton_flow_lto_12a) <- month.abb

stockton_flow <- list(biop_2008_2009 = stockton_flow_2008_2009,
                      biop_itp_2018_2019 = stockton_flow_2019_biop_itp,
                      run_of_river = stockton_flow_run_of_river,
                      LTO_12a = stockton_flow_lto_12a)
# LTO action 5 scenario
stockton_flow$action_5 <- action_5$stockton_flow

usethis::use_data(stockton_flow, overwrite = TRUE)


# cvp exports ------------------------------------------------------------------
generate_cvp_exports <- function(calsim_data) {
  cvp_exports <- calsim_data |>
    select(date, DEL_CVP_TOTAL) |>
    filter(
      year(date) >= 1980, year(date) <= 2000) |>
    transmute(
      date = date,
      year = year(date),
      month = month(date),
      cvpExportsQcfs = DEL_CVP_TOTAL,
      cvpExportsQcms = cfs_to_cms(DEL_CVP_TOTAL)
    ) |>
    select(year, month, cvpExportsQcms) |>
    pivot_wider(names_from = year,
                values_from = cvpExportsQcms) |>
    select(-month) |>
    as.matrix()

  rownames(cvp_exports) <- month.abb

  return(cvp_exports)
}

cvp_exports_2008_2009 <- generate_cvp_exports(calsim_2008_2009)
cvp_exports_2019_biop_itp <- generate_cvp_exports(calsim_2019_biop_itp)
cvp_exports_run_of_river <- generate_cvp_exports(calsim_run_of_river)

# calsim 3
cvp_exports_lto <- calsim3_data |> filter(node %in% c("DEL_CVP_TOTAL_N", "DEL_CVP_TOTAL_S"),
                       year(date) >= 1980, year(date) <= 2000) |>
  group_by(date = as_date(date)) |>
  summarise(
    cvpExportsQcms = cfs_to_cms(sum(flow_cfs))
  ) |> ungroup() |>
  transmute(year =year(date), month = month(date), cvpExportsQcms) |>
  pivot_wider(names_from = year,
              values_from = cvpExportsQcms) |>
  select(-month) |>
  as.matrix()

rownames(cvp_exports_lto) <- month.abb

cvp_exports <- list(biop_2008_2009 = cvp_exports_2008_2009,
                    biop_itp_2018_2019 = cvp_exports_2019_biop_itp,
                    run_of_river = cvp_exports_run_of_river,
                    LTO_12a = cvp_exports_lto)
# LTO action 5 scenario
cvp_exports$action_5 <- action_5$cvp_exports

usethis::use_data(cvp_exports, overwrite = TRUE)

# swp exports ------------------------------------------------------------------
generate_swp_exports <- function(calsim_data) {
  swp_exports <- calsim_data |>
    select(date, DEL_SWP_TOTAL) |>
    filter(
      year(date) >= 1980, year(date) <= 2000) |>
    transmute(
      date = date,
      year = year(date),
      month = month(date),
      swpExportsQcfs = DEL_SWP_TOTAL,
      swpExportsQcms = cfs_to_cms(DEL_SWP_TOTAL)
    ) |>
    select(year, month, swpExportsQcms) |>
    pivot_wider(names_from = year,
                values_from = swpExportsQcms) |>
    select(-month) |>
    as.matrix()

  rownames(swp_exports) <- month.abb

  return(swp_exports)
}

swp_exports_2008_2009 <- generate_swp_exports(calsim_2008_2009)
swp_exports_2019_biop_itp <- generate_swp_exports(calsim_2019_biop_itp)
swp_exports_run_of_river <- generate_swp_exports(calsim_run_of_river)

swp_exports_lto <- calsim3_data |> filter(node %in% c("DEL_SWP_PMI", "DEL_SWP_PAG", "DEL_SWP_PIN"),
                                          year(date) >= 1980, year(date) <= 2000) |>
  group_by(date = as_date(date)) |>
  summarise(
    swpExportsQcms = cfs_to_cms(sum(flow_cfs))
  ) |> ungroup() |>
  transmute(year =year(date), month = month(date), swpExportsQcms) |>
  pivot_wider(names_from = year,
              values_from = swpExportsQcms) |>
  select(-month) |>
  as.matrix()

rownames(swp_exports_lto) <- month.abb

swp_exports <- list(biop_2008_2009 = swp_exports_2008_2009,
                    biop_itp_2018_2019 = swp_exports_2019_biop_itp,
                    run_of_river = swp_exports_run_of_river,
                    LTO_12a = swp_exports_lto)
# LTO action 5 scenario
swp_exports$action_5 <- action_5$swp_exports

usethis::use_data(swp_exports, overwrite = TRUE)

