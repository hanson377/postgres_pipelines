## pulls seasonally adjusted labor force participation rates for states, counties, and major metrocs
## after pulling, script cleans data and saves it to PostgreSQL database for easy access later


library(blsAPI)
library(DBI)
library(dplyr)
library(tidyr)
library(odbc)
library(data.table)


## set up post gres connections

con <- DBI::dbConnect(odbc::odbc(),
  driver = "PostgreSQL Driver",
  database = "economic_data",
  UID    = pg_name,
  PWD    = pg_password,
  host = pg_host,
  port = pg_port)

## load api key for bls
source("/Users/hanson377/Desktop/script_parameters/bls_keys.R")

## create string for identifying data to pull

state_keys <- read.delim('https://download.bls.gov/pub/time.series/la/la.area', header = TRUE, sep = "\t")
state_keys <- subset(state_keys,area_type_code == 'A')

state_keys <- state_keys %>% select(area_text,area_code ) %>% filter(area_text != 'All States') %>% filter(area_text != 'All Metropolitan Statistical Areas')
state_list <- list(state_keys$area_code)

prefix <- 'LA'
season_adjustment <- 'U'
measure_code <- '07'
state_keys$series_id <- paste(prefix,season_adjustment,state_keys$area_code,measure_code,sep='')

state_series_id <- state_keys[['series_id']]


state_keys$rank <- seq(1,nrow(state_keys),1)
state_keys$group <- (floor(state_keys$rank/50))+1


## define groups to run through function below
groups <- state_keys %>% select(group)
groups <- unique(groups)
groups <- groups[['group']]

## create function to pull the state keys we need and the associated id and tap the bls api for such
tap_api <- function(x) {
state_keys <- subset(state_keys, group == x)
series_id <- state_keys[['series_id']]

payload <- list(
'seriesid' = series_id,
'startyear' = 2005,
'endyear' = 2021,
'registrationKey' = API_Key)
employment_population_rate <- blsAPI(payload, api_version = 2, return_data_frame = T)
}

data <- list()

for (i in groups) {
data[[i]] = data.frame(tap_api(i))
}

employment_population_rate <- rbindlist(data)

employment_population_rate$area_code <- substr(employment_population_rate$seriesID,4,18)
employment_population_rate <- employment_population_rate %>% left_join(state_keys, by='area_code')
employment_population_rate <- employment_population_rate %>% select(area_code,area_text,year,period,periodname=periodName,value) %>% mutate(geo_type = 'state')


## drop old table, create new one
dbExecute(con,"DROP TABLE employment_population_rate;")
dbWriteTable(con, "employment_population_rate", employment_population_rate, OVERWRITE = TRUE, append = TRUE,row.names =FALSE)

## test that data is live
data_test <- dbGetQuery(con, "SELECT * FROM employment_population_rate")
rm(data_test)
