## pulls seasonally adjusted unemployed rates for states, counties, and major metrocs
## after pulling, script cleans data and saves it to PostgreSQL database for easy access later


library(blsAPI)
library(DBI)
library(dplyr)
library(tidyr)
library(odbc)
library(data.table)


## set up post gres connections
source("/Users/hanson377/Desktop/script_parameters/postgres_keys.R")

con <- DBI::dbConnect(odbc::odbc(),
  driver = "PostgreSQL Driver",
  database = "economic_data",
  UID    = pg_name,
  PWD    = pg_password,
  host = pg_host,
  port = pg_port)

source("/Users/hanson377/Desktop/script_parameters/bls_keys.R")

state_keys <- read.delim('https://download.bls.gov/pub/time.series/la/la.area', header = TRUE, sep = "\t")
state_keys <- subset(state_keys,area_type_code == 'A')

state_keys <- state_keys %>% select(area_text,area_code ) %>% filter(area_text != 'All States') %>% filter(area_text != 'All Metropolitan Statistical Areas')
state_list <- list(state_keys$area_code)

prefix <- 'LA'
season_adjustment <- 'U'
measure_code <- '03'
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
state_unemployment <- blsAPI(payload, api_version = 2, return_data_frame = T)
}

data <- list()

for (i in groups) {
data[[i]] = data.frame(tap_api(i))
}

state_unemployment <- rbindlist(data)

state_unemployment$area_code <- substr(state_unemployment$seriesID,4,18)
state_unemployment <- state_unemployment %>% left_join(state_keys, by='area_code')
state_unemployment <- state_unemployment %>% select(area_code,area_text,year,period,periodname=periodName,value) %>% mutate(geo_type = 'state')


## drop old table, create new one
dbExecute(con,"DROP TABLE state_unemployment_rate;")
dbWriteTable(con, "state_unemployment_rate", state_unemployment, OVERWRITE = TRUE, append = TRUE,row.names =FALSE)

## test that data is live
data_test <- dbGetQuery(con, "SELECT * FROM state_unemployment_rate limit 100")
rm(data_test)

## now do metro statistical areas
msa_keys <- read.delim('https://download.bls.gov/pub/time.series/la/la.area', header = TRUE, sep = "\t")
msa_keys <- subset(state_keys,area_type_code == 'B')

msa_keys <- msa_keys %>% select(area_code,area_text)

msa_keys$series_id <- paste(prefix,season_adjustment,msa_keys$area_code,measure_code,sep='')

msa_keys$rank <- seq(1,nrow(msa_keys),1)
msa_keys$group <- (floor(msa_keys$rank/50))+1


## define groups to run through function below
groups <- msa_keys %>% select(group)
groups <- unique(groups)
groups <- groups[['group']]

## create function to pull the msa keys we need and the associated id and tap the bls api for such
tap_api <- function(x) {
msa_keys <- subset(msa_keys, group == x)
series_id <- msa_keys[['series_id']]

payload <- list(
'seriesid' = series_id,
'startyear' = 2005,
'endyear' = 2021,
'registrationKey' = API_Key)
msa_unemployment <- blsAPI(payload, api_version = 2, return_data_frame = T)
}

data <- list()

for (i in groups) {s
data[[i]] = data.frame(tap_api(i))
}

msa_unemployment <- rbindlist(data)

msa_unemployment$area_code <- substr(msa_unemployment$seriesID,4,18)
msa_unemployment <- msa_unemployment %>% left_join(msa_keys, by='area_code')
msa_unemployment <- msa_unemployment %>% select(area_code,area_text,year,period,periodname=periodName,value) %>% mutate(geo_type = 'msa')
msa_unemployment$area_text <- gsub(',','',msa_unemployment$area_text)

## drop old table, create new one
dbExecute(con,"DROP TABLE msa_unemployment;")
dbWriteTable(con, "msa_unemployment", msa_unemployment, OVERWRITE = TRUE, append = TRUE,row.names =FALSE)

## test that data is live
data_test <- dbGetQuery(con, "SELECT * FROM msa_unemployment limit 100")
rm(data_test)


## now lets do county
county_keys <- read.delim('https://download.bls.gov/pub/time.series/la/la.area', header = TRUE, sep = "\t")
county_keys <- subset(state_keys,area_type_code == 'F')

county_keys$series_id <- paste(prefix,season_adjustment,county_keys$area_code,measure_code,sep='')

county_keys$rank <- seq(1,nrow(county_keys),1)
county_keys$group <- (floor(county_keys$rank/50))+1


## define groups to run through function below
groups <- county_keys %>% select(group)
groups <- unique(groups)
groups <- groups[['group']]

## create function to pull the county keys we need and the associated id and tap the bls api for such
tap_api <- function(x) {
county_keys <- subset(county_keys, group == x)
county_series_id <- county_keys[['series_id']]

payload <- list(
'seriesid' = county_series_id,
'startyear' = 2005,
'endyear' = 2020,
'registrationKey' = API_Key)
county_unemployment <- blsAPI(payload, api_version = 2, return_data_frame = T)
}

data <- list()
for (i in groups) {
data[[i]] = data.frame(tap_api(i))
}

county_unemployment <- rbindlist(data)

county_unemployment$area_code <- substr(county_unemployment$seriesID,4,18)
county_unemployment <- county_unemployment %>% left_join(county_keys, by='area_code')
county_unemployment <- county_unemployment %>% select(area_code,area_text,year,period,periodname=periodName,value) %>% mutate(geo_type = 'county')


## drop old table, create new one
dbExecute(con,"DROP TABLE county_unemployment;")
dbWriteTable(con, "county_unemployment", county_unemployment, OVERWRITE = TRUE, append = TRUE,row.names =FALSE)

## test that data is live
data_test <- dbGetQuery(con, "SELECT * FROM county_unemployment limit 100")
rm(data_test)
