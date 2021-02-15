
library(blsAPI)
library(dplyr)
library(tidyr)
library(data.table)
library(odbc)

## connect to local postgres db
source("/Users/hanson377/Desktop/script_parameters/economic_data.R")

con <- DBI::dbConnect(odbc::odbc(),
  driver = "PostgreSQL Driver",
  database = "economic_data",
  UID    = pg_name,
  PWD    = pg_password,
  host = pg_host,
  port = pg_port)

source("/Users/hanson377/Desktop/script_parameters/bls_keys.R")

state_keys <- read.delim('https://download.bls.gov/pub/time.series/la/la.area', header = TRUE, sep = "\t")
state_keys <- state_keys %>% filter(area_type_code == 'A') %>% select(area_text,area_code )

prefix <- 'SM'
season_adjustment <- 'U'
industry <- '05000000'
area_code <- '00000'
data_type <- '03'
state_keys$series_id <- paste(prefix,season_adjustment,state_keys$state_code,area_code,industry,data_type,sep='')

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
'endyear' = 2020,
'registrationKey' = API_Key)
private_wages <- blsAPI(payload, api_version = 2, return_data_frame = T)
}

data <- list()

for (i in groups) {
data[[i]] = data.frame(tap_api(i))
}

private_wages <- rbindlist(data)

private_wages$state_code <- substr(private_wages$seriesID,4,5)
private_wages <- private_wages %>% left_join(state_keys, by='state_code')
private_wages <- private_wages %>% select(state_code,state_name,year,period,periodname=periodName,value) %>% mutate(geo_type = 'state')


## drop old table, create new one
dbExecute(con,"DROP TABLE private_wages;")
dbExecute(con,"CREATE TABLE private_wages (state_code VARCHAR(50), state_name TEXT, year INT, period VARCHAR(50),periodName VARCHAR(50),value VARCHAR(50),geo_type VARCHAR(50));")
dbWriteTable(con, "private_wages", private_wages, OVERWRITE = TRUE, append = TRUE,row.names =FALSE)

## test that data is live
data_test <- dbGetQuery(con, "SELECT * FROM private_wages")
rm(data_test)
