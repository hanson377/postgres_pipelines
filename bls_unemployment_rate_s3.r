## pulls seasonally adjusted unemployed rates for states, counties, and major metrocs
## after pulling, script cleans data and saves it to PostgreSQL database for easy access later


library(blsAPI)
library(DBI)
library(dplyr)
library(tidyr)
library(odbc)
library(data.table)
library(aws.s3)

## set up s3 connection
source("/Users/hanson377/Desktop/script_parameters/aws_keys.R")

Sys.setenv(
  "AWS_ACCESS_KEY_ID" = key_id,
  "AWS_SECRET_ACCESS_KEY" = access_key,
  "AWS_DEFAULT_REGION" = region
)


## now, getting to pulling some data
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

## now lets save it as a csv to local directory
write.csv(state_unemployment, file.path(tempdir(), "state_unemployment.csv"))


# Upload files to S3 bucket
put_object(
  file = file.path(tempdir(), "state_unemployment.csv"),
  object = "state_unemployment.csv",
  bucket = "testbls"
)


## see if it was there
get_bucket(bucket = "testbls")
