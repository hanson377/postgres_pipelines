## Generic script for scraping tweets from twitter and saving them to Postgres database for analysis

## load libraries
library(odbc)
library(DBI)
library(rtweet)
library(dplyr)

## set up post gres connections
source("/Users/hanson377/Desktop/script_parameters/economic_data.R")

con <- DBI::dbConnect(odbc::odbc(),
  driver = "PostgreSQL Driver",
  database = "twitter_data",
  UID    = pg_name,
  PWD    = pg_password,
  host = pg_host,
  port = pg_port)

##dbExecute(con,"CREATE TABLE tweets (user_id VARCHAR(10000), status_id VARCHAR(10000), created_at DATE, screen_name VARCHAR(10000), text VARCHAR(10000), source VARCHAR(10000), is_quote BOOLEAN, is_retweet BOOLEAN, country VARCHAR(10000), country_code VARCHAR(10000), location VARCHAR(10000), description VARCHAR(10000), followers_count INT, friends_count INT, account_created_at DATE, batch_date DATE, id BIGSERIAL NOT NULL PRIMARY KEY);")

## create twitter token from local function
source("/Users/hanson377/Desktop/script_parameters/create_token.R")

## create hashtag keys to look at, set stream time
keys <- "#trump,#maga,#gop,#politics"
streamtime <- 15 ## run for 60 seconds

## massage data a little
filename <- paste0("nlp_stream_",format(Sys.time(),'%d_%m_%Y__%H_%M_%S'),".json")
stream_tweets(q = keys, timeout = streamtime, file_name = filename)
data <- parse_stream(filename)

vars <- c('user_id','status_id','created_at','screen_name','text','source','is_quote','is_retweet','country','country_code','location','description','followers_count','friends_count','account_created_at')
data <- data %>% select(vars) %>% mutate(batch_date = Sys.time())

## write to database
dbWriteTable(con, "tweets", data, append = T, row.names =FALSE)
file.remove(filename)

## test that data is live
data_test <- dbGetQuery(con, "SELECT * FROM tweets LIMIT 20")
