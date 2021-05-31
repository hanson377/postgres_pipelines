library(dplyr)

laborParticipationRate <- dbGetQuery(con, "SELECT * FROM state_labor_participation_rate")
laborParticipationRate <- laborParticipationRate %>% select(state = area_text,year,month = period,participation_rate=value)

laborForcePopulation <- dbGetQuery(con, "SELECT * FROM state_labor_force_population")
laborForcePopulation <- laborForcePopulation %>% select(state = area_text,year,month = period,labor_force_population=value)

unemploymentRate <- dbGetQuery(con, "SELECT * FROM state_unemployment_rate")
unemploymentRate <- unemploymentRate %>% select(state = area_text,year,month = period,unemployment_rate=value)

unemploymentPopulation <- dbGetQuery(con, "SELECT * FROM unemployment_population")
unemploymentPopulation <- unemploymentPopulation %>% select(state = area_text,year,month = period,unemployment_population=value)

employmentPopulationRate <- dbGetQuery(con, "SELECT * FROM employment_population_rate")
employmentPopulationRate <- employmentPopulationRate %>% select(state = area_text,year,month = period,employment_population_rate=value)

employmentPopulation <- dbGetQuery(con, "SELECT * FROM employment_population")
employmentPopulation <- employmentPopulation %>% select(state = area_text,year,month = period,employment_population=value)

civilianPopulation <- dbGetQuery(con, "SELECT * FROM civilian_population")
civilianPopulation <- civilianPopulation %>% select(state = area_text,year,month = period,civilian_population=value)

##
data <- unemploymentRate %>%
  left_join(laborParticipationRate,by=c('year','month','state')) %>%
  left_join(laborForcePopulation,by=c('year','month','state')) %>%
  left_join(employmentPopulationRate,by=c('year','month','state')) %>%
  left_join(unemploymentPopulation,by=c('year','month','state')) %>%
  left_join(civilianPopulation,by=c('year','month','state')) %>%
  left_join(employmentPopulation,by=c('year','month','state'))

## create data variable
data$month <- substr(data$month,2,3)
data$date <- paste(data$year,'-',data$month,'-01',sep='')
data$date <- as_date(data$date)

## convert numerics
data$unemployment_rate <- as.numeric(data$unemployment_rate)
data$participation_rate <- as.numeric(data$participation_rate)
data$employment_population_rate <- as.numeric(data$employment_population_rate)

data$labor_force_population <- as.numeric(data$labor_force)
data$unemployment_population <- as.numeric(data$unemployment_population)
data$civilian_population <- as.numeric(data$civilian_population)
data$employment_population <- as.numeric(data$employment_population)

## clean up
data <- data %>% select(date,year,month,state,unemployment_rate,participation_rate,employment_population_rate,labor_force_population,employment_population,unemployment_population,civilian_population)


## set up post gres connections
source("/Users/hanson377/Desktop/script_parameters/postgres_keys.R")

con <- DBI::dbConnect(odbc::odbc(),
  driver = "PostgreSQL Driver",
  database = "economic_data",
  UID    = pg_name,
  PWD    = pg_password,
  host = pg_host,
  port = pg_port)


  ## drop old table, create new one
  dbExecute(con,"DROP TABLE monthly_state_data;")
  dbWriteTable(con, "monthly_state_data", data, OVERWRITE = TRUE, append = TRUE,row.names =FALSE)

  ## test that data is live
  data_test <- dbGetQuery(con, "SELECT * FROM monthly_state_data")
  rm(data_test)
