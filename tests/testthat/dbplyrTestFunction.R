library(dplyr)
library(testthat)
# options("DEBUG_DATABASECONNECTOR_DBPLYR" = TRUE)

testDbplyrFunctions <- function(connectionDetails, cdmDatabaseSchema) {
  assertTempEmulationSchemaSet(connectionDetails$dbms)
  
  connection <- connect(connectionDetails)
  on.exit(disconnect(connection))
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  observationPeriod <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "observation_period"))
  
  nMales <- person %>%
    filter(gender_concept_id == 8507) %>%
    count() %>%
    pull()
  expect_gt(nMales, 1)
  
  nMales2 <- person %>%
    filter(gender_concept_id %in% c(8507)) %>%
    count() %>%
    pull()
  expect_gt(nMales2, 1)
  
  personSample <- person %>%
    slice_sample(n = 10) %>%
    collect()
  expect_equal(nrow(personSample), 10)
  
  sexString <- person %>%
    mutate(sex = ifelse(.data$gender_concept_id == 8507, "Male",
                        ifelse(.data$gender_concept_id == 8532, "Female", NA)
    )) %>%
    select("person_id", "sex") %>%
    head() %>%
    collect()
  expect_true(all(sexString$sex %in% c("Male", "Female")))
  
  cars2 <- copy_to(connection, cars, overwrite = TRUE)
  cars2 <- cars2 %>% collect()
  expect_equivalent(arrange(cars, speed, dist), arrange(cars2, speed, dist))
  
  nObsOverOneYear <- observationPeriod %>%
    filter(dateDiff("day", observation_period_start_date, observation_period_end_date) > 365) %>%
    count() %>%
    pull()
  
  expect_gt(nObsOverOneYear, 1)
  
  tempTable <- person %>%
    filter(gender_concept_id == 8507) %>%
    compute()
  nMales2 <- tempTable %>%
    count() %>%
    pull()
  expect_gt(nMales2, 1)
  
  longestObsPeriod <- observationPeriod %>%
    mutate(duration = dateDiff("day", observation_period_start_date, observation_period_end_date)) %>%
    arrange(desc(duration)) %>%
    relocate(duration) %>%
    head(1) %>%
    collect()
  expect_gt(longestObsPeriod$duration, 1)
  expect_equal(which(names(longestObsPeriod) == "duration"), 1)
  
  topAges <- person %>%
    inner_join(observationPeriod, by = "person_id") %>%
    mutate(age = year(observation_period_start_date) - year_of_birth) %>%
    distinct(age) %>%
    rename(person_age = age) %>%
    arrange(desc(person_age)) %>%
    head(10) %>%
    collect()
  expect_equal(nrow(topAges), 10)
  
  # Casting duration to numeric because platforms like SQL Server compute the mean by first computing the sum, which
  # will not fit in an integer:
  durationDist <- person %>%
    inner_join(observationPeriod, by = "person_id") %>%
    mutate(duration = dateDiff("day", observation_period_start_date, observation_period_end_date)) %>%
    group_by(gender_concept_id) %>%
    summarize(mean_duration = mean(duration, na.rm = TRUE),
              min_duration = min(duration, na.rm = TRUE),
              max_duration = max(duration, na.rm = TRUE),
              count_duration = n()) %>%
    collect()
  
  expect_equal(nrow(durationDist), 2)
  
  resultOfAntiJoin <- observationPeriod %>% 
    anti_join(
      person %>%
        filter(!is.null(race_concept_id)),
      by = "person_id"
    ) %>%
    group_by(period_type_concept_id) %>%
    summarize(value_count = n()) %>%
    collect()
  expect_s3_class(resultOfAntiJoin, "data.frame")
  
  personTwice <- person %>%
    union_all(person) %>%
    count() %>%
    collect()
  expect_gt(personTwice$n, 1)
  
  top10PersonsHardWay <- person %>%
    head(100) %>%
    mutate(rn = row_number(person_id)) %>%
    filter(rn <= 10) %>%
    collect()
  expect_equal(nrow(top10PersonsHardWay), 10)
  
  dataWithNa <- cars
  dataWithNa$speed[2] <- NA
  dataWithNa <- copy_to(connection, dataWithNa, overwrite = TRUE)
  filteredRow <- dataWithNa %>%
    filter(is.na(speed)) %>%
    collect()
  expect_equal(nrow(filteredRow), 1)
  
  # dumbNameCars <- cars
  # names(dumbNameCars) <- c("Car speed", "Dist. to Stop")
  # copy_to(connection, dumbNameCars, name = "dn_cars")
  
  dropEmulatedTempTables(connection)
  # disconnect(connection)
}
