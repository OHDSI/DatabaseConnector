library(dplyr)
library(testthat)
# options("DEBUG_DATABASECONNECTOR_DBPLYR" = TRUE)

testDbplyrFunctions <- function(connectionDetails, cdmDatabaseSchema) {
  assertTempEmulationSchemaSet(connectionDetails$dbms)
  
  connection <- connect(connectionDetails)
  on.exit(dropEmulatedTempTables(connection))
  on.exit(disconnect(connection), add = TRUE)
  
  person <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "person"))
  observationPeriod <- tbl(connection, inDatabaseSchema(cdmDatabaseSchema, "observation_period"))
  
  # Test filter, arrange, relocate, distinct -----------------------------------
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
  
  longestObsPeriod <- observationPeriod %>%
    mutate(duration = dateDiff("day", observation_period_start_date, observation_period_end_date)) %>%
    arrange(desc(duration)) %>%
    # relocate(duration) %>% # relocate of field containing custom function no longer works in dbplr 2.3.1.
    head(1) %>%
    collect()
  expect_gt(longestObsPeriod$duration, 1)
  
  topAges <- person %>%
    inner_join(observationPeriod, by = "person_id") %>%
    mutate(age = year(observation_period_start_date) - year_of_birth) %>%
    distinct(age) %>%
    rename(person_age = age) %>%
    arrange(desc(person_age)) %>%
    head(10) %>%
    collect()
  expect_gt(nrow(topAges), 1)
  
  # Test copy_inline -----------------------------------------------------------
  rows <- dbplyr::copy_inline(connection, mtcars) %>% 
    filter(hp > 200) %>%
    arrange(wt, mpg) %>%
    collect()
  rows2 <- mtcars %>% 
    filter(hp > 200) %>%
    arrange(wt, mpg) %>%
    collect()
  expect_equivalent(rows, rows2, tolerance = 1e-6)

  # Test slicing ---------------------------------------------------------------
  personSample <- person %>%
    slice_sample(n = 10) %>%
    relocate(care_site_id) %>%
    collect()
  expect_equal(nrow(personSample), 10)
  expect_equal(which(names(personSample) == "care_site_id"), 1)
  
  # Test ifelse ----------------------------------------------------------------
  sexString <- person %>%
    mutate(sex = ifelse(.data$gender_concept_id == 8507, "Male",
                        ifelse(.data$gender_concept_id == 8532, "Female", NA)
    )) %>%
    select("person_id", "sex") %>%
    head() %>%
    collect()
  expect_true(all(sexString$sex %in% c("Male", "Female")))
  
  # Test creation of temp tables -----------------------------------------------
  cars2 <- copy_to(connection, cars, overwrite = TRUE)
  cars2 <- cars2 %>% collect()
  expect_equivalent(arrange(cars, speed, dist), arrange(cars2, speed, dist))
  
  tempTable <- person %>%
    filter(gender_concept_id == 8507) %>%
    compute()
  nMales2 <- tempTable %>%
    count() %>%
    pull()
  expect_gt(nMales2, 1)
  
  dataWithNa <- cars
  dataWithNa$speed[2] <- NA
  dataWithNa <- copy_to(connection, dataWithNa, overwrite = TRUE)
  filteredRow <- dataWithNa %>%
    filter(is.na(speed)) %>%
    collect()
  expect_equal(nrow(filteredRow), 1)
  
  aPersonId <- person %>%
    head(1) %>%
    pull(person_id)
  localTable = tibble(person_id = aPersonId, person_name = "Pedro")
  remoteTable <- copy_to(connection, localTable, overwrite = TRUE)
  result <- remoteTable %>%
    left_join(person, by = join_by(person_id)) %>%
    collect()
  expect_equal(result$person_name, "Pedro")
  
  # Test joins and unions ------------------------------------------------------
  
  # Casting duration to numeric because platforms like SQL Server compute the mean by first computing the sum, which
  # will not fit in an integer:
  durationDist <- person %>%
    inner_join(observationPeriod, by = "person_id") %>%
    mutate(duration = as.numeric(dateDiff("day", observation_period_start_date, observation_period_end_date))) %>%
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
  
  # Skipping until DatabaseConnector 7 or a new dbplyr version. See #271
  # tripleJoin <- person %>%
  #   left_join(observationPeriod, by = join_by(person_id)) %>%
  #   left_join(observationPeriod %>%
  #               select(person_id, dummy = observation_period_start_date),
  #             by = join_by(person_id)) %>%
  #   collect()
  # expect_gt(nrow(tripleJoin), 0)
  
  # Test row_number ------------------------------------------------------------
  top10PersonsHardWay <- person %>%
    mutate(rn = row_number(person_id)) %>%
    filter(rn <= 10) %>%
    collect()
  expect_equal(nrow(top10PersonsHardWay), 10)
  
  # Test date functions --------------------------------------------------------
  nObsOverOneYear <- observationPeriod %>%
    filter(dateDiff("day", observation_period_start_date, observation_period_end_date) > 365) %>%
    count() %>%
    pull()
  
  expect_gt(nObsOverOneYear, 1)
  
  testData <- observationPeriod %>%
    mutate(plus_one_date = dateAdd("day", 1, observation_period_start_date),
           end_of_month_date = eoMonth(observation_period_start_date),
           obs_year = year(observation_period_start_date),
           obs_month = month(observation_period_start_date),
           obs_day = day(observation_period_start_date)) %>%
    mutate(is_later = if_else(plus_one_date > observation_period_start_date, 1, 0)) %>%
    head(1) %>%
    collect()
  expect_equal(as.Date(testData$plus_one_date), dateAdd("day", 1, testData$observation_period_start_date))
  expect_equal(testData$end_of_month_date, eoMonth(testData$observation_period_start_date))
  expect_equal(testData$obs_year, year(testData$observation_period_start_date))
  expect_equal(testData$obs_month, month(testData$observation_period_start_date))
  expect_equal(testData$obs_day, day(testData$observation_period_start_date))
  expect_equal(testData$is_later, 1)

  # dumbNameCars <- cars
  # names(dumbNameCars) <- c("Car speed", "Dist. to Stop")
  # copy_to(connection, dumbNameCars, name = "dn_cars")
  
  # disconnect(connection)
  invisible(NULL)
}
