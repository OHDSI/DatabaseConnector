# Some code testing how other R functions deal with Integer 64

set.seed(0)
day.start <- "1960/01/01"
day.end <- "2000/12/31"
time.start <- as.POSIXct("2018-11-12 09:04:07 CET")
dayseq <- seq.Date(as.Date(day.start), as.Date(day.end), by = "week")
timeSeq <- time.start + (1:length(dayseq)) * 60 * 60 * 24
makeRandomStrings <- function(n = 1, lenght = 12) {
  randomString <- c(1:n)
  for (i in 1:n) randomString[i] <- paste(sample(c(0:9, letters, LETTERS), lenght, replace = TRUE),
                                          collapse = "")
  return(randomString)
}
bigInts <- bit64::runif64(length(dayseq))
data <- data.frame(startDate = dayseq,
                   someDatetime = timeSeq,
                   personId = as.integer(round(runif(length(dayseq), 1, 1e+07))),
                   value = runif(length(dayseq)),
                   id = makeRandomStrings(length(dayseq)),
                   bigInts = bigInts,
                   stringsAsFactors = FALSE)

data$startDate[4] <- NA
data$someDatetime[6] <- NA
data$personId[5] <- NA
data$value[2] <- NA
data$id[3] <- NA
data$bigInts[7] <- NA
data$bigInts[8] <- 3.3043e+10
library(bit64)

# Native R CSV files ----------------------------------------
write.csv(data, "s:/temp/data.csv", row.names = FALSE)

data2 <- read.csv("s:/temp/data.csv", stringsAsFactors = FALSE)
all.equal(data, data2)
# Error

data2 <- read.csv("s:/temp/data.csv", colClasses = c("Date", "POSIXct", "integer", "numeric", "character", "integer64"))
all.equal(data, data2)
# Note: origin of POSIxct has shifted
           
# Readr CSV files --------------------------------------------------
readr::write_csv(data, "s:/temp/data.csv")

data2 <- readr::read_csv("s:/temp/data.csv")
all.equal(data, data2)
# Error

# Currently no ability to specify an integer64 column: https://github.com/tidyverse/readr/issues/633

# RDS files --------------------------------------------------------
saveRDS(data, "s:/temp/data.rds")
data2 <- readRDS("s:/temp/data.rds")
all.equal(data, data2)
# True

