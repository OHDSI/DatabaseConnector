# Create the package data containing SQL reserved words
sqlReservedWords <- readr::read_csv("dev/sql_reserved_words.csv")
sqlReservedWords <- unique(sqlReservedWords$reserved_words)
usethis::use_data(sqlReservedWords, overwrite = T)
