library(testthat)

test_that("Create zip file", {

  # Create temp file to zip:
  fileToZip <- tempfile()
  zipFile <- tempfile()
  x <- data.frame(a = runif(100), b = sample(letters, 100, replace = TRUE))
  write.csv(x, fileToZip)

  # Zip:
  createZipFile(zipFile = zipFile, files = fileToZip)

  # Check if file is created and smaller than uncompressed file:
  expect_true(file.exists(zipFile))
  expect_lt(file.info(zipFile)$size, file.info(fileToZip)$size)

  # Cleanup
  unlink(fileToZip)
  unlink(zipFile)
})
