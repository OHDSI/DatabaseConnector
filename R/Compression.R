# @file InsertTable.R
#
# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of DatabaseConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Compress files and/or folders into a single zip file
#'
#' @details
#' Uses Java's compression library to create a zip file. It is similar to `utils::zip`, except
#' that it does not require an external zip tool to be available on the system path.
#'
#' @param zipFile The path to the zip file to be created.
#' @param files   The files and/or folders to be included in the zip file. Folders will be included recursively.
#' @param rootFolder  The root folder. All files will be stored with relative paths relative to this folder.
#' @param compressionLevel A number between 1 and 9. 9 compresses best, but it also takes the longest.
#'
#' @export
createZipFile <- function(zipFile, files, rootFolder = getwd(), compressionLevel = 9) {
  files <- sapply(as.character(files), normalizePath)
  suppressWarnings(zipFile <- normalizePath(as.character(zipFile), mustWork = FALSE))
  rootFolder <- normalizePath(as.character(rootFolder))
  compressionLevel <- as.integer(compressionLevel)
  rJava::J("org.ohdsi.databaseConnector.Compression")$createZipFile(
    files,
    rootFolder,
    zipFile,
    compressionLevel
  )
}
