# @file PackageMaintenance
#
# Copyright 2022 Observational Health Data Sciences and Informatics
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

# Manually delete package from library. Avoids "Already in use" message when rebuilding
unloadNamespace("DatabaseConnector")
.rs.restartR()
folder <- system.file(package = "DatabaseConnector")
folder
unlink(folder, recursive = TRUE, force = TRUE)
file.exists(folder)

# Format and check code --------------------------------------------------------
styler::style_pkg()
OhdsiRTools::checkUsagePackage("DatabaseConnector")
OhdsiRTools::updateCopyrightYearFolder()
devtools::spell_check()

# Create manual ----------------------------------------------------------------
unlink("extras/DatabaseConnector.pdf")
shell("R CMD Rd2pdf ./ --output=extras/DatabaseConnector.pdf")

dir.create("inst/doc")
rmarkdown::render("vignettes/Connecting.Rmd",
                  output_file = "../inst/doc/Connecting.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))
unlink("inst/doc/Connecting.tex")

rmarkdown::render("vignettes/Querying.Rmd",
                  output_file = "../inst/doc/Querying.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))
unlink("inst/doc/Querying.tex")

rmarkdown::render("vignettes/DbiAndDbplyr.Rmd",
                  output_file = "../inst/doc/DbiAndDbplyr.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))
unlink("inst/doc/DbiAndDbplyr.tex")

pkgdown::build_site()
OhdsiRTools::fixHadesLogo()

# Drop all emulated temp tables that haven't been cleaned up -------------------
connection <- connect(
  dbms = "oracle",
  user = Sys.getenv("CDM5_ORACLE_USER"),
  password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
  server = Sys.getenv("CDM5_ORACLE_SERVER")
)
databaseSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
tables <- getTableNames(connection, databaseSchema, cast = "none")
sql <- paste(sprintf("DROP TABLE %s.%s;", databaseSchema, tables), collapse= "\n")
executeSql(connection, sql)
disconnect(connection)

connection <- connect(
  dbms = "postgresql",
  user = Sys.getenv("CDM5_POSTGRESQL_USER"),
  password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
  server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
)
databaseSchema <- Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA")
tables <- getTableNames(connection, databaseSchema)
sql <- paste(sprintf("DROP TABLE %s.\"%s\" CASCADE;", databaseSchema, tables), collapse= "\n")
executeSql(connection, sql)
disconnect(connection)

# Reverse dependency checks (taken from GA workflow) ---------------------------
utils::download.file("https://raw.githubusercontent.com/OHDSI/.github/main/ReverseDependencyCheckFunctions.R", "ReverseDependencyCheckFunctions.R")
source("ReverseDependencyCheckFunctions.R")
saveRDS(prepareForReverseDependencyCheck(), "reverseDependencies.rds")
reverseDependencies <- readRDS("reverseDependencies.rds")
if (nrow(reverseDependencies) > 0)
  for (i in 14:nrow(reverseDependencies))
    checkPackage(package = reverseDependencies$name[i], inCran = reverseDependencies$inCran[i])
unlink("ReverseDependencyCheckFunctions.R")
unlink("reverseDependencies.rds")

# Release package --------------------------------------------------------------
devtools::check_win_devel()

devtools::check_rhub()

devtools::release()
