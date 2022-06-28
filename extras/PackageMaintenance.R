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

# Format and check code:
styler::style_pkg()
OhdsiRTools::checkUsagePackage("DatabaseConnector")
OhdsiRTools::updateCopyrightYearFolder()
devtools::spell_check()

# Create manual:
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

# DBI 1.1.2 and 1.1.3 are causing errors when building the package website, so downgrading to 1.1.1 for now:
require(devtools)
install_version("DBI", version = "1.1.1", repos = "http://cran.us.r-project.org")
roxygen2::roxygenize('.', roclets = c('rd', 'collate', 'namespace'))

pkgdown::build_site()
OhdsiRTools::fixHadesLogo()

# Release package:
devtools::check_win_devel()

devtools::check_rhub()

devtools::release()
