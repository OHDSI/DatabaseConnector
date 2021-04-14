This update includes 2 changes and 1 bugfix since 4.0.0 (see NEWS.md)

Fixed URLs as requested. Note that the URL issues did not show when running R Check --as-cran locally, or on any of the testing servers including WinBuilder. It would be helpful to be able to detect these issues before submitting to CRAN.

---

## Test environments
* Ubuntu 20.03, R 4.0.3
* Microsoft Windows Server 2019, R 4.0.4
* MacOS, R 4.0.3
* Windows 10, R 4.0.3

## R CMD check results

There were no ERRORs or WARNINGs. 

## Downstream dependencies

- Eunomia depends on DatabaseConnector, and has been tested with this new version. No problems were found.