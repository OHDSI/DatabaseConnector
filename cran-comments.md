Resubmission based on previous CRAN submission comments. I've addressed all but two:

1. "Please reduce the tarball size (15587618 bytes) to less than 5 MB."

I can't reduce the size because the JDBC jar files take up this much space. Is there a way around this restriction? For example, I noticed the XLConnect package has its jar files in the XLConnectJars package.


2. "Please ensure that your functions do not write by default or in your examples/vignettes/tests in the user's home filespace. That is not allow by CRAN policies. Please only write/save files if the user has specified a directory. In your examples/vignettes/tests you can write to tempdir()."

I assume this relates to the .bulkLoadRedshift and .bulkLoadPdw functions? I've modified these to use tempdir() since these are temporary files that will be deleted at the end of the function. Is that sufficient?

---

## Test environments
* Ubuntu 14.04.5 LTS (Travis), R 3.4.4
* Windows 7, R 3.4.4

## R CMD check results

There were no ERRORs or WARNINGs. There is one note because the java directory is 15.8Mb.

## Downstream dependencies

There are no downstream dependencies.