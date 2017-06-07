#' Check to see if a connection to Amazon S3 is valid and usable
#'
#' @description
#' For Amazon Redshift database connections, this function will check to see if a connection to 
#' Amazon S3 is configured properly and usable. A valid AWS S3 connection is necessary to 
#' bulk load data to Redshift when using the \code{\link{insertTable}} function.
#'
#' @details
#' The function relies upon the CloudyR S3 library to test a connection to an S3 bucket using
#' AWS S3 credentials. Credentials are configured either directly into the System Environment
#' using the following keys: 
#' 
#' Sys.setenv("AWS_ACCESS_KEY_ID" = "some_access_key_id",
#'            "AWS_SECRET_ACCESS_KEY" = "some_secret_access_key",
#'            "AWS_DEFAULT_REGION" = "some_aws_region",
#'            "AWS_BUCKET_NAME" = "some_bucket_name",
#'            "AWS_OBJECT_KEY" = "some_object_key",
#'            "AWS_SSE_TYPE" = "server_side_encryption_type")
#' 
#' Alternatively, an AWS credentials file, saved locally in ~./aws/credentials file, can 
#' auto-populate these System Environment variables if it includes the following keys 
#' in the default profile:
#' 
#' [default]
#' aws_access_key_id = some_access_key_id
#' aws_secret_access_key = some_secret_access_key
#' aws_default_region = some_aws_region
#' aws_bucket_name = some_bucket_name
#' aws_object_key = some_object_key
#' aws_sse_type = server_side_encryption_type
#' 
#' @references \url{https://github.com/cloudyr/aws.s3} 
#' @references \url{https://aws.amazon.com/blogs/security/a-new-and-standardized-way-to-manage-credentials-in-the-aws-sdks/}
#' @return
#' TRUE if the Aws S3 connection is valid and usable, FALSE if not
#'
#' @export
checkAwsS3Connection <- function()
{
  checkCredentials <- function()
  {
    awsS3File <- aws.signature::read_credentials()
    if (!is.null(awsS3File))
    {
      aws.signature::use_credentials()
      
      if ("AWS_BUCKET_NAME" %in% names(awsS3File[["default"]]))
      {
        Sys.setenv(AWS_BUCKET_NAME = awsS3File[["default"]][["AWS_BUCKET_NAME"]])
      }
      if ("AWS_OBJECT_KEY" %in% names(awsS3File[["default"]]))
      {
        Sys.setenv(AWS_OBJECT_KEY = awsS3File[["default"]][["AWS_OBJECT_KEY"]])
      }
      if ("AWS_SSE_TYPE" %in% names(awsS3File[["default"]]))
      {
        Sys.setenv(AWS_SSE_TYPE = awsS3File[["default"]][["AWS_SSE_TYPE"]])
      }
    }
    
    if (Sys.getenv("AWS_ACCESS_KEY_ID") != "" && 
        Sys.getenv("AWS_SECRET_ACCESS_KEY") != "" &&
        Sys.getenv("AWS_BUCKET_NAME") != "" &&
        Sys.getenv("AWS_DEFAULT_REGION") != "")
    {
      return (TRUE)
    }
    return (FALSE)
  }
  
  if (!checkCredentials())
  {
    warning("No valid AWS S3 credentials found, will use slower SQL inserts")
    return(FALSE)
  }
  if (!aws.s3::bucket_exists(bucket = Sys.getenv("AWS_BUCKET_NAME")))
  {
    warning("No valid AWS S3 bucket found, will use slower SQL inserts")
    return (FALSE)
  }
  writeLines("AWS S3 credentials are complete and Bucket is accessible.")
  if (Sys.getenv("AWS_OBJECT_KEY") == "")
  {
    writeLines(paste0("Files will be placed at AWS S3 bucket root: ", 
                      Sys.getenv("AWS_BUCKET_NAME")))
  }
  else
  {
    writeLines(paste0("Files will be placed at AWS S3 location: ", 
                      paste(Sys.getenv("AWS_BUCKET_NAME"), 
                            Sys.getenv("AWS_OBJECT_KEY"), 
                            sep = "/", collapse = "")))
  }
  
  if (Sys.getenv("AWS_SSE_TYPE") == "")
  {
    warning("Not using Server Side Encryption for AWS S3")
  }
  return(TRUE)
}