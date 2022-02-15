COPY @sqlTableName
FROM 's3://@s3RepoName/@pathToFiles/@fileName'
CREDENTIALS 'aws_access_key_id=@awsAccessKey;aws_secret_access_key=@awsSecretAccessKey'
gzip
IGNOREHEADER AS 1 BLANKSASNULL EMPTYASNULL DELIMITER ',' csv;
