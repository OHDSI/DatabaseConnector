COPY @qname
FROM 's3://@s3RepoName/@pathToFiles/@fileName.gz'
CREDENTIALS 'aws_access_key_id=@awsAccessKey;aws_secret_access_key=@awsSecretAccessKey'
IGNOREHEADER AS 1 BLANKSASNULL EMPTYASNULL DELIMITER ',' csv quote as '`';