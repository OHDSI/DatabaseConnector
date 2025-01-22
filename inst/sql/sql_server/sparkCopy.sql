COPY INTO @sqlTableName
FROM 'abfss://@azureStorageAccount.dfs.core.windows.net/@fileName'
WITH (
 CREDENTIAL (AZURE_SAS_TOKEN = '@azureAccountKey')
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
   'header' = 'true',
   'inferSchema' = 'true'
);
