COPY INTO @sqlTableName
FROM 'abfss://@azureContainerName@@azureStorageAccount.dfs.core.windows.net/@fileName'
FILEFORMAT = CSV
FORMAT_OPTIONS (
   'header' = 'true',
   'inferSchema' = 'true'
);
