IF OBJECT_ID('@qname', 'U') IS NOT NULL
DROP TABLE @qname;

select * into @qname
from @stagingDatabaseSchema.@tableName
;