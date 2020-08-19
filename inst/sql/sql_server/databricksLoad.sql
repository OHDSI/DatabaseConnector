DROP TABLE if exists @stagingDatabaseSchema.@tableName;

create table @stagingDatabaseSchema.@tableName
(
  @tableDdl
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
;

load data  inpath '/@rootFolder/@fileName.txt' into table @stagingDatabaseSchema.@tableName;