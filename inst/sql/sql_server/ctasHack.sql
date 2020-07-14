@distribution
select @varNames
into @tempName
from
(
  @selectSqls
)
;