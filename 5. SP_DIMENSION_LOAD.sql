CREATE PROCEDURE SP_DIMENSION_LOAD

AS

DECLARE @strsql varchar(max);

BEGIN TRY

-- import country dimension

SET @strsql = 'MERGE DW_DIM_COUNTRY C
               USING STG_WDICOUNTRY STG ON C.CountryCode = STG.CountryCode
               WHEN MATCHED THEN
                    UPDATE SET ShortName = STG.ShortName,
                               TableName = STG.Tablename,
                               LongName = STG.LongName,
                               Region = STG.Region
               WHEN NOT MATCHED BY TARGET AND STG.CountryCode in (SELECT CountryCode FROM REF_REPORTING_COUNTRY) THEN
                    INSERT(CountryCode, ShortName, TableName, LongName, Region)
                    VALUES(STG.CountryCode, STG.ShortName, STG.TableName, STG.LongName, STG.Region);'

EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'Country dimension : Step 5 - Load dimension data from staging area'
    
    -- manual country import for countries not present in STG_WDICOUNTRY
SET @strsql = 'MERGE DW_DIM_COUNTRY C
               USING (SELECT ''Fin'' as CountryCode, ''Finland'' as ShortName, ''Finland'' as TableName, ''Finland'' as LongName, ''Northern Europe'' as Region ) STG ON C.CountryCode = STG.CountryCode
               WHEN MATCHED THEN
                    UPDATE SET ShortName = STG.ShortName,
                               TableName = STG.Tablename,
                               LongName = STG.LongName,
                               Region = STG.Region
               WHEN NOT MATCHED BY TARGET AND STG.CountryCode in (SELECT CountryCode FROM REF_REPORTING_COUNTRY) THEN
                    INSERT(CountryCode, ShortName, TableName, LongName, Region)
                    VALUES(STG.CountryCode, STG.ShortName, STG.TableName, STG.LongName, STG.Region);'

EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'Country dimension : Step 6 - Manual country import'

-- import time dimension

SET @strsql = 'MERGE DW_DIM_TIME T
               USING STG_YEARS STG ON T.Year = STG.Year
               WHEN MATCHED THEN
                    UPDATE SET StartDate = cast(STG.startDate as date)
               WHEN NOT MATCHED BY TARGET THEN
                    INSERT(Year, StartDate)
                    VALUES(cast(STG.Year as numeric(10)), cast(STG.StartDate as date));'

EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'Time dimension : Step 3 - Load dimension data from staging area'

END TRY

BEGIN CATCH

  THROW

END CATCH

