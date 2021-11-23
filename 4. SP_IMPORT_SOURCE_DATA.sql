/**
** Creation Date: 22-NOV-2021
** Author: Nirantar Seewooruttun
**/

CREATE PROCEDURE SP_IMPORT_SOURCE_DATA
  
AS

DECLARE
       @errfilename_country varchar(100), 
       @errfilename_time varchar(100),
       @errfilename_cpi varchar(100),
       @errfilename_wdidata varchar(100),
       @strsql varchar(max);

       SET @errfilename_country = 'C:\sampledata\WDICountry_ERROR_' + CONVERT(varchar, GETDATE()) + '.csv';      
       SET @errfilename_time = 'C:\sampledata\YearList_ERROR_' + CONVERT(varchar, GETDATE()) + '.csv';
       SET @errfilename_cpi = 'C:\sampledata\CPI2020_GlobalTablesTS_210125_ERROR_' + CONVERT(varchar, GETDATE()) + '.csv';
       SET @errfilename_wdidata = 'C:\sampledata\WDIData_ERROR_' + CONVERT(varchar, GETDATE()) + '.csv';

BEGIN TRY

    SET NOCOUNT ON

    -- Country dimension source data import

    SET @strsql = 'TRUNCATE TABLE STG_WDICOUNTRY;'

    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'Country dimension : Step 1 - Truncate staging table for source data import'

    SET @strsql = 'BULK INSERT STG_WDICOUNTRY
                   FROM ''C:\sampledata\WDICountry.csv''
                   WITH (
                         DATAFILETYPE = ''char'',
                         FIRSTROW = 2,
                         FIELDTERMINATOR = ''","'',
                         ROWTERMINATOR = ''\n'',
                         FIELDQUOTE=''"'', 
                         ERRORFILE = 
                        ''' + @errfilename_country + ''');'

    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'Country dimension : Step 2 - source data import'
    
    SET @strsql = 'UPDATE STG_WDICOUNTRY SET CountryCode = REPLACE(REPLACE(CountryCode,''"'',''''),''",'','''');'
    EXECUTE SP_EXECUTE_SCRIPT @strsql, @p_descrption = 'Country dimension : Step 3 - Remove leading characters'
    
    SET @strsql = 'UPDATE STG_WDICOUNTRY SET LatestTradeData = REPLACE(REPLACE(REPLACE(LatestTradeData,''"'',''''),''",'',''''),'','','''');'
    EXECUTE SP_EXECUTE_SCRIPT @strsql, @p_descrption = 'Country dimension : Step 4 - Remove trailing characters'
    
    -- Time dimension source data import

    SET @strsql = 'TRUNCATE TABLE STG_YEARS;'

    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'Time Dimension : Step 1 - Truncate staging table for source data import'

    SET @strsql = 'BULK INSERT STG_YEARS
                   FROM ''C:\sampledata\YearList.csv''
                   WITH (
                         FORMAT = ''CSV'',
                         FIRSTROW = 2,
                         FIELDTERMINATOR = '','',
                         ROWTERMINATOR = ''\n'',
                         ERRORFILE = 
                        ''' + @errfilename_time + ''');'

    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'Time Dimension : Step 2 - source data import'
    
    
    -- CPI fact source data import
    
    SET @strsql = 'TRUNCATE TABLE STG_CPIDATA;'

    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'CPI Fact : Step 1 - Truncate staging table for source data import'

    SET @strsql = 'BULK INSERT STG_CPIDATA
                   FROM ''C:\sampledata\CPI2020_GlobalTablesTS_210125.csv''
                   WITH (
                         FORMAT = ''CSV'',
                         FIRSTROW = 2,
                         FIELDTERMINATOR = '','',
                         ROWTERMINATOR = ''\n'',
                         ERRORFILE = 
                        ''' + @errfilename_cpi + ''');'

    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'CPI Fact : Step 2 - source data import'
    
    
    -- CPI fact source data import
    
    SET @strsql = 'TRUNCATE TABLE STG_WDIDATA;'

    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'WDIDATA Fact : Step 1 - Truncate staging table for source data import'
    
    SET @strsql = 'BULK INSERT STG_WDIDATA
                   FROM ''C:\sampledata\WDIData.csv''
                   WITH (
                         DATAFILETYPE = ''char'',
                         FIRSTROW = 2,
                         FIELDTERMINATOR = ''","'',
                         ROWTERMINATOR = ''\n'',
                         FIELDQUOTE=''"'',
                         ERRORFILE = 
                        ''' + @errfilename_wdidata + ''');'
                        
    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'WDIDATA Fact : Step 2 - source data import'
    
    SET @strsql = 'UPDATE STG_WDIDATA SET CountryName = REPLACE(REPLACE(CountryName,''"'',''''),''",'','''');'
    EXECUTE SP_EXECUTE_SCRIPT @strsql, @p_descrption = 'WDIDATA Fact : Step 3 - Remove leading characters'
    
    SET @strsql = 'UPDATE STG_WDIDATA SET YR2020 = REPLACE(REPLACE(REPLACE(YR2020,''"'',''''),''",'',''''),'','','''');'
    EXECUTE SP_EXECUTE_SCRIPT @strsql, @p_descrption = 'WDIDATA Fact : Step 4 - Remove trailing characters'
    
    
END TRY

BEGIN CATCH

    THROW;

END CATCH
GO