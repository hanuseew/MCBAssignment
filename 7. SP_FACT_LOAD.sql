CREATE PROCEDURE SP_FACT_LOAD

AS

DECLARE @strsql varchar(max);

BEGIN TRY

    SET @STRSQL = 'DROP TABLE IF EXISTS TMP_FACT_WDIDATA;'
    
    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'WDI Fact : Step 2 - Drop temporary fact table if exists' 
    
    SET @strsql = '
                SELECT CountryName, CountryCode, Year, 
                        [ER_FSH_PROD_MT] AS ER_FSH_PROD_MT,
                        [AG_LND_AGRI_K2] AS AG_LND_AGRI_K2,
                        [IC_REG_DURS] AS IC_REG_DURS,
                        [IC_BUS_NREG] AS IC_BUS_NREG,
                        [SL_AGR_EMPL_ZS] AS SL_AGR_EMPL_ZS,
                        [SL_EMP_SELF_ZS] AS SL_EMP_SELF_ZS
                    INTO  TMP_FACT_WDIDATA
                    FROM  (SELECT CountryName, CountryCode, Year, KPICODE, KPIVALUE
                            FROM VW_WDIDATA
                            WHERE 1=1
                            AND Year >= (Select Distinct YearStartLoad From REF_PARAMETERS)
                            ) V
                            PIVOT
                            (
                            SUM(KPIVALUE)
                            FOR KPICODE IN ([ER_FSH_PROD_MT],
                                            [AG_LND_AGRI_K2],
                                            [IC_REG_DURS],
                                            [IC_BUS_NREG],
                                            [SL_AGR_EMPL_ZS],
                                            [SL_EMP_SELF_ZS]
                                            )
                            ) AS PVT
                ;'
    
    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'WDI Fact : Step 3 - Load source facts into temp fact table' 
    
    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = 'TRUNCATE TABLE DW_FACT_WDIDATA;', @p_descrption = 'WDI Fact : Step 4 - Truncate fact table' 
    
    SET @strsql = 'INSERT INTO DW_FACT_WDIDATA (CountryID, TimeID, CountryCode, CountryName, Year, 
                                ER_FSH_PROD_MT, AG_LND_AGRI_K2, IC_REG_DURS, IC_BUS_NREG,  SL_AGR_EMPL_ZS, SL_EMP_SELF_ZS, CPI
                                )
                SELECT C.CountryID, T.TimeID, 
                    TMP.CountryCode, TMP.CountryName,
                    TMP.Year,
                    ISNULL(ER_FSH_PROD_MT, 0) AS ER_FSH_PROD_MT,
                    ISNULL(AG_LND_AGRI_K2, 0) AS AG_LND_AGRI_K2,
                    ISNULL(IC_REG_DURS, 0) AS IC_REG_DURS,
                    ISNULL(IC_BUS_NREG, 0) AS IC_BUS_NREG,
                    ISNULL(SL_AGR_EMPL_ZS, 0) AS SL_AGR_EMPL_ZS,
                    ISNULL(SL_EMP_SELF_ZS, 0) AS SL_EMP_SELF_ZS,
                    ISNULL(CPI.CPIScore,0) AS CPI
                    FROM TMP_FACT_WDIDATA TMP
                LEFT JOIN VW_CPIDATA CPI
                    ON TMP.CountryCode = CPI.ISO3CountryCode
                    AND TMP.YEAR = CPI.Year
                LEFT JOIN DW_DIM_COUNTRY C
                    ON TMP.CountryCode = C.CountryCode
                LEFT JOIN DW_DIM_TIME T
                    ON TMP.YEAR = T.Year
                '
    EXECUTE SP_EXECUTE_SCRIPT @p_sqlscript = @strsql, @p_descrption = 'WDI Fact : Step 5 - Load final fact table for reporting' 
    
END TRY
    
BEGIN CATCH
    
      THROW
    
END CATCH