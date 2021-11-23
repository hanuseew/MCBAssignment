/**
** Creation Date: 22-NOV-2021
** Author: Nirantar Seewooruttun
**/

CREATE PROCEDURE [dbo].[SP_EXECUTE_SCRIPT]
   @p_sqlscript varchar(max),
   @p_descrption varchar(255)
  
AS

BEGIN TRY

    SET NOCOUNT ON

    INSERT INTO REF_LogScripts(DateLog, Description, SQLScript)
    VALUES (SYSDATETIME(), @p_descrption, @p_sqlscript);

    DECLARE
           @strsql nvarchar(max);
       SET @strsql = @p_sqlscript;
  
    EXECUTE sp_executesql @strsql;

END TRY

BEGIN CATCH

    THROW;

END CATCH
