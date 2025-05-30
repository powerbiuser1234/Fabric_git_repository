CREATE PROCEDURE [dbo].[SP_User_Branch_Test]
   @Username VARCHAR(255),
   @Branch_Code VARCHAR(MAX),  -- JSON array-like input: e.g., '["75101","95101"]'
   @Role_Name VARCHAR(255),
   @Status VARCHAR(50) = 'Pending', -- Default status is 'Pending'
   @Approver_Email VARCHAR(100),
   @Approver_Comment VARCHAR(500)='null' 
AS
BEGIN
    BEGIN TRY
        -- Remove the brackets from the JSON array and replace quotes with nothing
        SET @Branch_Code = REPLACE(REPLACE(@Branch_Code, '[', ''), ']', '');  -- Remove square brackets
        SET @Branch_Code = REPLACE(@Branch_Code, '"', '');  -- Remove double quotes

        -- Now the @Branch_Code is a comma-separated string: e.g., '75101,95101'

        -- First, Check if the records already exist for the given Branch_Code(s) and Username
        IF EXISTS (
            SELECT 1 FROM [dbo].[USER_BRANCH_AUDIT]
            WHERE Username = @Username
            AND Role_Name = @Role_Name
            AND Branch_Code IN (SELECT DISTINCT TRIM(value) FROM STRING_SPLIT(@Branch_Code, ','))
        )
        BEGIN
            -- If records exist, update them with the approval details
            UPDATE [dbo].[USER_BRANCH_AUDIT]
            SET Status = @Status,
                Updated_Time = GETUTCDATE(),
                Approver_Email = @Approver_Email,
                Approver_Comment = @Approver_Comment
            WHERE Username = @Username AND Role_Name = @Role_Name
                AND Branch_Code IN (SELECT DISTINCT TRIM(value) FROM STRING_SPLIT(@Branch_Code, ','));

            PRINT 'Status updated successfully for all Branch Codes';
        END
        ELSE
        BEGIN
            -- If records do not exist, insert them
            INSERT INTO [dbo].[USER_BRANCH_AUDIT] 
                (Username, Branch_Code, Role_Name, Status, Submitted_Time, Approver_Email, Approver_Comment)
            SELECT 
                @Username, 
                TRIM(value) AS Branch_Code,  -- Trim spaces around branch codes to avoid duplicates
                @Role_Name, 
                @Status, 
                GETUTCDATE() AS Submitted_Time,
                @Approver_Email,
                @Approver_Comment
            FROM (SELECT DISTINCT TRIM(value) AS value FROM STRING_SPLIT(@Branch_Code, ',')) AS UniqueBranchCodes;  -- Remove duplicates by using DISTINCT

            PRINT 'Data inserted successfully for all Branch Codes';
        END

    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH
END;