CREATE PROCEDURE [dbo].[SP_User_Region_Test]
   @Username VARCHAR(255),
   @Region_Name VARCHAR(MAX),  -- JSON array-like input: e.g., '["East","West"]'
   @Role_Name VARCHAR(255),
   @Status VARCHAR(50) = 'Pending', -- Default status is 'Pending'
   @Approver_Email VARCHAR(100),
   @Approver_Comment VARCHAR(500)='null'
AS
BEGIN
    BEGIN TRY
        -- Remove the brackets and quotes from the JSON array
        SET @Region_Name = REPLACE(REPLACE(@Region_Name, '[', ''), ']', '');
        SET @Region_Name = REPLACE(@Region_Name, '"', '');

        -- Now @Region_Name is a comma-separated string: e.g., 'East,West'

        -- Check if the records already exist for the given Region_Name(s) and Username
        IF EXISTS (
            SELECT 1 FROM [dbo].[USER_REGION_AUDIT]
            WHERE Username = @Username
            AND Role_Name = @Role_Name
            AND Region_Name IN (SELECT DISTINCT TRIM(value) FROM STRING_SPLIT(@Region_Name, ','))
        )
        BEGIN
            -- If records exist, update them
            UPDATE [dbo].[USER_REGION_AUDIT]
            SET Status = @Status,
                Updated_Time = GETUTCDATE(),
                Approver_Email = @Approver_Email,
                Approver_Comment = @Approver_Comment
            WHERE Username = @Username AND Role_Name = @Role_Name
                AND Region_Name IN (SELECT DISTINCT TRIM(value) FROM STRING_SPLIT(@Region_Name, ','));

            PRINT 'Status updated successfully for all Region Names';
        END
        ELSE
        BEGIN
            -- If records do not exist, insert them
            INSERT INTO [dbo].[USER_REGION_AUDIT] 
                (Username, Region_Name, Role_Name, Status, Submitted_Time, Approver_Email, Approver_Comment)
            SELECT 
                @Username, 
                TRIM(value) AS Region_Name,
                @Role_Name, 
                @Status, 
                GETUTCDATE(),
                @Approver_Email,
                @Approver_Comment
            FROM (SELECT DISTINCT TRIM(value) AS value FROM STRING_SPLIT(@Region_Name, ',')) AS UniqueRegionNames;

            PRINT 'Data inserted successfully for all Region Names';
        END

    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH
END;