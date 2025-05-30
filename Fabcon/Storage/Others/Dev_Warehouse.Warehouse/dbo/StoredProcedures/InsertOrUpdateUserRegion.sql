CREATE PROCEDURE [dbo].[InsertOrUpdateUserRegion]
    @Username VARCHAR(255),
    @Region_Name VARCHAR(100),
    @Role_Name VARCHAR(255),
    @Status VARCHAR(50) = 'Pending' -- Default status is 'Pending'
AS
BEGIN
    BEGIN TRY
        -- Check if the record already exists
        IF EXISTS (
            SELECT 1 FROM [dbo].[user_region_new]
            WHERE Username = @Username AND Region_Name = @Region_Name AND Role_Name = @Role_Name
        )
        BEGIN
            -- Update the status only if it's changed
            UPDATE [dbo].[user_region_new]
            SET Status = @Status
            WHERE Username = @Username AND Region_Name = @Region_Name AND Role_Name = @Role_Name;

            PRINT 'Status updated successfully';
        END
        ELSE
        BEGIN
            -- Insert a new record with 'Pending' status
            INSERT INTO [dbo].[user_region_new] (Username, Region_Name, Role_Name, Status)
            VALUES (@Username, @Region_Name, @Role_Name, @Status);

            PRINT 'Data inserted successfully';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH
END;