CREATE PROCEDURE [dbo].[SP_User_Region]
    @Username VARCHAR(255),
    @Region_Name VARCHAR(100),
    @Role_Name VARCHAR(255),
    @Status VARCHAR(50) = 'Pending', -- Default status is 'Pending'
    @Approver_Email VARCHAR(100),
    @Approver_Comment VARCHAR(500)
AS
BEGIN
    BEGIN TRY
        -- Check if the record already exists
        IF EXISTS (
            SELECT 1 FROM [dbo].[USER_REGION_AUDIT]
            WHERE Username = @Username AND Region_Name = @Region_Name AND Role_Name = @Role_Name
        )
        BEGIN
            -- Update the status only if it's changed
            UPDATE [dbo].[USER_REGION_AUDIT]
            SET Status = @Status,
                Updated_Time = GETUTCDATE(),
                Approver_Email= @Approver_Email,
                Approver_Comment=@Approver_Comment
            WHERE Username = @Username AND Region_Name = @Region_Name AND Role_Name = @Role_Name;

            PRINT 'Status updated successfully';
        END
        ELSE
        BEGIN
            -- Insert a new record with 'Pending' status
            INSERT INTO [dbo].[USER_REGION_AUDIT] (Username, Region_Name, Role_Name, Status,Submitted_Time)
            VALUES (@Username, @Region_Name, @Role_Name, @Status,GETUTCDATE());

            PRINT 'Data inserted successfully';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH
END;