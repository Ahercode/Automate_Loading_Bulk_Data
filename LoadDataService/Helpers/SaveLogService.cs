using System.Data.SqlClient;

namespace LoadDataService.Helpers;

public class SaveLogService
{
    private readonly string _connectionString;

    public SaveLogService(string connectionString)
    {
        _connectionString = connectionString;
    }
    
    public async Task LogToDatabase(string status, string responseText)
    {
        using (SqlConnection connection = new SqlConnection(_connectionString))
        {
            string query = "INSERT INTO [SERVICE_LOG_DB].[dbo].[SalesLoadLog] (LogDate,Operation,Status,ResponseText) VALUES (@LogDate,@Operation,@Status,@ResponseText)";
            SqlCommand command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@LogDate", DateTime.Now);
            command.Parameters.AddWithValue("@Operation", "Load Sales Data");
            command.Parameters.AddWithValue("@Status", status);
            command.Parameters.AddWithValue("@ResponseText", responseText);
            connection.Open();
            await command.ExecuteNonQueryAsync();
        }
    }
}