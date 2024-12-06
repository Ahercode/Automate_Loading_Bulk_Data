using System.Text;
using System.Text.Json;
using System.Data.SqlClient;
using LoadDataService.Helpers;

namespace LoadDataService;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private static readonly HttpClient Client = new()
    {
        Timeout = TimeSpan.FromMinutes(2) // Set timeout to 2 minutes
    };
    private const string apiUrl = "http://10.120.120.2:98/lottoService.svc/POST_TICKET?";
    private readonly string? _connectionString;
    private readonly int _pageSize;
    private readonly SaveLogService _saveLogService;

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _connectionString = configuration.GetConnectionString("DefaultConnection");
        var logConnection = configuration.GetConnectionString("LogConnection");
        _pageSize = configuration.GetValue<int>("PageSize");
        _saveLogService = new SaveLogService(logConnection);
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            int lastId = 0;
            string baseQuery = "SELECT R.*, P.ProductID FROM [SGLAPI2].[dbo].[RealTimeTickets] R JOIN [SGLAPI2].[dbo].product P ON P.Product_Name=R.ProductCode WHERE SalesDate > '2024-06-01 00:00:00' AND RealTimeTicket_Id > @lastId";
            List<Dictionary<string, object>> data;

            do
            {
                string query = $"SELECT TOP {_pageSize} * FROM ({baseQuery}) AS alias ORDER BY RealTimeTicket_Id";

                data = LoadDataFromSqlServer(_connectionString, query, lastId);
                
                if (data.Count > 0)
                {
                    await PostDataAsync(data);
                    
                    lastId = GetLastId(data); 
                }
            }
            while (data.Count > 0);

            await Task.Delay(1000, stoppingToken); 
        }
        
        await _saveLogService.LogToDatabase("Stopped", "Service Stopped");
    }
    
    private static int GetLastId(List<Dictionary<string, object>> data)
    {
        return data.Max(record => (int) record["RealTimeTickets_Id"]);
    }
    
    static List<Dictionary<string, object>> LoadDataFromSqlServer(string connectionString, string query, int lastId)
    {
        var data = new List<Dictionary<string, object>>();

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            SqlCommand command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@lastId", lastId);
            command.CommandTimeout = 600; 
            connection.Open();
            using (SqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var record = new Dictionary<string, object>
                    {
                        ["RealTimeTickets_Id"] = reader["RealTimeTicket_Id"],
                        ["serial"] = reader["RetailerID"].ToString()!,
                        ["CellID"] = reader["PhoneNumber"].ToString()!,
                        ["NetworkID"] = reader["PhoneNetwork"].ToString()!,
                        ["Draw_Number"] = reader["DrawNo"].ToString()!,
                        ["Product_ID"] = reader["ProductID"].ToString()!,
                        ["Sale_Date"] = ((DateTime)reader["SalesDate"]).ToString("yyyy-MM-dd HH:mm:ss"),
                        ["Upload_Date"] = ((DateTime)reader["POSDateTime"]).ToString("yyyy-MM-dd HH:mm:ss"),
                        ["Perm_Number"] = reader["PermNumber"].ToString()!,
                        ["Perm_Bet"] = reader["PermBet"].ToString()!,
                        ["Perm_Weight"] = reader["PermWeight"].ToString()!,
                        ["Perm_Amount"] = reader["PermAmount"].ToString()!,
                        ["NAP_Bet"] = reader["DirectNumber"].ToString()!,
                        ["EncryptedData"] = reader["EncryptedData"].ToString()!,
                        ["key"] = reader["Key"].ToString()!,
                        ["NAP_Amount"] = reader["DirectBet"].ToString()!,
                        ["Total"] = reader["TotalAmount"].ToString()!,
                        ["Ticket_Number"] = reader["TicketNumber"].ToString()!,
                        ["Balance"] = reader["POSBalance"].ToString()!,
                        ["PHONE_NUMBER"] = reader["PhoneNetwork"].ToString()!,
                        ["type"] = "00",
                        ["fullTicket"] = reader["FullTicket"].ToString()!,
                        ["direct_amount"] = reader["DirectAmount"].ToString()!,
                        ["directWeight"] = reader["DirectWeight"].ToString()!
                    };
                    data.Add(record);
                }
            }
        }
        return data;
    }
    
    private async Task PostDataAsync(List<Dictionary<string, object>> chunk)
    {
        var json = JsonSerializer.Serialize(chunk);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        HttpResponseMessage response = null;
        int maxRetries = 3;
        int retries = 0;
        
        try
        {
            while (retries < maxRetries)
            {
                response = await Client.PostAsync(apiUrl, content);
                
                Console.WriteLine($"Response Data: {response.StatusCode}");
        
                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine("Success: " + await response.Content.ReadAsStringAsync());
                    await _saveLogService.LogToDatabase("Success", await response.Content.ReadAsStringAsync());
                    break;
                }
        
                retries++;
                Console.WriteLine($"Failed: {response.StatusCode} {await response.Content.ReadAsStringAsync()}");
                await _saveLogService.LogToDatabase("Failed", await response.Content.ReadAsStringAsync());
                await Task.Delay(2000 * retries); 
            }
        }
        catch (Exception e)
        {
            Console.WriteLine($"Falied to post data: {e.Message}");
        }
    }
}