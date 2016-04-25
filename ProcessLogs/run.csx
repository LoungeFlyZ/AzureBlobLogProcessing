#r "System.Configuration"
#r "System.Data"

using System.Data.SqlClient;
using System;
using CsvHelper;
using CsvHelper.Configuration;
using System.IO;


public static void Run(string myBlob, TraceWriter log)
{
    log.Verbose($"New Log!");
    
    using (TextReader sr = new StringReader(myBlob))
    {
        var csv = new CsvReader(sr);
        csv.Configuration.Delimiter = ";";
        csv.Configuration.RegisterClassMap<LogEntryMap>();

        var records = csv.GetRecords<LogEntry>().ToList();

        // filter out rows we want to keep. Only successful getblob requests for our mp3 files. 
        records = FilterRecords(records);

        SendToDB(records, log);
    }
    
    log.Verbose($"DONE");
}

public static List<LogEntry> FilterRecords(List<LogEntry> entries)
{
    // filter 
    var filteredEntries =
        from e in entries
        where
            e.RequestURL.ToLower().EndsWith("mp3") == true &&
            e.RESTOperationType.ToLower() == "getblob" &&
            (e.RequestStatus.ToLower() == "anonymoussuccess" || e.RequestStatus.ToLower() == "success")
        select e;
    
    return filteredEntries.ToList();
}

private static string insertSql = "INSERT INTO [dbo].[EpisodeLogsTest] ([TransactionDateTime],[OperationType]," +
                                    "[ObjectKey],[UserAgent],[Referrer],[Episode],[DeviceType],[IPAddress],[RequestID]) " +
                                    "VALUES (@val1,@val2,@val3,@val4,@val5,@val6,@val7,@val8,@val9)";

public static void SendToDB(List<LogEntry> entries, TraceWriter log)
{
    var str = "Server=tcp:FOO,1433;Database=DBNAME;User ID=USERNAME;Password=PASSWORD;Trusted_Connection=False;Encrypt=True;";

    using (SqlConnection conn = new SqlConnection(str))
    {
        conn.Open();

        foreach (LogEntry e in entries)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(insertSql, conn))
                {
                    cmd.Parameters.AddWithValue("@val1", e.TransactionStartTime);
                    cmd.Parameters.AddWithValue("@val2", e.RESTOperationType);
                    cmd.Parameters.AddWithValue("@val3", e.ObjectKey);
                    cmd.Parameters.AddWithValue("@val4", new string(e.UserAgent.Take(255).ToArray()));
                    cmd.Parameters.AddWithValue("@val5", new string(e.Referrer.Take(1024).ToArray()));
                    cmd.Parameters.AddWithValue("@val6", e.ObjectKey.Substring(e.ObjectKey.Length - 7, 7).Substring(0, 3)); // extract the episode number from the url
                    cmd.Parameters.AddWithValue("@val7", ParseDeviceType(e.UserAgent)); // get the device type from user agent
                    cmd.Parameters.AddWithValue("@val8", e.ClientIP.Contains(":") ? e.ClientIP.Substring(0, e.ClientIP.IndexOf(':')) : e.ClientIP); // remove the port
                    cmd.Parameters.AddWithValue("@val9", e.RequestID);

                    int rows = cmd.ExecuteNonQuery();

                    if(rows > 0)
                    {
                        log.Verbose($"Row Inserted: {e.RequestID}");
                    }
                }
            }
            catch(Exception ex)
            {
                // something went wrong. 
                log.Verbose($"Error inserting row: {ex.Message}");
            }
        }
    }
}

public static string ParseDeviceType(string userAgent)
{

    if (userAgent.ToLower().Contains("stitcher")) return "Stitcher";
    if (userAgent.ToLower().Contains("overcast")) return "Overcast";
    if (userAgent.ToLower().Contains("chrome")) return "Chrome";
    if (userAgent.ToLower().Contains("windows mobile")) return "Windows Mobile";
    if (userAgent.ToLower().Contains("windows phone")) return "Windows Phone";
    if (userAgent.ToLower().Contains("apple tv")) return "Apple TV";
    if (userAgent.ToLower().Contains("itunes")) return "iTunes";
    if (userAgent.ToLower().Contains("feedreader")) return "Feedreader";
    if (userAgent.ToLower().Contains("windows nt")) return "Windows";
    if (userAgent.ToLower().Contains("mozilla")) return "Mozilla";
    if (userAgent.ToLower().Contains("zune")) return "Zune";
    if (userAgent.ToLower().Contains("pocket casts")) return "Pocket Casts";
    if (userAgent.ToLower().Contains("android")) return "Android";
    if (userAgent.ToLower().Contains("iphone")) return "iPhone";
    if (userAgent.ToLower().Contains("ipad")) return "iPad";
    if (userAgent.ToLower().Contains("ipod")) return "iPod";
    if (userAgent.ToLower().Contains("ios")) return "iOS";

    string other;

    if(userAgent.Contains(@"/"))
        other = userAgent.Substring(0, userAgent.IndexOf(@"/"));
    else
        other = new string(userAgent.Take(40).ToArray());

    return other;
}


public class LogEntry
{
    public string LogVersion {get; set;}
    public DateTime TransactionStartTime {get; set;}
    public string RESTOperationType {get; set;}
    public string RequestStatus {get; set;}
    public string HTTPStatusCode {get; set;}
    public string E2ELatency {get; set;}
    public string ServerLatency {get; set;}
    public string AuthenticationType {get; set;}
    public string RequestorAccountName {get; set;}
    public string OwnerAccountName {get; set;}
    public string ServiceType {get; set;}
    public string RequestURL {get; set;}
    public string ObjectKey {get; set;}
    public string RequestID {get; set;}
    public string OperationNumber {get; set;}
    public string ClientIP  {get; set;}
    public string RequestVersion {get; set;}
    public string RequestHeaderSize {get; set;}
    public string RequestPacketSize {get; set;}
    public string ResponseHeaderSize  {get; set;}
    public string ResponsePacketSize {get; set;}
    public string RequestContentLength {get; set;}
    public string RequestMD5 {get; set;}
    public string ServerMD5 {get; set;}
    public string ETag {get; set;}
    public string LastModifiedTime {get; set;}
    public string ConditionsUsed {get; set;}
    public string UserAgent {get; set;}
    public string Referrer  {get; set;}
    public string ClientRequestID {get; set;}
    
}

public sealed class LogEntryMap : CsvClassMap<LogEntry>
{
    public LogEntryMap()
    {
        Map( m => m.LogVersion ).Index(0);
        Map( m => m.TransactionStartTime ).Index(1);
        Map( m => m.RESTOperationType ).Index(2);
        Map( m => m.RequestStatus ).Index(3);
        Map( m => m.HTTPStatusCode ).Index(4);
        Map( m => m.E2ELatency ).Index(5);
        Map( m => m.ServerLatency ).Index(6);
        Map( m => m.AuthenticationType ).Index(7);
        Map( m => m.RequestorAccountName ).Index(8);
        Map( m => m.OwnerAccountName ).Index(9);
        Map( m => m.ServiceType ).Index(10);
        Map( m => m.RequestURL ).Index(11);
        Map( m => m.ObjectKey ).Index(12);
        Map( m => m.RequestID ).Index(13);
        Map( m => m.OperationNumber ).Index(14);
        Map( m => m.ClientIP ).Index(15);
        Map( m => m.RequestVersion ).Index(16);
        Map( m => m.RequestHeaderSize ).Index(17);
        Map( m => m.RequestPacketSize ).Index(18);
        Map( m => m.ResponseHeaderSize ).Index(19);
        Map( m => m.ResponsePacketSize ).Index(20);
        Map( m => m.RequestContentLength ).Index(21);
        Map( m => m.RequestMD5 ).Index(22);
        Map( m => m.ServerMD5 ).Index(23);
        Map( m => m.ETag ).Index(24);
        Map( m => m.LastModifiedTime ).Index(25);
        Map( m => m.ConditionsUsed ).Index(26);
        Map( m => m.UserAgent ).Index(27);
        Map( m => m.Referrer ).Index(28);
        Map( m => m.ClientRequestID ).Index(29);
    }
}