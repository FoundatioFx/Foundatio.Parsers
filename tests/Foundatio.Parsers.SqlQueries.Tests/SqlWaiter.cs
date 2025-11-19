using System;
using System.Threading;
using Microsoft.Data.SqlClient;

namespace Foundatio.Parsers.SqlQueries.Tests;

public static class SqlWaiter
{
    private static bool _checked;
    private static readonly object _lock = new();

    public static void Wait(
        string connectionString,
        TimeSpan? timeout = null,
        int delayMs = 1000)
    {
        if (_checked)
            return;

        lock (_lock)
        {
            if (_checked)
                return;

            timeout ??= TimeSpan.FromSeconds(30);
            var end = DateTime.UtcNow + timeout.Value;

            string masterCs = BuildMasterConnectionString(connectionString);

            while (DateTime.UtcNow < end)
            {
                try
                {
                    using var conn = new SqlConnection(masterCs);
                    conn.Open();

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "SELECT 1";
                    cmd.ExecuteScalar();

                    _checked = true;
                    return;
                }
                catch
                {
                    Thread.Sleep(delayMs);
                }
            }

            throw new Exception("Failed to connect to SQL Server within timeout.");
        }
    }

    private static string BuildMasterConnectionString(string cs)
    {
        var builder = new SqlConnectionStringBuilder(cs)
        {
            InitialCatalog = "master"
        };

        return builder.ToString();
    }
}
