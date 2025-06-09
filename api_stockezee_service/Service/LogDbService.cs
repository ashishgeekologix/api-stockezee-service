using Npgsql;

namespace api_stockezee_service.Service
{
    public class LogDbService
    {
        private readonly Func<NpgsqlConnection> _createConnection;

        public LogDbService(Func<NpgsqlConnection> createConnection)
        {
            _createConnection = createConnection;
        }

        public async Task LogExceptionAsync(string logLevel, string source, string message, string stackTrace)
        {
            var sql = @"INSERT INTO application_logs (log_level, source, message, stack_trace) VALUES (@logLevel, @source, @message, @stackTrace);";
            using var conn = _createConnection();
            await conn.OpenAsync();
            using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("logLevel", logLevel ?? "ERROR");
            cmd.Parameters.AddWithValue("source", source ?? "");
            cmd.Parameters.AddWithValue("message", message ?? "");
            cmd.Parameters.AddWithValue("stackTrace", stackTrace ?? "");
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
