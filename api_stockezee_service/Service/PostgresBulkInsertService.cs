using api_stockezee_service.Models.Entities.Resource;
using Npgsql;

namespace api_stockezee_service.Service
{
    public class PostgresBulkInsertService
    {
        private readonly Func<NpgsqlConnection> _createConnection;

        public PostgresBulkInsertService(Func<NpgsqlConnection> createConnection)
        {
            _createConnection = createConnection;
        }

        public async Task Fii_State_BulkInsertAsync(List<FiiStateData> entities)
        {
            await using var conn = _createConnection();
            await conn.OpenAsync();

            await using var batch = new NpgsqlBatch(conn);

            foreach (var tick in entities)
            {

                var cmd = new NpgsqlBatchCommand(@"
            INSERT INTO fii_oi 
(type, reporting_date, buy_no_of_contracts, buy_amount, sell_no_of_contracts, sell_amount,net_amount,oi_no_of_contracts,oi_value)
VALUES 
(@type, @reporting_date, @buy_no_of_contracts, @buy_amount, @sell_no_of_contracts, @sell_amount,@net_amount,@oi_no_of_contracts,@oi_value)
ON CONFLICT (type, reporting_date)
DO UPDATE SET 
    buy_no_of_contracts = EXCLUDED.buy_no_of_contracts,
    buy_amount = EXCLUDED.buy_amount,
    sell_no_of_contracts = EXCLUDED.sell_no_of_contracts,
    sell_amount = EXCLUDED.sell_amount,
    net_amount = EXCLUDED.net_amount,
    oi_no_of_contracts = EXCLUDED.oi_no_of_contracts,
    oi_value = EXCLUDED.oi_value




    ;");

                cmd.Parameters.AddWithValue("type", tick.type);
                cmd.Parameters.AddWithValue("reporting_date", DateTime.Parse(tick.reporting_date));
                cmd.Parameters.AddWithValue("buy_no_of_contracts", tick.buy_no_of_contracts);
                cmd.Parameters.AddWithValue("buy_amount", tick.buy_amount);
                cmd.Parameters.AddWithValue("sell_no_of_contracts", tick.sell_no_of_contracts);
                cmd.Parameters.AddWithValue("sell_amount", tick.sell_amount);
                cmd.Parameters.AddWithValue("net_amount", tick.net_amount);
                cmd.Parameters.AddWithValue("oi_no_of_contracts", tick.oi_no_of_contracts);
                cmd.Parameters.AddWithValue("oi_value", tick.oi_value);

                batch.BatchCommands.Add(cmd);
            }

            await batch.ExecuteNonQueryAsync();

            Console.WriteLine($"Inserted {entities.Count} ticks at {DateTime.Now}");
        }

    }
}
