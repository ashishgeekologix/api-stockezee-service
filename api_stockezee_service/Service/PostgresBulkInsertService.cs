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

            try
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
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }


        public async Task ForthCommingResult_BulkInsertAsync(List<ForthCommingData> entities)
        {
            try
            {
                using var conn = _createConnection();
                await conn.OpenAsync();

                // Truncate the table before inserting
                using (var truncateCmd = new NpgsqlCommand("TRUNCATE TABLE forth_comming_result;", conn))
                {
                    await truncateCmd.ExecuteNonQueryAsync();
                }

                // Bulk insert using binary importer
                using (var writer = conn.BeginBinaryImport(@"
        COPY forth_comming_result (scrip_code, short_name, long_name, meeting_date, url) FROM STDIN (FORMAT BINARY)
    "))
                {
                    foreach (var entity in entities)
                    {
                        await writer.StartRowAsync();
                        await writer.WriteAsync(entity.scrip_Code ?? "", NpgsqlTypes.NpgsqlDbType.Text);
                        await writer.WriteAsync(entity.short_name ?? "", NpgsqlTypes.NpgsqlDbType.Text);
                        await writer.WriteAsync(entity.long_Name ?? "", NpgsqlTypes.NpgsqlDbType.Text);
                        await writer.WriteAsync(entity.meeting_date ?? "", NpgsqlTypes.NpgsqlDbType.Text);
                        await writer.WriteAsync(entity.url ?? "", NpgsqlTypes.NpgsqlDbType.Text);
                    }
                    await writer.CompleteAsync();
                }

                Console.WriteLine($"Inserted {entities.Count} ticks at {DateTime.Now}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }


        }


        public async Task Ban_List_BulkInsertAsync(List<NseBanData> entities)
        {

            try
            {
                await using var conn = _createConnection();
                await conn.OpenAsync();

                await using var batch = new NpgsqlBatch(conn);


                foreach (var tick in entities)
                {

                    var cmd = new NpgsqlBatchCommand(@"

            INSERT INTO nse_ban_list 
(symbol_name, created_at, mwpl_value, oi_value, weightage, previous_day_in_ban, limitfornextday, created_date)
VALUES 
(@symbol_name, @created_at, @mwpl_value, @oi_value, @weightage, @previous_day_in_ban, @limitfornextday, @created_date)
ON CONFLICT (symbol_name, created_at)
DO UPDATE SET
    mwpl_value          = EXCLUDED.mwpl_value,
    oi_value            = EXCLUDED.oi_value,
    weightage           = EXCLUDED.weightage,
    previous_day_in_ban = EXCLUDED.previous_day_in_ban,
    limitfornextday     = EXCLUDED.limitfornextday,
    created_date        = EXCLUDED.created_date;

");

                    cmd.Parameters.AddWithValue("symbol_name", tick.NSESymbol);
                    cmd.Parameters.AddWithValue("created_at", tick.Date);
                    cmd.Parameters.AddWithValue("mwpl_value", tick.MWPL);
                    cmd.Parameters.AddWithValue("oi_value", tick.OpenInterest);
                    cmd.Parameters.AddWithValue("weightage", tick.weightage);
                    cmd.Parameters.AddWithValue("previous_day_in_ban", tick.Previous_Day_in_Ban);
                    cmd.Parameters.AddWithValue("limitfornextday", tick.LimitforNextDay);
                    cmd.Parameters.AddWithValue("created_date", DateTime.Now);


                    batch.BatchCommands.Add(cmd);
                }

                await batch.ExecuteNonQueryAsync();

                Console.WriteLine($"Inserted {entities.Count} ticks at {DateTime.Now}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        public async Task Global_Eq_Stock_BulkInsertAsync(List<EquityStockData> entities)
        {
            try
            {
                await using var conn = _createConnection();
                await conn.OpenAsync();

                await using var batch = new NpgsqlBatch(conn);


                foreach (var tick in entities)
                {

                    var cmd = new NpgsqlBatchCommand(@"

            INSERT INTO public.global_eq_stock_data_daily (
    symbol_name,
    open, high, low, close,
    change, change_percent, last_trade_price,
    volume, high52, low52, created_at,
    time, region, market_status
)
VALUES (
    @SymbolName,
    @Open, @High, @Low, @Close,
    @Change, @ChangePercent, @LastTradePrice,
    @Volume, @High52, @Low52, @CreatedAt,
    @Time, @Region, @MarketStatus
)
ON CONFLICT (symbol_name)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    change = EXCLUDED.change,
    change_percent = EXCLUDED.change_percent,
    last_trade_price = EXCLUDED.last_trade_price,
    volume = EXCLUDED.volume,
    high52 = EXCLUDED.high52,
    low52 = EXCLUDED.low52,
    created_at = EXCLUDED.created_at,
    time = EXCLUDED.time,
    region = EXCLUDED.region,
    market_status = EXCLUDED.market_status;

");

                    cmd.Parameters.AddWithValue("SymbolName", tick.Symbol);
                    cmd.Parameters.AddWithValue("Open", tick.Open);
                    cmd.Parameters.AddWithValue("High", tick.High);
                    cmd.Parameters.AddWithValue("Low", tick.Low);
                    cmd.Parameters.AddWithValue("Close", tick.Close);
                    cmd.Parameters.AddWithValue("Change", (tick.LastPrice - tick.Close));
                    // Calculate change percent: if Close is 0, return 0.00, else ((LastPrice - Close) / Close) * 100
                    decimal changePercent = tick.Close == 0
                        ? 0.00m
                        : Math.Round((decimal)((tick.LastPrice - tick.Close) / tick.Close * 100), 2);
                    cmd.Parameters.AddWithValue("ChangePercent", changePercent);
                    cmd.Parameters.AddWithValue("LastTradePrice", tick.LastPrice);
                    cmd.Parameters.AddWithValue("Volume", tick.Volume);
                    cmd.Parameters.AddWithValue("High52", 0);
                    cmd.Parameters.AddWithValue("Low52", 0);
                    cmd.Parameters.AddWithValue("CreatedAt", tick.Timestamp);
                    cmd.Parameters.AddWithValue("Time", tick.Timestamp);
                    cmd.Parameters.AddWithValue("Region", GetRegionBySymbol(tick.Symbol));
                    // Calculate MarketStatus based on the condition:
                    // If (created_at + time) >= (current IST - 30 min) AND created_at.Date == current IST.Date, then true, else false.
                    DateTime createdAt = tick.Timestamp; // or use tick.Timestamp if you want to use the data's timestamp
                    DateTime tickTime = tick.Timestamp;
                    TimeZoneInfo indiaTimeZone = TimeZoneInfo.FindSystemTimeZoneById("India Standard Time");
                    DateTime indianNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, indiaTimeZone);
                    DateTime combinedDateTime = new DateTime(
                        createdAt.Year, createdAt.Month, createdAt.Day,
                        tickTime.Hour, tickTime.Minute, tickTime.Second, tickTime.Millisecond
                    );

                    bool marketStatus =
                        combinedDateTime >= indianNow.AddMinutes(-30) &&
                        createdAt.Date == indianNow.Date;

                    cmd.Parameters.AddWithValue("MarketStatus", marketStatus);


                    batch.BatchCommands.Add(cmd);
                }

                await batch.ExecuteNonQueryAsync();

                Console.WriteLine($"Inserted {entities.Count} ticks at {DateTime.Now}");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        private static string GetRegionBySymbol(string symbol)
        {
            if (string.IsNullOrWhiteSpace(symbol))
                return "Unknown";

            switch (symbol.Trim().ToUpperInvariant())
            {
                case "USCOMPOSITE":
                case "US30":
                case "US100":
                case "US500":
                case "US10YRYIELD":
                    return "US";
                case "FRANCE40":
                case "GERMANY40":
                case "UK100":
                    return "Europe";
                case "SHANGHAICHINA":
                case "JAPAN225":
                case "HANGSENG":
                case "AUS200":
                    return "Asia";
                default:
                    return "Unknown";
            }
        }

    }
}
