using api_stockezee_service.Models.Entities.Resource;
using api_stockezee_service.Models.RedisEntity;
using api_stockezee_service.Models.Request.Resource;
using Npgsql;

namespace api_stockezee_service.Service
{
    public class PostgresBulkInsertService
    {
        private readonly LogDbService _log;
        private readonly Func<NpgsqlConnection> _createConnection;

        public PostgresBulkInsertService(LogDbService log, Func<NpgsqlConnection> createConnection)
        {
            this._log = log;
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


        public async Task ForthCommingResult_BulkInsertAsync(List<ForthCommingRequest> entities)
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


        public async Task Fii_Cash_BulkInsertAsync(List<FiiCashData> entities)
        {

            try
            {
                await using var conn = _createConnection();
                await conn.OpenAsync();

                await using var batch = new NpgsqlBatch(conn);

                foreach (var data in entities)
                {

                    var cmd = new NpgsqlBatchCommand(@"
            INSERT INTO fii_cash (category, created_at, buy_value, sell_value, net_value)
            VALUES (@category, @created_at, @buy_value, @sell_value, @net_value)
            ON CONFLICT (category, created_at)
            DO UPDATE SET
                buy_value = EXCLUDED.buy_value,
                sell_value = EXCLUDED.sell_value,
                net_value = EXCLUDED.net_value




    ;");



                    cmd.Parameters.AddWithValue("category", data.category);

                    //// Replace this line:
                    //cmd.Parameters.AddWithValue("created_at", DateTime.Parse(data.date));

                    // With this safer parsing logic:
                    if (!DateTime.TryParseExact(data.date, new[] { "dd/MM/yyyy", "yyyy-MM-dd", "MM/dd/yyyy" },
                        System.Globalization.CultureInfo.InvariantCulture,
                        System.Globalization.DateTimeStyles.None, out var parsedDate))
                    {
                        // Handle parse failure, e.g. log or throw
                        throw new FormatException($"Invalid date format: {data.date}");
                    }
                    cmd.Parameters.AddWithValue("created_at", parsedDate);
                    cmd.Parameters.AddWithValue("buy_value", data.buyValue);
                    cmd.Parameters.AddWithValue("sell_value", data.saleValue);
                    cmd.Parameters.AddWithValue("net_value", data.netValue);

                    batch.BatchCommands.Add(cmd);
                }

                await batch.ExecuteNonQueryAsync();

                Console.WriteLine($"Inserted {entities.Count} ticks at {DateTime.Now}");
            }
            catch (Exception ex)
            {
                await _log.LogExceptionAsync("ERROR", GetType().Name, ex.Message, ex.StackTrace);
            }

        }

        public async Task Fao_Data_BulkInsertAsync(List<FaoData> entities)
        {

            try
            {
                var query = @"
INSERT INTO fao_data (
    client_type, future_index_long, future_index_short, future_stock_long, future_stock_short,
    option_index_call_long, option_index_put_long, option_index_call_short, option_index_put_short,
    option_stock_call_long, option_stock_put_long, option_stock_call_short, option_stock_put_short,
    total_long_contracts, total_short_contracts, created_at
)
VALUES (
    @client_type, @future_index_long, @future_index_short, @future_stock_long, @future_stock_short,
    @option_index_call_long, @option_index_put_long, @option_index_call_short, @option_index_put_short,
    @option_stock_call_long, @option_stock_put_long, @option_stock_call_short, @option_stock_put_short,
    @total_long_contracts, @total_short_contracts, @created_at
)
ON CONFLICT (client_type, created_at)
DO UPDATE SET
    future_index_long = EXCLUDED.future_index_long,
    future_index_short = EXCLUDED.future_index_short,
    future_stock_long = EXCLUDED.future_stock_long,
    future_stock_short = EXCLUDED.future_stock_short,
    option_index_call_long = EXCLUDED.option_index_call_long,
    option_index_put_long = EXCLUDED.option_index_put_long,
    option_index_call_short = EXCLUDED.option_index_call_short,
    option_index_put_short = EXCLUDED.option_index_put_short,
    option_stock_call_long = EXCLUDED.option_stock_call_long,
    option_stock_put_long = EXCLUDED.option_stock_put_long,
    option_stock_call_short = EXCLUDED.option_stock_call_short,
    option_stock_put_short = EXCLUDED.option_stock_put_short,
    total_long_contracts = EXCLUDED.total_long_contracts,
    total_short_contracts = EXCLUDED.total_short_contracts;
";


                await using var conn = _createConnection();
                await conn.OpenAsync();

                await using var batch = new NpgsqlBatch(conn);

                foreach (var data in entities)
                {

                    var cmd = new NpgsqlBatchCommand(query);



                    cmd.Parameters.AddWithValue("client_type", data.ClientType);
                    cmd.Parameters.AddWithValue("future_index_long", data.FutureIndexLong);
                    cmd.Parameters.AddWithValue("future_index_short", data.FutureIndexShort);
                    cmd.Parameters.AddWithValue("future_stock_long", data.FutureStockLong);
                    cmd.Parameters.AddWithValue("future_stock_short", data.FutureStockShort);
                    cmd.Parameters.AddWithValue("option_index_call_long", data.OptionIndexCallLong);
                    cmd.Parameters.AddWithValue("option_index_put_long", data.OptionIndexPutLong);
                    cmd.Parameters.AddWithValue("option_index_call_short", data.OptionIndexCallShort);
                    cmd.Parameters.AddWithValue("option_index_put_short", data.OptionIndexPutShort);
                    cmd.Parameters.AddWithValue("option_stock_call_long", data.OptionStockCallLong);
                    cmd.Parameters.AddWithValue("option_stock_put_long", data.OptionStockPutLong);
                    cmd.Parameters.AddWithValue("option_stock_call_short", data.OptionStockCallShort);
                    cmd.Parameters.AddWithValue("option_stock_put_short", data.OptionStockPutShort);
                    cmd.Parameters.AddWithValue("total_long_contracts", data.TotalLongContracts);
                    cmd.Parameters.AddWithValue("total_short_contracts", data.TotalShortContracts);
                    cmd.Parameters.AddWithValue("created_at", data.created_at);
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
    }
}
