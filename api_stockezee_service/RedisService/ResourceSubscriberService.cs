
using api_stockezee_service.Models.Entities.Resource;
using api_stockezee_service.Models.Request.Resource;
using api_stockezee_service.Service;
using Newtonsoft.Json;
using Npgsql;
using StackExchange.Redis;

namespace api_stockezee_service.RedisService
{
    public class ResourceSubscriberService : BackgroundService
    {
        private readonly IConnectionMultiplexer _redis;
        //private readonly Func<NpgsqlConnection> _createConnection;
        private readonly PostgresBulkInsertService _bulkInsertService;

        //private readonly IWebHostEnvironment _env;
        //private string contentRoot = string.Empty;

        public ResourceSubscriberService(
            IConnectionMultiplexer redis,
            Func<NpgsqlConnection> createConnection, PostgresBulkInsertService bulkInsertService)
        {
            _redis = redis;
            //_createConnection = createConnection;
            this._bulkInsertService = bulkInsertService;

        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Initialize Redis subscription
            var subscriber = _redis.GetSubscriber();

            // Subscribe to the channel you're interested in


            await subscriber.SubscribeAsync(RedisChannel.Literal("fii_state_data"), async (channel, message) =>
            {
                // Handle received message
                message = CompressionHelper.DecompressFromBase64(message);
                var entities = JsonConvert.DeserializeObject<List<FiiStateData>>(message);
                if (entities.Any())
                {
                    await _bulkInsertService.Fii_State_BulkInsertAsync(entities);
                    Console.WriteLine($"Inserted {entities.Count} records into PostgreSQL.");
                }

            });

            await subscriber.SubscribeAsync(RedisChannel.Literal("forthcomming_result"), async (channel, message) =>
            {
                // Handle received message
                message = CompressionHelper.DecompressFromBase64(message);
                var entities = JsonConvert.DeserializeObject<List<ForthCommingRequest>>(message);
                if (entities.Any())
                {
                    await _bulkInsertService.ForthCommingResult_BulkInsertAsync(entities);
                    Console.WriteLine($"Inserted {entities.Count} records into PostgreSQL.");
                }

            });

            await subscriber.SubscribeAsync(RedisChannel.Literal("nse_ban_list"), async (channel, message) =>
            {
                // Handle received message
                message = CompressionHelper.DecompressFromBase64(message);
                var entities = JsonConvert.DeserializeObject<List<NseBanData>>(message);
                if (entities.Any())
                {
                    await _bulkInsertService.Ban_List_BulkInsertAsync(entities);
                    Console.WriteLine($"Inserted {entities.Count} records into PostgreSQL.");
                }

            });
            await subscriber.SubscribeAsync(RedisChannel.Literal("global_eq_stock_data_daily"), async (channel, message) =>
            {
                // Handle received message
                message = CompressionHelper.DecompressFromBase64(message);
                var entities = JsonConvert.DeserializeObject<List<EquityStockData>>(message);
                if (entities.Any())
                {
                    await _bulkInsertService.Global_Eq_Stock_BulkInsertAsync(entities);
                    Console.WriteLine($"Inserted {entities.Count} records into PostgreSQL.");
                }

            });

        }
    }
}
