﻿
using api_stockezee_service.Models.Entities.Resource;
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


            // Bse Option Chain
            await subscriber.SubscribeAsync(RedisChannel.Literal("fii_state_data"), async (channel, message) =>
            {
                // Handle received message
                message = CompressionHelper.DecompressFromBase64(message);
                var entities = JsonConvert.DeserializeObject<List<FiiStateData>>(message);
                if (entities.Any())
                {
                    //await _bulkInsertService.BulkInsertOptionTickersAsync(tickDataList);
                    await _bulkInsertService.Fii_State_BulkInsertAsync(entities);
                    Console.WriteLine($"Inserted {entities.Count} records into PostgreSQL.");
                }
                //await _dbService.Write_To_DB("save_bse_option_data_daily", "Jobs_Bse_Option_Data_Daily", message);

            });
        }
    }
}
