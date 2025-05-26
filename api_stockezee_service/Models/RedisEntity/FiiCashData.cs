using Newtonsoft.Json;

namespace api_stockezee_service.Models.RedisEntity
{
    public class FiiCashData
    {
        [JsonProperty("Category")]
        public string category { get; set; }
        [JsonProperty("Date")]
        public string date { get; set; }
        [JsonProperty("BuyValue")]
        public decimal buyValue { get; set; }
        [JsonProperty("SaleValue")]
        public decimal sellValue { get; set; }
        [JsonProperty("NetValue")]
        public decimal netValue { get; set; }
    }
}
