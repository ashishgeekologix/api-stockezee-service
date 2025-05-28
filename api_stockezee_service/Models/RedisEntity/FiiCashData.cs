namespace api_stockezee_service.Models.RedisEntity
{
    public class FiiCashData
    {

        public string category { get; set; }

        public string date { get; set; }

        public decimal buyValue { get; set; }

        public decimal saleValue { get; set; }

        public decimal netValue { get; set; }
    }
}
