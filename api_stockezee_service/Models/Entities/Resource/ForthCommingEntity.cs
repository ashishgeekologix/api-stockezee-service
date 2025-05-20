namespace api_stockezee_service.Models.Entities.Resource
{
    public class ForthCommingEntity
    {
        public string scrip_code { get; set; }
        public string symbol { get; set; }
        public string long_name { get; set; }
        public DateTime meeting_date { get; set; }
        public double last_trade_price { get; set; }
        public double change { get; set; }
        public double change_percent { get; set; }
    }
}
