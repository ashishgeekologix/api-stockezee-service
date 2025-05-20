namespace api_stockezee_service.Models.Entities.Resource
{
    public class FnoLotSizeData
    {
        public string underlying { get; set; }
        public string symbol { get; set; }
        public string created_at { get; set; }
        public string month_data { get; set; }

        public decimal last_trade_price { get; set; }
        public decimal change { get; set; }
        public decimal change_percent { get; set; }
    }
    public class FnoLotSizeDetailData
    {
        public IEnumerable<FnoLotSizeData> Data { get; set; }
        public List<string> Key { get; set; }
    }
}
