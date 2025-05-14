namespace api_stockezee_service.Models.Entities.Resource
{
    public class FnoLotSizeData
    {
        public string underlying { get; set; }
        public string symbol { get; set; }
        public string created_at { get; set; }
        public string month_data { get; set; }
    }
    public class FnoLotSizeDetailData
    {
        public List<FnoLotSizeData> Data { get; set; }
        public List<string> Key { get; set; }
    }
}
