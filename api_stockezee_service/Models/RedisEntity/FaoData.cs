namespace api_stockezee_service.Models.RedisEntity
{
    public class FaoData
    {
        public string ClientType { get; set; }
        public long FutureIndexLong { get; set; }
        public long FutureIndexShort { get; set; }
        public long FutureStockLong { get; set; }
        public long FutureStockShort { get; set; }
        public long OptionIndexCallLong { get; set; }
        public long OptionIndexPutLong { get; set; }
        public long OptionIndexCallShort { get; set; }
        public long OptionIndexPutShort { get; set; }
        public long OptionStockCallLong { get; set; }
        public long OptionStockPutLong { get; set; }
        public long OptionStockCallShort { get; set; }
        public long OptionStockPutShort { get; set; }
        public long TotalLongContracts { get; set; }
        public long TotalShortContracts { get; set; }
        public DateTime created_at { get; set; }
    }
}
