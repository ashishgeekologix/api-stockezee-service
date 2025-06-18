namespace api_stockezee_service.Models.RedisEntity
{
    public class BhavCopyData
    {
        public string SYMBOL { get; set; }
        public string SERIES { get; set; }
        public string DATE1 { get; set; }
        public decimal PREV_CLOSE { get; set; }
        public decimal OPEN_PRICE { get; set; }
        public decimal HIGH_PRICE { get; set; }
        public decimal LOW_PRICE { get; set; }
        public decimal LAST_PRICE { get; set; }
        public decimal CLOSE_PRICE { get; set; }
        public decimal AVG_PRICE { get; set; }
        public decimal TTL_TRD_QNTY { get; set; }
        public decimal TURNOVER_LACS { get; set; }
        public decimal NO_OF_TRADES { get; set; }
        public decimal DELIV_QTY { get; set; }
        public decimal DELIV_PER { get; set; }
    }
}
