namespace api_stockezee_service.Models.Entities.Resource
{
    public class EquityStockData
    {
        public int OIDayHigh { get; set; }
        public int OI { get; set; }
        public object LastTradeTime { get; set; }
        public List<object> Offers { get; set; }
        public List<object> Bids { get; set; }
        public double Change { get; set; }
        public double Close { get; set; }
        public double Low { get; set; }
        public double High { get; set; }
        public double Open { get; set; }
        public int SellQuantity { get; set; }
        public int BuyQuantity { get; set; }
        public int Volume { get; set; }
        public double AveragePrice { get; set; }
        public int LastQuantity { get; set; }
        public double LastPrice { get; set; }
        public int InstrumentToken { get; set; }
        public int OIDayLow { get; set; }
        public DateTime Timestamp { get; set; }
        public string Symbol { get; set; }
        public DateTime time { get; set; }
    }
}
