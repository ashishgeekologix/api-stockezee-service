namespace api_stockezee_service.Models.Entities.Resource
{
    public class NseBanData
    {
        public string NSESymbol { get; set; }
        public string LimitforNextDay { get; set; }
        public DateTime Date { get; set; }
        public double MWPL { get; set; }
        public double OpenInterest { get; set; }
        public int nse_oi_id { get; set; }
        public double mwpl_value { get; set; }
        public double weightage { get; set; }
        public string Previous_Day_in_Ban { get; set; }
    }
}
