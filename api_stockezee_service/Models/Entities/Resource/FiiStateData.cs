namespace api_stockezee_service.Models.Entities.Resource
{
    public class FiiStateData
    {
        public string type { get; set; }
        public string reporting_date { get; set; }
        public decimal buy_no_of_contracts { get; set; }
        public decimal buy_amount { get; set; }
        public decimal sell_no_of_contracts { get; set; }
        public decimal sell_amount { get; set; }
        public decimal net_amount { get; set; }
        public decimal oi_no_of_contracts { get; set; }
        public decimal oi_value { get; set; }
    }
}
