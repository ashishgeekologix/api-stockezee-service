namespace api_stockezee_service.Models.Entities.Analysis
{
    public class StockAnalysisData
    {
        public dynamic spot_price { get; set; }

        public dynamic intraday_chart { get; set; }
        public dynamic company_details { get; set; }
        public dynamic company_peers { get; set; }
        public dynamic company_financials { get; set; }
        public dynamic company_shareholding { get; set; }
        public dynamic company_performance { get; set; }
        public dynamic company_volume { get; set; }
        public dynamic company_about { get; set; }

        public dynamic company_high_low_snapshot { get; set; }


        public StockAnalysisData()
        {
            spot_price = null;
            intraday_chart = null;
            company_details = null;
            company_peers = null;
            company_financials = null;
            company_shareholding = null;
            company_performance = null;
            company_volume = null;
            company_about = null;
            company_high_low_snapshot = null;
        }
    }
}
