namespace api_stockezee_service.Models.Entities.Resource
{
    public class SecuritiesBanDetailData
    {
        public string date { get; set; }
        public IEnumerable<SecuritiesBanData> securities_ban_result { get; set; }
        public IEnumerable<SecuritiesBanData> possible_entrants_result { get; set; }
        public IEnumerable<SecuritiesBanData> possible_exits_result { get; set; }
        public IEnumerable<SecuritiesBanData> all_list_result { get; set; }
    }
    public class SecuritiesBanData
    {
        public string symbol_name { get; set; }
        public decimal? current_percent { get; set; }
        public decimal? previous_percent { get; set; }
        public string limitfornextday { get; set; }
        public DateTime current_dt { get; set; }
        public double last_trade_price { get; set; }
        public double change { get; set; }
        public double change_percent { get; set; }
    }
}
