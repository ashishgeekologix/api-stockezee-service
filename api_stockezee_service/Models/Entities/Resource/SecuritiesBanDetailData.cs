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
    }
}
