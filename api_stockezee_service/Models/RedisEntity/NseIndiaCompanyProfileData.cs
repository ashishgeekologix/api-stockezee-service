namespace api_stockezee_service.Models.RedisEntity
{
    public class NseIndiaCompanyProfileData
    {
        public string symbol { get; set; }
        public string company_name { get; set; }
        public string isin { get; set; }
        public string identifier { get; set; }
        public bool? is_municipal_bond { get; set; }
        public string series { get; set; }
        public string last_update_time { get; set; }
        public string board_status { get; set; }
        public string trading_status { get; set; }
        public DateTime? listing_date { get; set; }
        public decimal? pd_sector_pe { get; set; }
        public decimal? pd_symbol_pe { get; set; }
        public string pd_sector_ind { get; set; }
        public string industry { get; set; }
        public string sector { get; set; }
        public string basic_industry { get; set; }
        public decimal? total_market_cap { get; set; }
        public decimal? ffmc { get; set; }
        public DateTime? created_at { get; set; }
        public TimeSpan? time { get; set; }
    }
}
