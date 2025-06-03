using Newtonsoft.Json;

namespace api_stockezee_service.Models.RedisEntity
{
    public class RangeBreakoutCurrent
    {

        [JsonProperty("symbol")]
        public string symbol_name { get; set; }
        public DateTime time { get; set; }
        public decimal high { get; set; }
        public decimal low { get; set; }

        [JsonProperty("lastprice")]
        public decimal close { get; set; }
        public string breakout_direction { get; set; }
        public double break_point { get; set; }
        public double current_score { get; set; }

        public string last_direction { get; set; }

    }

    //public class OrbRange
    //{
    //    public string symbol_name { get; set; }
    //    public TimeSpan time { get; set; } // Assuming time is stored as just time (not DateTime)

    //    public decimal high { get; set; }
    //    public decimal low { get; set; }
    //    public decimal close { get; set; }

    //    public double current_score { get; set; }

    //    public string last_direction { get; set; }
    //}

    public class RangeBreakout
    {
        public string symbol_name { get; set; }
        public TimeSpan time { get; set; }
        public decimal high { get; set; }
        public decimal low { get; set; }

        public decimal close { get; set; }
        public string breakout_direction { get; set; }
        public double break_point { get; set; }
        public double current_score { get; set; }

        public string last_direction { get; set; }
    }

    //public class RangeBreakout
    //{
    //    public string symbol_name { get; set; }
    //    public TimeSpan time { get; set; }
    //    public string breakout_direction { get; set; }
    //    public double break_point { get; set; }
    //    public double current_score { get; set; }
    //}



}
