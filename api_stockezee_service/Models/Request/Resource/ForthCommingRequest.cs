namespace api_stockezee_service.Models.Request.Resource
{
    public class ForthCommingRequest
    {
        public string scrip_Code { get; set; }
        public string short_name { get; set; }
        public string long_Name { get; set; }
        public string meeting_date { get; set; }
        public string url { get; set; }
    }
}
