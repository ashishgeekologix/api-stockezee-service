using api_stockezee_service.Models;
using api_stockezee_service.Models.Entities.Resource;
using api_stockezee_service.Service;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace api_stockezee_service.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class ResourceController : ControllerBase
    {
        private readonly PgResourceDbService _pgResource;

        public ResourceController(PgResourceDbService pgResource)
        {
            this._pgResource = pgResource;
        }

        [HttpGet("fii-daily-states")]
        public async Task<IActionResult> FilDailyStates()
        {
            var result = await _pgResource.FiiStateData();

            return Ok(result);
        }

        [HttpGet("holidays-list")]
        public async Task<IActionResult> HolidaysList()
        {
            var result = await _pgResource.HolidayList();

            return Ok(result);
        }

        [HttpGet("fno-lot-size")]
        public async Task<IActionResult> FnoLotSize()
        {
            var result = new ResultObjectDTO<dynamic>();

            var res = await _pgResource.FnoLotSize();
            result.Result = res.Result;
            result.ResultMessage = res.ResultMessage;
            var dict = JsonSerializer.Deserialize<Dictionary<string, string>>(res.ResultData.FirstOrDefault().month_data);
            List<string> keys = new List<string>(dict.Keys);
            result.ResultData = new
            {
                data = res.ResultData,
                keys = keys
            };
            return Ok(result);
        }

        [HttpGet("forth-comming-result")]
        public async Task<IActionResult> ForthCommingResult()
        {
            var result = await _pgResource.ForthComingResult();

            return Ok(result);
        }

        [HttpGet("ban-list")]
        public async Task<IActionResult> BanList()
        {
            var res = await _pgResource.SecuritiesBanList();
            var result = new ResultObjectDTO<SecuritiesBanDetailData>()
            {
                Result = res.Result,
                ResultMessage = res.ResultMessage,
                ResultData = new SecuritiesBanDetailData()
                {
                    date = res.ResultData.Max(_ => _.current_dt).ToString("yyyy-MM-dd"),
                    securities_ban_result = res.ResultData.Where(_ => _.limitfornextday == "No Fresh Positions").ToList(),
                    possible_entrants_result = res.ResultData.Where(_ => _.limitfornextday == "No Fresh Positions" && _.change_percent > 80).ToList(),
                    possible_exits_result = res.ResultData.Where(_ => _.limitfornextday == "No Fresh Positions" && _.change_percent < 85).ToList(),
                    all_list_result = res.ResultData
                }
            };


            return Ok(result);
        }
        [HttpGet("global-market")]
        public async Task<IActionResult> GlobalMarket()
        {
            var result = await _pgResource.GlobalMarket();
            return Ok(result);
        }

    }
}
