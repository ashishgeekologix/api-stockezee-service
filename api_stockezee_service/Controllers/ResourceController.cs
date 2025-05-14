using api_stockezee_service.Service;
using Microsoft.AspNetCore.Mvc;

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
            var result = await _pgResource.FnoLotSize();

            return Ok(result);
        }

        [HttpGet("forth-comming-result")]
        public async Task<IActionResult> ForthCommingResult()
        {
            var result = await _pgResource.ForthComingResult();

            return Ok(result);
        }
    }
}
