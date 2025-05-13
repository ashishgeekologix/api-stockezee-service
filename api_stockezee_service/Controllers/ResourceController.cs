using api_stockezee_service.Service;
using Microsoft.AspNetCore.Mvc;

namespace api_stockezee_service.Controllers
{
    [Route("api/[controller]")]
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
    }
}
