using api_stockezee_service.Service;
using Microsoft.AspNetCore.Mvc;

namespace api_stockezee_service.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AnalysisController : ControllerBase
    {
        private readonly PgAnalysisService _pgAnalysis;

        public AnalysisController(PgResourceDbService pgResource, PgAnalysisService pgAnalysis)
        {
            this._pgAnalysis = pgAnalysis;
        }

        [HttpGet("stock-analysis")]
        public async Task<IActionResult> StockAnalysis(string symbol)
        {
            var result = await _pgAnalysis.StockAnalysis(symbol);

            return Ok(result);
        }
    }
}
