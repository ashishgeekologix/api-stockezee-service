using api_stockezee_service.Service;
using Microsoft.AspNetCore.Mvc;

namespace api_stockezee_service.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SymbolController : ControllerBase
    {
        private readonly PgResourceDbService _pgResource;
        private readonly PgAnalysisService _pgAnalysis;

        public SymbolController(PgResourceDbService pgResource, PgAnalysisService pgAnalysis)
        {
            this._pgResource = pgResource;
            this._pgAnalysis = pgAnalysis;
        }

        [HttpGet("eq-stock")]
        public async Task<IActionResult> EqStockData(string request)
        {
            var result = await _pgResource.IndianIndices(request);


            return Ok(result);
        }

        [HttpGet("orb-range")]
        public async Task<IActionResult> OrbRange()
        {
            var result = await _pgResource.OrbRangeBreakout();
            return Ok(result);
        }


        [HttpGet("stock-analysis")]
        public async Task<IActionResult> StockAnalysis(string symbol)
        {
            var result = await _pgAnalysis.StockAnalysis(symbol);

            return Ok(result);
        }
    }
}
