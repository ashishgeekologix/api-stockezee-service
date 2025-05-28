using api_stockezee_service.Service;
using Microsoft.AspNetCore.Mvc;

namespace api_stockezee_service.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SymbolController : ControllerBase
    {
        private readonly PgResourceDbService _pgResource;

        public SymbolController(PgResourceDbService pgResource)
        {
            this._pgResource = pgResource;
        }

        [HttpGet("eq-stock")]
        public async Task<IActionResult> EqStockData(string request)
        {
            var result = await _pgResource.IndianIndices(request);

            return Ok(result);
        }
    }
}
