using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;

namespace api_stockezee_service.Utility
{
    public class CustomValidationFilter : IActionFilter
    {
        public void OnActionExecuting(ActionExecutingContext context)
        {
            if (!context.ModelState.IsValid)
            {
                // Extract trace ID from HttpContext
                var traceId = context.HttpContext.TraceIdentifier;
                // Create a structured error response
                var errors = context.ModelState
                    .Where(m => m.Value.Errors.Count > 0)
                    .ToDictionary(
                        kvp => kvp.Key,
                        kvp => kvp.Value.Errors.Select(e => e.ErrorMessage).ToArray()
                    );

                var errorResponse = new
                {
                    result = 0,
                    resultMessage = "Validation failed",
                    resultData = new
                    {
                        traceId,
                        errors = errors,

                    }
                };

                context.Result = new JsonResult(errorResponse)
                {
                    StatusCode = 400
                };
            }
        }

        public void OnActionExecuted(ActionExecutedContext context)
        {
            // Do nothing
        }
    }
}
