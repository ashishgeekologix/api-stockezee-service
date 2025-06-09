using api_stockezee_service.Service;

namespace api_stockezee_service.Utility
{
    public class ExceptionLoggingMiddleware
    {
        private readonly RequestDelegate _next;

        public ExceptionLoggingMiddleware(RequestDelegate next)
        {
            _next = next;
        }

        public async Task InvokeAsync(HttpContext context, LogDbService logDbService)
        {
            try
            {
                await _next(context);
            }
            catch (Exception ex)
            {
                // Log exception to PostgreSQL
                await logDbService.LogExceptionAsync(
                    "ERROR",
                    context.Request.Path,
                    ex.Message,
                    ex.StackTrace ?? string.Empty
                );
                // Optionally rethrow or return a generic error response
                context.Response.StatusCode = 500;
                await context.Response.WriteAsJsonAsync(new { result = 0, resultMessage = "An unexpected error occurred." });
            }
        }
    }

    public static class ExceptionLoggingMiddlewareExtensions
    {
        public static IApplicationBuilder UseExceptionLogging(this IApplicationBuilder builder)
        {
            return builder.UseMiddleware<ExceptionLoggingMiddleware>();
        }
    }
}
