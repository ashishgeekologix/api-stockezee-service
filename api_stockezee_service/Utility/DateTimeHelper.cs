using System.Globalization;

namespace api_stockezee_service.Utility
{
    public static class DateTimeHelper
    {
        public static bool TryParseFlexibleDate(string input, out DateTime parsedDate)
        {
            var formats = new[] { "dd/MM/yyyy", "yyyy-MM-dd", "MM/dd/yyyy" };
            return DateTime.TryParseExact(
                input,
                formats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out parsedDate
            );
        }
    }
}
