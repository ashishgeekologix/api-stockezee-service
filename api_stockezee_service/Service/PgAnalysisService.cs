using api_stockezee_service.Models;
using api_stockezee_service.Models.Entities.Analysis;
using Dapper;
using Npgsql;

namespace api_stockezee_service.Service
{
    public class PgAnalysisService
    {
        private readonly Func<NpgsqlConnection> _createConnection;

        public PgAnalysisService(Func<NpgsqlConnection> createConnection)
        {
            _createConnection = createConnection;
        }
        public async Task<ResultObjectDTO<StockAnalysisData>> StockAnalysis(string symbol)
        {
            ResultObjectDTO<StockAnalysisData> resQuote = new ResultObjectDTO<StockAnalysisData>();
            resQuote.ResultData = new StockAnalysisData();

            try
            {

                var sql = @" SELECT symbol_name, open, high, low, close, change, change_percent, last_trade_price, volume, time
        FROM public.nse_eq_stock_data_daily
        WHERE symbol_name = @symbol;

        SELECT 
            string_agg(to_char(time, 'HH24:MI:SS'), ',' ORDER BY time) AS time_csv,
            string_agg(last_trade_price::text, ',' ORDER BY time) AS last_trade_price_csv
        FROM public.nse_eq_stock_data_intraday_daily
        WHERE symbol_name = @symbol;
        
        select * From public.nse_company_details where symbol_name=@symbol;
        select * From public.nse_company_peers where parent_symbol_name=@symbol;
        select * From public.nse_company_financials where symbol_name=@symbol order by period asc;
        select * From public.nse_company_shareholding where symbol_name=@symbol order by period asc;
    
";
                using var conn = _createConnection();
                using var multi = await conn.QueryMultipleAsync(sql, new { symbol });
                resQuote.ResultData.spot_price = await multi.ReadFirstOrDefaultAsync<dynamic>();
                resQuote.ResultData.intraday_chart = await multi.ReadFirstOrDefaultAsync<dynamic>();

                resQuote.ResultData.company_details = await multi.ReadFirstOrDefaultAsync<dynamic>();
                resQuote.ResultData.company_peers = await multi.ReadAsync<dynamic>();
                resQuote.ResultData.company_financials = await multi.ReadAsync<dynamic>();
                resQuote.ResultData.company_shareholding = await multi.ReadAsync<dynamic>();


                if (resQuote.ResultData is null)
                {
                    resQuote.ResultMessage = "Data Not Found.";
                    resQuote.Result = ResultType.Error;
                }
                else
                {
                    resQuote.ResultMessage = "Success";
                    resQuote.Result = ResultType.Success;
                }

            }
            catch (Exception ex)
            {
                resQuote.ResultMessage = ex.Message.ToString();
                resQuote.Result = ResultType.Error;
            }
            return resQuote;
        }

    }
}
