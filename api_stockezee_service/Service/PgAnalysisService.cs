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

                var sql = @" SELECT symbol_name, open, high, low, close, change, change_percent, last_trade_price, volume, time,high52,low52
        FROM public.nse_eq_stock_data_daily
        WHERE symbol_name = @symbol;

        SELECT 
            string_agg(to_char(time, 'HH24:MI:SS'), ',' ORDER BY time) AS time_csv,
            string_agg(last_trade_price::text, ',' ORDER BY time) AS last_trade_price_csv
        FROM public.nse_eq_stock_data_intraday_daily
        WHERE symbol_name = @symbol;
        
        select  cd.*,cp.industry,cp.sector,CASE 
        WHEN cp.total_market_cap >= 50000 THEN 'Large Cap'
        WHEN cp.total_market_cap >= 10000 THEN 'Mid Cap'
        ELSE 'Small Cap'
    END AS cap_category From nse_company_details cd inner join nse_company_profile cp on cd.symbol_name
                =cp.symbol_name   where cd.symbol_name= @symbol;
        select * From public.nse_company_peers where parent_symbol_name=@symbol;
        select * From public.nse_company_financials where symbol_name=@symbol order by period asc;
        select * From public.nse_company_shareholding where symbol_name=@symbol order by period asc;
        
        ;WITH ranked_data AS (
        select symbol_name,last_trade_price close,created_at,0 rn From nse_eq_stock_data_daily where symbol_name='RELIANCE'
		UNION ALL
		SELECT 
    	symbol symbol_name,
    	close,
   	 	created_at,
    	ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY created_at DESC) AS rn
  		FROM nse_bhav_copy
  		WHERE symbol = @symbol limit 400

            ),

                reference_prices AS (
                  SELECT
                  	
                    MAX(CASE WHEN rn = 0 THEN close END) AS close_today,
                    MAX(CASE WHEN rn = 3 THEN close END) AS close_3d,
                    MAX(CASE WHEN rn = 7 THEN close END) AS close_7d,
                    MAX(CASE WHEN rn = 30 THEN close END) AS close_1m,
                    MAX(CASE WHEN rn = 90 THEN close END) AS close_3m,
                    MAX(CASE WHEN rn = 180 THEN close END) AS close_6m,
                    MAX(CASE WHEN rn = 365 THEN close END) AS close_1y
                  FROM ranked_data 
                )
                
                SELECT 
                  
                  ROUND(((close_today - close_3d) / close_3d) * 100, 2) AS pct_3d,
                  ROUND(((close_today - close_7d) / close_7d) * 100, 2) AS pct_7d,
                  ROUND(((close_today - close_1m) / close_1m) * 100, 2) AS pct_1m,
                  ROUND(((close_today - close_3m) / close_3m) * 100, 2) AS pct_3m,
                  ROUND(((close_today - close_6m) / close_6m) * 100, 2) AS pct_6m,
                  ROUND(((close_today - close_1y) / close_1y) * 100, 2) AS pct_1y
                FROM reference_prices ;

    
                            ";
                using var conn = _createConnection();
                using var multi = await conn.QueryMultipleAsync(sql, new { symbol });
                resQuote.ResultData.spot_price = await multi.ReadFirstOrDefaultAsync<dynamic>();
                resQuote.ResultData.intraday_chart = await multi.ReadFirstOrDefaultAsync<dynamic>();

                resQuote.ResultData.company_details = await multi.ReadFirstOrDefaultAsync<dynamic>();
                resQuote.ResultData.company_peers = await multi.ReadAsync<dynamic>();
                resQuote.ResultData.company_financials = await multi.ReadAsync<dynamic>();
                resQuote.ResultData.company_shareholding = await multi.ReadAsync<dynamic>();
                resQuote.ResultData.company_performance = await multi.ReadAsync<dynamic>();


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
