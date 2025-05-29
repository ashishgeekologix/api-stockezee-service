using api_stockezee_service.Models;
using api_stockezee_service.Models.Entities.Resource;
using Dapper;
using Npgsql;
using System.Data;


namespace api_stockezee_service.Service
{
    public class PgResourceDbService
    {

        private readonly Func<NpgsqlConnection> _createConnection;

        public PgResourceDbService(Func<NpgsqlConnection> createConnection)
        {
            _createConnection = createConnection;
        }

        public async Task<ResultObjectDTO<dynamic>> FiiStateData()
        {
            ResultObjectDTO<dynamic> resQuote = new ResultObjectDTO<dynamic>();
            try
            {

                var sql = @"
                    SELECT  type, reporting_date, buy_no_of_contracts, buy_amount, sell_no_of_contracts, sell_amount, net_amount, oi_no_of_contracts, oi_value
                       FROM fii_oi
                        WHERE reporting_date >= CURRENT_DATE - INTERVAL '10 days'
                        ORDER BY reporting_date DESC, type DESC;
                        
                        ";


                using (IDbConnection conn = _createConnection())
                {
                    resQuote.ResultData = await conn.QueryAsync<dynamic>(sql);



                }
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


        public async Task<ResultObjectDTO<dynamic>> HolidayList()
        {
            ResultObjectDTO<dynamic> resQuote = new ResultObjectDTO<dynamic>();
            try
            {

                var sql = @"
                    SELECT 
  ROW_NUMBER() OVER (PARTITION BY hl.typ ORDER BY hl.id ASC) AS id,
  hl.typ,
  hl.holiday_desc,
  TO_CHAR(hl.holiday_date, 'Month DD, YYYY') AS holiday_date,
  hl.holiday_day,
  COALESCE(hl.morning_session, '') AS morning_session,
  COALESCE(hl.evening_session, '') AS evening_session
FROM holidays_list hl
WHERE hl.is_active = true
ORDER BY id, holiday_date;
                        
                        ";


                using (IDbConnection conn = _createConnection())
                {
                    resQuote.ResultData = await conn.QueryAsync<dynamic>(sql);



                }
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

        public async Task<ResultObjectDTO<IEnumerable<FnoLotSizeData>>> FnoLotSize()
        {
            ResultObjectDTO<IEnumerable<FnoLotSizeData>> resQuote = new ResultObjectDTO<IEnumerable<FnoLotSizeData>>();
            try
            {

                //                var sql = @"
                //                    WITH extracted_keys AS (
                //    SELECT DISTINCT
                //        jsonb_object_keys(month_data::jsonb) AS key
                //    FROM public.nse_fno_lot_size
                //),
                //key_with_month_order AS (
                //    SELECT 
                //        key,
                //        TO_DATE('01-' || key, 'DD-Mon-YY') AS month_sort
                //    FROM extracted_keys
                //)

                //SELECT jsonb_build_object(
                //  'data', jsonb_agg(
                //    jsonb_build_object(
                //      'underlying', underlying,
                //      'symbol', symbol,
                //      'created_at', TO_CHAR(created_at, 'MM/DD/YYYY HH24:MI:SS'),
                //      'month_data', month_data
                //    )
                //  ),
                //  'key', (
                //    SELECT jsonb_agg(key ORDER BY month_sort)
                //    FROM key_with_month_order
                //  )
                //) AS response_json
                //FROM public.nse_fno_lot_size;

                //                        ";

                var sql = @"select fn.*,eq.last_trade_price,eq.change,eq.change_percent From nse_fno_lot_size fn inner join nse_eq_stock_data_daily eq on fn_Nse_Get_Stock_Symbol(fn.symbol)=eq.symbol_name ";

                using (IDbConnection conn = _createConnection())
                {
                    //var json = await conn.QueryFirstOrDefaultAsync<string>(sql);

                    //resQuote.ResultData = JsonConvert.DeserializeObject<FnoLotSizeDetailData>(json);
                    resQuote.ResultData = await conn.QueryAsync<FnoLotSizeData>(sql);
                }
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


        public async Task<ResultObjectDTO<IEnumerable<ForthCommingEntity>>> ForthComingResult()
        {
            ResultObjectDTO<IEnumerable<ForthCommingEntity>> resQuote = new ResultObjectDTO<IEnumerable<ForthCommingEntity>>();
            try
            {

                //var sql = @"
                //                    SELECT 
                //  fd.scrip_code,
                //  fd.short_name AS symbol,
                //  UPPER(fd.long_name) AS long_name,
                //  fd.meeting_date::DATE AS meeting_date
                //FROM forth_comming_result fd
                //ORDER BY 
                //  fd.meeting_date::DATE,
                //  fd.short_name;

                //                        ";

                var sql = @"
                                                                                           SELECT 
                  fd.scrip_code,
                  fd.short_name AS symbol,
                  UPPER(fd.long_name) AS long_name,
                  fd.meeting_date::DATE AS meeting_date,
                  COALESCE(eq.last_trade_price, 0) AS last_trade_price,
                  COALESCE(eq.change, 0) AS change,
                  COALESCE(eq.change_percent, 0) AS change_percent
                FROM forth_comming_result fd  left join nse_eq_stock_data_daily eq on fd.short_name=eq.symbol_name
                ORDER BY 
                  fd.meeting_date::DATE,
                  fd.short_name;

                                        ";


                using (IDbConnection conn = _createConnection())
                {
                    resQuote.ResultData = await conn.QueryAsync<ForthCommingEntity>(sql);



                }
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


        public async Task<ResultObjectDTO<IEnumerable<SecuritiesBanData>>> SecuritiesBanList()
        {
            ResultObjectDTO<IEnumerable<SecuritiesBanData>> resQuote = new ResultObjectDTO<IEnumerable<SecuritiesBanData>>();
            //resQuote.ResultData = new SecuritiesBanDetailData();
            //resQuote.ResultData.date = string.Empty;
            //resQuote.ResultData.securities_ban_result = new List<SecuritiesBanData>();
            //resQuote.ResultData.possible_entrants_result = new List<SecuritiesBanData>();
            //resQuote.ResultData.possible_exits_result = new List<SecuritiesBanData>();
            //resQuote.ResultData.all_list_result = new List<SecuritiesBanData>();

            try
            {

                //var sql = @"
                //                    -- Step 1: Create a temporary table and insert the computed data

                //CREATE TEMP TABLE nse_oi_futures_temp AS
                //WITH ordered_dates AS (
                //  SELECT DISTINCT created_at
                //  FROM nse_ban_list
                //  ORDER BY created_at DESC
                //  LIMIT 2
                //),
                //max_dates AS (
                //  SELECT 
                //    MAX(created_at) AS max_date,
                //    MIN(created_at) AS prv_date
                //  FROM ordered_dates
                //),
                //temp_data AS (
                //  SELECT *,
                //    ROUND((oi_value * 100 / NULLIF(mwpl_value, 0)), 2)::DECIMAL(18,2) AS mwpl_percent,
                //    previous_day_in_ban AS curr_previous_day_in_ban
                //  FROM nse_ban_list
                //  JOIN max_dates ON created_at = max_dates.max_date
                //)
                //SELECT 
                //  nf.symbol_name,
                //  t.mwpl_percent AS current_percent,
                //  ROUND((nf.oi_value * 100 / NULLIF(nf.mwpl_value, 0)), 2)::DECIMAL(18,2) AS previous_percent,
                //  t.limitfornextday,
                //  t.created_at AS current_dt,
                //  nf.created_at AS previous_dt
                //FROM nse_ban_list nf
                //JOIN max_dates ON nf.created_at = max_dates.prv_date
                //JOIN temp_data t ON t.symbol_name = nf.symbol_name;

                //-- Step 2: Select max date from temporary context
                //SELECT TO_CHAR(MAX(current_dt), 'YYYY-MM-DD') AS max_date_str FROM nse_oi_futures_temp;

                //-- Step 3: Securities in ban
                //SELECT * FROM nse_oi_futures_temp
                //WHERE limitfornextday = 'No Fresh Positions'
                //ORDER BY current_percent DESC;

                //-- Step 4: Possible entrants
                //SELECT * FROM nse_oi_futures_temp
                //WHERE limitfornextday != 'No Fresh Positions'
                //  AND current_percent > 80
                //ORDER BY current_percent DESC;

                //-- Step 5: Possible exits
                //SELECT * FROM nse_oi_futures_temp
                //WHERE limitfornextday = 'No Fresh Positions'
                //  AND current_percent < 85
                //ORDER BY current_percent DESC;

                //-- Step 6: All records
                //SELECT * FROM nse_oi_futures_temp
                //ORDER BY current_percent DESC;

                //-- Step 7: Drop temp table explicitly (optional, auto-drops at session end)
                //DROP TABLE IF EXISTS nse_oi_futures_temp;
                //;


                //                        ";

                var sql = @"
CREATE TEMP TABLE nse_oi_futures_temp AS
WITH ordered_dates AS (
  SELECT DISTINCT created_at
  FROM nse_ban_list
  ORDER BY created_at DESC
  LIMIT 2
),
max_dates AS (
  SELECT 
    MAX(created_at) AS max_date,
    MIN(created_at) AS prv_date
  FROM ordered_dates
),
temp_data AS (
  SELECT *,
    ROUND((oi_value * 100 / NULLIF(mwpl_value, 0)), 2)::DECIMAL(18,2) AS mwpl_percent,
    previous_day_in_ban AS curr_previous_day_in_ban
  FROM nse_ban_list
  JOIN max_dates ON created_at = max_dates.max_date
)
SELECT 
  nf.symbol_name,
  t.mwpl_percent AS current_percent,
  ROUND((nf.oi_value * 100 / NULLIF(nf.mwpl_value, 0)), 2)::DECIMAL(18,2) AS previous_percent,
  t.limitfornextday,
  t.created_at AS current_dt,
  nf.created_at AS previous_dt,
  eq.last_trade_price,
  eq.change,
  eq.change_percent
  
FROM nse_ban_list nf left join nse_eq_stock_data_daily eq on nf.symbol_name=eq.symbol_name
JOIN max_dates ON nf.created_at = max_dates.prv_date
JOIN temp_data t ON t.symbol_name = nf.symbol_name;


SELECT * FROM nse_oi_futures_temp 
ORDER BY current_percent DESC;

DROP TABLE IF EXISTS nse_oi_futures_temp;";


                using (IDbConnection conn = _createConnection())
                {
                    using var data = await conn.QueryMultipleAsync(sql);


                    resQuote.ResultData = await data.ReadAsync<SecuritiesBanData>();
                    //resQuote.ResultData.date = await data.ReadFirstOrDefaultAsync<string>();
                    //resQuote.ResultData.securities_ban_result = await data.ReadAsync<SecuritiesBanData>();
                    //resQuote.ResultData.possible_entrants_result = await data.ReadAsync<SecuritiesBanData>();
                    //resQuote.ResultData.possible_exits_result = await data.ReadAsync<SecuritiesBanData>();
                    //resQuote.ResultData.all_list_result = await data.ReadAsync<SecuritiesBanData>();


                }

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

        public async Task<ResultObjectDTO<dynamic>> GlobalMarket()
        {
            ResultObjectDTO<dynamic> resQuote = new ResultObjectDTO<dynamic>();
            try
            {

                var sql = @"
                    SELECT
  CASE UPPER(symbol_name)
    WHEN 'US500' THEN 'S&P 500 FUTURES'
    WHEN 'US30' THEN 'DOW JONES'
    WHEN 'US100' THEN 'NASDAQ FUTURES'
    WHEN 'FRANCE40' THEN 'CAC 40'
    WHEN 'UK100' THEN 'FTSE 100'
    WHEN 'GERMANY40' THEN 'DAX'
    WHEN 'JAPAN225' THEN 'NIKKEI 225'
    WHEN 'HANGSENG' THEN 'HANG SENG'
    WHEN 'SHANGHAICHINA' THEN 'SHANGHAI'
    ELSE symbol_name
  END AS symbol_name,
  open,
  high,
  low,
  close,
  change,
  change_percent,
  last_trade_price,
  volume,
  high52,
  low52,
  created_at,
  time,
  region,
  CASE
    WHEN created_at::date = CURRENT_DATE
         AND (created_at + time) >= (CURRENT_TIMESTAMP - INTERVAL '30 minutes')
    THEN 1
    ELSE 0
  END AS market_status
FROM global_eq_stock_data_daily;
                        
                        ";


                using (IDbConnection conn = _createConnection())
                {
                    resQuote.ResultData = await conn.QueryAsync<dynamic>(sql);



                }
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

        public async Task<ResultObjectDTO<dynamic>> FiiDiiData(string segment, string participant)
        {
            ResultObjectDTO<dynamic> resQuote = new ResultObjectDTO<dynamic>();
            try
            {
                string sql = string.Empty;
                var param = new DynamicParameters();
                param.Add("@client_type", participant);
                if (segment.Equals("cash_market", StringComparison.OrdinalIgnoreCase))
                {
                    sql = @"
                    SELECT 
                            created_at,
                            MAX(CASE WHEN category = 'FII/FPI' THEN buy_value END) AS fii_buy_value,
                            MAX(CASE WHEN category = 'FII/FPI' THEN sell_value END) AS fii_sell_value,
                            MAX(CASE WHEN category = 'FII/FPI' THEN net_value END) AS fii_net_value,
                            
                            MAX(CASE WHEN category = 'DII' THEN buy_value END) AS dii_buy_value,
                            MAX(CASE WHEN category = 'DII' THEN sell_value END) AS dii_sell_value,
                            MAX(CASE WHEN category = 'DII' THEN net_value END) AS dii_net_value
                        
                        FROM fii_cash
                        GROUP BY created_at 
                        ORDER BY created_at DESC  limit 30;
                        
                        ";

                }
                else if (segment.Equals("index_future", StringComparison.OrdinalIgnoreCase))
                {
                    sql = @"select  CAST(created_at as date) created_at,future_index_long,future_index_short,(future_index_long-future_index_short) future_index_net,
	(future_index_long-LAG(future_index_long) OVER (ORDER BY CAST(created_at as date) desc)) AS future_index_long_change,  
	(future_index_short-LAG(future_index_short) OVER (ORDER BY CAST(created_at as date) desc)) AS future_index_short_change,	
	((future_index_long-future_index_short)-LAG((future_index_long-future_index_short)) OVER (ORDER BY CAST(created_at as date) desc)) AS future_index_net_change
from fao_data  where client_type=@client_type  order by cast(created_at as date) desc limit  20;";



                }


                else if (segment.Equals("index_option", StringComparison.OrdinalIgnoreCase))
                {
                    sql = @"select CAST(created_at as date) created_at,
	            option_index_call_long,option_index_call_short,(option_index_call_long - option_index_call_short) option_index_call_net,
	option_index_put_long,option_index_put_short,(option_index_put_long - option_index_put_short) option_index_put_net,

	(option_index_call_long - LAG(option_index_call_long) OVER(ORDER BY CAST(created_at as date) desc)) AS option_index_call_long_change,
	(option_index_call_short - LAG(option_index_call_short) OVER(ORDER BY CAST(created_at as date) desc)) AS option_index_call_short_change,
	(option_index_put_long - LAG(option_index_put_long) OVER(ORDER BY CAST(created_at as date) desc)) AS option_index_put_long_change,
	(option_index_put_short - LAG(option_index_put_short) OVER(ORDER BY CAST(created_at as date) desc)) AS option_index_put_short_change,
	((option_index_call_long - option_index_call_short) - LAG((option_index_call_long - option_index_call_short)) OVER(ORDER BY CAST(created_at as date) desc)) AS option_index_call_net_change,
	((option_index_put_long - option_index_put_short) - LAG((option_index_put_long - option_index_put_short)) OVER(ORDER BY CAST(created_at as date) desc)) AS option_index_put_net_change

from fao_data  where client_type = @client_type  order by cast(created_at as date) desc limit 20 ;";
                }

                else if (segment.Equals("stock_future", StringComparison.OrdinalIgnoreCase))
                {
                    sql = @"select CAST(created_at as date) created_at,future_stock_long,future_stock_short,(future_stock_long-future_stock_short) future_stock_net,
	(future_stock_long-LAG(future_stock_long) OVER (ORDER BY CAST(created_at as date) desc)) AS future_stock_long_change,  
	(future_stock_short-LAG(future_stock_short) OVER (ORDER BY CAST(created_at as date) desc)) AS future_stock_short_change,	
	((future_stock_long-future_stock_short)-LAG((future_stock_long-future_stock_short)) OVER (ORDER BY CAST(created_at as date) desc)) AS future_stock_net_change
from fao_data where client_type=@client_type  order by cast(created_at as date) desc limit 20;";
                }

                else if (segment.Equals("stock_option", StringComparison.OrdinalIgnoreCase))
                {
                    sql = @"select  CAST(created_at as date) created_at,
	option_stock_call_long,option_stock_call_short,(option_stock_call_long-option_stock_call_short) option_stock_call_net,
	option_stock_put_long,option_stock_put_short,(option_stock_put_long-option_stock_put_short) option_stock_put_net,

	(option_stock_call_long-LAG(option_stock_call_long) OVER (ORDER BY CAST(created_at as date) desc)) AS option_stock_call_long_change,
	(option_stock_call_short-LAG(option_stock_call_short) OVER (ORDER BY CAST(created_at as date) desc)) AS option_stock_call_short_change,
	(option_stock_put_long-LAG(option_stock_put_long) OVER (ORDER BY CAST(created_at as date) desc)) AS option_stock_put_long_change,
	(option_stock_put_short-LAG(option_stock_put_short) OVER (ORDER BY CAST(created_at as date) desc)) AS option_stock_put_short_change,
	((option_stock_call_long-option_stock_call_short)-LAG((option_stock_call_long-option_stock_call_short)) OVER (ORDER BY CAST(created_at as date) desc)) AS option_stock_call_net_change,
	((option_stock_put_long-option_stock_put_short)-LAG((option_stock_put_long-option_stock_put_short)) OVER (ORDER BY CAST(created_at as date) desc)) AS option_stock_put_net_change

from fao_data where client_type=@client_type  order by cast(created_at as date) desc  limit 20;";
                }


                using (IDbConnection conn = _createConnection())
                {
                    resQuote.ResultData = await conn.QueryAsync<dynamic>(sql, param);

                }
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

        public async Task InsertNseEqStockHistoricalDailyAsync()
        {
            var sql = @"
                INSERT INTO nse_eq_stock_historical_daily
                SELECT *
                FROM public.nse_eq_stock_data_daily
                WHERE created_at = CURRENT_DATE
                AND NOT EXISTS (
                    SELECT 1
                    FROM nse_eq_stock_historical_daily
                    WHERE created_at = CURRENT_DATE
                    LIMIT 1
                );
            ";
            try
            {
                using var conn = _createConnection();
                await conn.OpenAsync();
                using var cmd = new NpgsqlCommand(sql, conn);
                await cmd.ExecuteNonQueryAsync();

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting NSE EQ stock historical daily data: {ex.Message}");
            }

        }

        public async Task<ResultObjectDTO<dynamic>> IndianIndices(string request)
        {
            ResultObjectDTO<dynamic> resQuote = new ResultObjectDTO<dynamic>();
            try
            {

                var sql = @"
                        select symbol_name,open,high,low,close,change,change_percent,last_trade_price From nse_eq_stock_data_daily where symbol_name IN ('NIFTY 50','NIFTY BANK','NIFTY FIN SERVICE','NIFTY MIDCAP SELECT (MIDCPNIFTY)','INDIA VIX','NIFTY TOTAL MKT','NIFTY NEXT 50','NIFTY 100','NIFTY MIDCAP 100','NIFTY 500','NIFTY AUTO','NIFTY SMLCAP 100','NIFTY FMCG','NIFTY METAL','NIFTY PHARMA','NIFTY PSU BANK','NIFTY IT','NIFTY SMLCAP 250','NIFTY MIDCAP 150','NIFTY COMMODITIES');
                        
                        ";
                using var conn = _createConnection();
                await conn.OpenAsync();
                resQuote.ResultData = await conn.QueryAsync<dynamic>(sql);

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
