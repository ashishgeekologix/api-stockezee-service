using api_stockezee_service.Models;
using api_stockezee_service.Models.Entities.Resource;
using Dapper;
using Newtonsoft.Json;
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

        public async Task<ResultObjectDTO<FnoLotSizeDetailData>> FnoLotSize()
        {
            ResultObjectDTO<FnoLotSizeDetailData> resQuote = new ResultObjectDTO<FnoLotSizeDetailData>();
            try
            {

                var sql = @"
                    WITH extracted_keys AS (
    SELECT DISTINCT
        jsonb_object_keys(month_data::jsonb) AS key
    FROM public.nse_fno_lot_size
),
key_with_month_order AS (
    SELECT 
        key,
        TO_DATE('01-' || key, 'DD-Mon-YY') AS month_sort
    FROM extracted_keys
)

SELECT jsonb_build_object(
  'data', jsonb_agg(
    jsonb_build_object(
      'underlying', underlying,
      'symbol', symbol,
      'created_at', TO_CHAR(created_at, 'MM/DD/YYYY HH24:MI:SS'),
      'month_data', month_data
    )
  ),
  'key', (
    SELECT jsonb_agg(key ORDER BY month_sort)
    FROM key_with_month_order
  )
) AS response_json
FROM public.nse_fno_lot_size;
                        
                        ";


                using (IDbConnection conn = _createConnection())
                {
                    var json = await conn.QueryFirstOrDefaultAsync<string>(sql);

                    resQuote.ResultData = JsonConvert.DeserializeObject<FnoLotSizeDetailData>(json);

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


        public async Task<ResultObjectDTO<dynamic>> ForthComingResult()
        {
            ResultObjectDTO<dynamic> resQuote = new ResultObjectDTO<dynamic>();
            try
            {

                var sql = @"
                    SELECT 
  fd.scrip_code,
  fd.short_name AS symbol,
  UPPER(fd.long_name) AS long_name,
  fd.meeting_date::DATE AS meeting_date
FROM forth_comming_result fd
ORDER BY 
  fd.meeting_date::DATE,
  fd.short_name;
                        
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
    }
}
