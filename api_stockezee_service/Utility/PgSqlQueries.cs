namespace api_stockezee_service.Utility
{
    public static class PgSqlQueries
    {
        public const string Select_Orb_Range = @"
            SELECT 
    eq.symbol_name,
    TIME '09:30:00' AS time,
    MAX(eq.high) AS high,
    MAX(eq.low) AS low,
    0 AS close,
	br.current_score,
	br.last_direction
FROM 
    public.nse_eq_stock_data_intraday_daily eq
JOIN 
    nse_fno_lot_size fn ON eq.symbol_name = fn.symbol
LEFT JOIN 	public.range_breakout br on eq.symbol_name=br.symbol_name
WHERE 
    eq.time BETWEEN TIME '09:15:00' AND TIME '09:30:00'
GROUP BY 
    eq.symbol_name,br.current_score,br.last_direction;
        ";


        public const string Select_Range_Current = @"select symbol_name,time,high,low,eq.last_trade_price close from public.nse_eq_stock_data_intraday_daily eq JOIN 
    nse_fno_lot_size fn ON eq.symbol_name = fn.symbol where time=@time";


        public const string Update_Breakout_Current = @"
        INSERT INTO range_breakout (
            symbol_name, time, break_direction, break_point,
            current_score, last_direction, created_at
        )
        VALUES (
            @SymbolName, @Time, @BreakDirection, @BreakPoint,
            @CurrentScore, @LastDirection, @CreatedAt
        )
        ON CONFLICT (symbol_name)
        DO UPDATE SET
            time = EXCLUDED.time,
            break_direction = EXCLUDED.break_direction,
            break_point = EXCLUDED.break_point,
            current_score = EXCLUDED.current_score,
            last_direction = EXCLUDED.last_direction,
            created_at = EXCLUDED.created_at;
    ";

        public const string Update_Breakout_Intraday = @"
        INSERT INTO range_breakout_intraday (
            symbol_name, time, break_direction, break_point,
            current_score, created_at
        )
        VALUES (
            @SymbolName, @Time, @BreakDirection, @BreakPoint,
            @CurrentScore, @CreatedAt
        )
        ON CONFLICT (symbol_name, time)
        DO UPDATE SET
            break_direction = EXCLUDED.break_direction,
            break_point = EXCLUDED.break_point,
            current_score = EXCLUDED.current_score,
            created_at = EXCLUDED.created_at;
    ";

    }
}
