using Npgsql;

namespace api_stockezee_service.Service
{
    public class PgDbService
    {
        private readonly Func<NpgsqlConnection> _createConnection;

        public PgDbService(Func<NpgsqlConnection> createConnection)
        {
            _createConnection = createConnection;
        }


    }
}
