using System;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using Neo4j.Driver;
using Npgsql;

namespace Neo4jVsPostgreSQL
{
    public static class DbHelper
    {
        public static class PostgreSqlHelper
        {
            public static async Task<NpgsqlConnection> OpenConnectionAsync()
            {
                var npgSqlConnection = new NpgsqlConnection(ConnectionManager.PostgresConnectionString);
                await npgSqlConnection.OpenAsync();
                return npgSqlConnection;
            }
        }

        public static class Neo4JHelper
        {
            public static IAsyncSession OpenNeo4JAsyncSession() => ConnectionManager.Neo4JDriver.AsyncSession();
        }
    }

    public static class PostgreSqlHelperExtensions
    {
        public static async Task ExecuteAsync(this NpgsqlConnection connection, string command)
        {
            await using var cmd = new NpgsqlCommand(command, connection);
            await cmd.ExecuteNonQueryAsync();
        }

        public static async Task<T> ReadAsync<T>(this NpgsqlConnection connection, string command,
            Func<NpgsqlDataReader, T> resultExtractor)
        {
            await using var cmd = new NpgsqlCommand(command, connection);

            var dataReader = await cmd.ExecuteReaderAsync();
            if (!dataReader.Read())
                throw new DataException("No response");
            return resultExtractor(dataReader);
        }

        public static async Task CreateTableAsync(this NpgsqlConnection dbConnection, string table,
            params string[] properties)
        {
            table = table.ToLower();
            await dbConnection.ExecuteAsync($"DROP TABLE IF EXISTS {table}");

            var sb = new StringBuilder($"CREATE TABLE {table} (");
            var first = true;
            foreach (var property in properties)
            {
                sb.Append($"{property} int4 NULL ");
                if (!first) continue;
                first = false;
                sb.Append(", ");
            }

            sb.Append(");");
            await dbConnection.ExecuteAsync(sb.ToString());
        }
    }

    public static class Neo4JHelperExtensions
    {
        public static async Task ExecuteAsync(this IAsyncSession asyncSession, string command)
        {
            await asyncSession.WriteTransactionAsync(async tx => { await tx.RunAsync(command); });
        }

        public static async Task<IRecord> ReadAsync(this IAsyncSession asyncSession, string command)
        {
            Console.WriteLine(command);
            return await asyncSession.ReadTransactionAsync(async tx =>
            {
                var result = await tx.RunAsync(command);
                if (!await result.FetchAsync())
                    throw new DataException("No response");
                return result.Current;
            });
        }

        public static Task TruncateAsync(this IAsyncSession asyncSession, string label)
            => asyncSession.ExecuteAsync($@"
MATCH (n:{label})
DELETE n
");
    }
}