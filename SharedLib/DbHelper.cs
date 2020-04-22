using System;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using Neo4j.Driver;
using Npgsql;

namespace SharedLib
{
    public static class DbHelper
    {
        public static class PostgreSql
        {
            public static async Task<NpgsqlConnection> OpenConnectionAsync()
            {
                var npgSqlConnection = new NpgsqlConnection(ConnectionManager.PostgresConnectionString);
                await npgSqlConnection.OpenAsync();
                return npgSqlConnection;
            }
        }

        public static class Neo4J
        {
            public static IAsyncSession OpenNeo4JAsyncSession() => ConnectionManager.Neo4JDriver.AsyncSession();

            public static Neo4JConnection Connection => new Neo4JConnection();

            public class Neo4JConnection : IDisposable
            {
                private readonly IDriver _driver;

                private readonly IAsyncSession _session;

                public Neo4JConnection()
                {
                    _driver =
                        GraphDatabase.Driver(ConnectionManager.Neo4JHost,
                            AuthTokens.Basic(ConnectionManager.Neo4JUser, ConnectionManager.Neo4JPass)
                        );
                    _session = _driver.AsyncSession();
                }

                public async Task WriteAsync(string commands)
                {
                    var cursor = await _session.RunAsync(commands);
                    await cursor.ConsumeAsync();
                }

                public async Task<TResult> ReadAsync<TResult>(string commands,
                    Func<IResultCursor, TResult> dataExtractor)
                {
                    var cursor = await _session.RunAsync(commands);
                    return dataExtractor(cursor);
                }

                public void Dispose()
                {
                    _session?.CloseAsync().Wait();
                    _driver?.CloseAsync().Wait();
                    _driver?.Dispose();
                }
            }
        }
    }

    public static class Neo4JConnectionExtensions
    {
        public static async Task<T> ReadAsync<T>(this DbHelper.Neo4J.Neo4JConnection conn, string command)
        {
            return await await conn.ReadAsync(command, async cursor =>
            {
                if (await cursor.FetchAsync())
                    return cursor.Current[0].As<T>();

                throw new NoDataException();
            });
        }
    }

    public static class PostgreSqlHelperExtensions
    {
        public static async Task ExecuteAsync(this NpgsqlConnection connection, string command)
        {
            using var cmd = new NpgsqlCommand(command, connection);
            await cmd.ExecuteNonQueryAsync();
        }

        public static async Task<T> ReadAsync<T>(this NpgsqlConnection connection, string command,
            Func<NpgsqlDataReader, T> resultExtractor)
        {
            using var cmd = new NpgsqlCommand(command, connection);

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
            var cursor = await asyncSession.RunAsync(command);
            await cursor.ConsumeAsync();
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