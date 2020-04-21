using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Neo4j.Driver;

namespace Neo4jVsPostgreSQL.BenchMarks
{
    [DryJob]
    [RPlotExporter]
    public class CompareOperationsBetweenDBs
    {
        // ReSharper disable once UnassignedField.Global
        [Params(DbNeo4J, "PostgreSQL")] public string Db;

        private const int OperationRecords = 500;
        private const string TableOrder = "OrdersTmp";
        private const string DbNeo4J = "Neo4J";

        [IterationSetup(Targets = new[] {nameof(Count), nameof(Sum), nameof(OrderBy)})]
        public void SetupOperations()
        {
            Task.Run(async () =>
            {
                // Prepare neo4j
                var asyncSession = DbHelper.Neo4JHelper.OpenNeo4JAsyncSession();
                await asyncSession.TruncateAsync(TableOrder);
                try
                {
                    await asyncSession.ExecuteAsync($"DROP INDEX ON :{TableOrder}(orderId)");
                }
                catch
                {
                    //
                }

                await asyncSession.ExecuteAsync($"CREATE INDEX ON :{TableOrder}(orderId)");

                // Prepare PostgreSQL
                // ReSharper disable once StringLiteralTypo
                const string indexNameOnProperty = "myidx3";
                await using var conn = await DbHelper.PostgreSqlHelper.OpenConnectionAsync();
                await conn.ExecuteAsync($"DROP INDEX IF EXISTS {indexNameOnProperty}");
                await conn.CreateTableAsync(TableOrder, "orderId", "price");
                await conn.ExecuteAsync($"CREATE INDEX {indexNameOnProperty} ON {TableOrder.ToLower()}(orderId)");

                // Prepare records
                for (var c = 1; c <= OperationRecords; c++)
                {
                    var orderId = c;
                    var price = OperationRecords - c;
                    await asyncSession.ExecuteAsync(
                        $"CREATE (o:{TableOrder}) " +
                        $"SET o.orderId = {orderId}, " +
                        $"o.price = {price}"
                    );

                    await conn.ExecuteAsync(
                        $"INSERT INTO {TableOrder.ToLower()}(orderId, price) VALUES ({orderId}, {price})"
                    );
                }
            }).Wait();
        }

        [Benchmark]
        public async Task<int> Count()
        {
            if (Db == DbNeo4J)
            {
                var asyncSession = DbHelper.Neo4JHelper.OpenNeo4JAsyncSession();
                var record = await asyncSession.ReadAsync(
                    $"MATCH (o:{TableOrder}) RETURN COUNT(*)"
                );
                return record[0].As<int>();
            }
            else
            {
                await using var conn = await DbHelper.PostgreSqlHelper.OpenConnectionAsync();
                return await conn.ReadAsync(
                    $"SELECT COUNT(*) FROM {TableOrder.ToLower()}",
                    reader => reader.GetInt32(0)
                );
            }
        }

        [Benchmark]
        public async Task<int> Sum()
        {
            if (Db == DbNeo4J)
            {
                var asyncSession = DbHelper.Neo4JHelper.OpenNeo4JAsyncSession();
                var record = await asyncSession.ReadAsync(
                    $"MATCH (o:{TableOrder}) RETURN SUM(o.price)"
                );
                return record[0].As<int>();
            }
            else
            {
                await using var conn = await DbHelper.PostgreSqlHelper.OpenConnectionAsync();
                return await conn.ReadAsync(
                    $"SELECT SUM(price) FROM {TableOrder.ToLower()}",
                    reader => reader.GetInt32(0)
                );
            }
        }

        [Benchmark]
        public async Task<int> OrderBy()
        {
            if (Db == DbNeo4J)
            {
                var asyncSession = DbHelper.Neo4JHelper.OpenNeo4JAsyncSession();
                var record = await asyncSession.ReadAsync(
                    $"MATCH (o:{TableOrder}) RETURN o.price ORDER BY o.price"
                );
                return record[0].As<int>();
            }
            else
            {
                await using var conn = await DbHelper.PostgreSqlHelper.OpenConnectionAsync();
                return await conn.ReadAsync(
                    $"SELECT price FROM {TableOrder.ToLower()} ORDER BY price",
                    reader => reader.GetInt32(0)
                );
            }
        }
    }
}