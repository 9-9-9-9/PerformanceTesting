using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using SharedLib;

namespace Neo4jVsPostgreSQL.BenchMarks
{
    // [DryJob]
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
                using var n4JConn = DbHelper.Neo4J.Connection;
                await n4JConn.TruncateAsync(TableOrder);
                try
                {
                    await n4JConn.WriteAsync($"DROP INDEX ON :{TableOrder}(orderId)");
                }
                catch
                {
                    //
                }

                await n4JConn.WriteAsync($"CREATE INDEX ON :{TableOrder}(orderId)");

                // Prepare PostgreSQL
                // ReSharper disable once StringLiteralTypo
                const string indexNameOnProperty = "myidx3";
                await using var conn = await DbHelper.PostgreSql.OpenConnectionAsync();
                await conn.ExecuteAsync($"DROP INDEX IF EXISTS {indexNameOnProperty}");
                await conn.CreateTableAsync(TableOrder, "orderId", "price");
                await conn.ExecuteAsync($"CREATE INDEX {indexNameOnProperty} ON {TableOrder.ToLower()}(orderId)");

                // Prepare records
                for (var c = 1; c <= OperationRecords; c++)
                {
                    var orderId = c;
                    var price = OperationRecords - c;
                    await n4JConn.WriteAsync(
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
                using var conn = DbHelper.Neo4J.Connection;
                return await conn.ReadAsync<int>(
                    $"MATCH (o:{TableOrder}) RETURN COUNT(*)"
                );
            }
            else
            {
                await using var conn = await DbHelper.PostgreSql.OpenConnectionAsync();
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
                using var conn = DbHelper.Neo4J.Connection;
                return await conn.ReadAsync<int>(
                    $"MATCH (o:{TableOrder}) RETURN SUM(o.price)"
                );
            }
            else
            {
                await using var conn = await DbHelper.PostgreSql.OpenConnectionAsync();
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
                using var conn = DbHelper.Neo4J.Connection;
                return await conn.ReadAsync<int>(
                    $"MATCH (o:{TableOrder}) RETURN o.price ORDER BY o.price"
                );
            }
            else
            {
                await using var conn = await DbHelper.PostgreSql.OpenConnectionAsync();
                return await conn.ReadAsync(
                    $"SELECT price FROM {TableOrder.ToLower()} ORDER BY price",
                    reader => reader.GetInt32(0)
                );
            }
        }
    }
}