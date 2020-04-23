using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using SharedLib;

namespace Neo4jVsPostgreSQL.BenchMarks
{
    // [DryJob]
    [RPlotExporter]
    public class InsertOperation
    {
        // ReSharper disable once UnassignedField.Global
        [Params(DbNeo4J, "PostgreSQL")] public string Db;

        private const int InsertRecords = 1000;
        private const string TableBenchInsert = "BenchInsert";
        private const string IndexNameOnProperty = "myidx1";
        private const string DbNeo4J = "Neo4J";

        [GlobalSetup(Target = nameof(InsertWoIdx))]
        public void SetupInsertWithoutIndex()
        {
            Task.Run(async () =>
            {
                using var conn = DbHelper.Neo4J.Connection;
                await conn.TruncateAsync(TableBenchInsert);
            }).Wait();
            
            Task.Run(async () =>
            {
                await using var conn = await DbHelper.PostgreSql.OpenConnectionAsync();
                await conn.CreateTableAsync(TableBenchInsert, "prop1", "prop2");
            }).Wait();
        }

        [Benchmark]
        public async Task InsertWoIdx()
        {
            if (Db == DbNeo4J)
            {
                using var conn = DbHelper.Neo4J.Connection;
                for (var c = 1; c <= InsertRecords; c++)
                    await conn.WriteAsync(
                        $@"
CREATE (o:{TableBenchInsert})
SET o.prop2 = {c}"
                    );
            }
            else
            {
                await using var conn = await DbHelper.PostgreSql.OpenConnectionAsync();
                for (var c = 1; c <= InsertRecords; c++)
                    await conn.ExecuteAsync(
                        $"INSERT INTO {TableBenchInsert.ToLower()}(prop2) VALUES ({c})"
                    );
            }
        }

        [GlobalSetup(Target = nameof(InsertWithIdx))]
        public void SetupInsertWithIndex()
        {
            Task.Run(async () =>
            {
                using var conn = DbHelper.Neo4J.Connection;
                await conn.TruncateAsync(TableBenchInsert);
                try
                {
                    await conn.WriteAsync($"DROP INDEX ON :{TableBenchInsert}(prop2)");
                }
                catch
                {
                    //
                }

                await conn.WriteAsync($"CREATE INDEX ON :{TableBenchInsert}(prop2)");
            }).Wait();
            
            Task.Run(async () =>
            {
                await using var conn = await DbHelper.PostgreSql.OpenConnectionAsync();
                await conn.ExecuteAsync($"DROP INDEX IF EXISTS {IndexNameOnProperty}");
                await conn.CreateTableAsync(TableBenchInsert, "prop1", "prop2");
                await conn.ExecuteAsync($"CREATE INDEX {IndexNameOnProperty} ON {TableBenchInsert.ToLower()}(prop2)");
            }).Wait();
        }

        [Benchmark]
        public async Task InsertWithIdx()
        {
            if (Db == DbNeo4J)
            {
                using var conn = DbHelper.Neo4J.Connection;
                for (var c = 1; c <= InsertRecords; c++)
                    await conn.WriteAsync(
                        $@"
CREATE (o:{TableBenchInsert})
SET o.prop2 = {c}"
                    );
            }
            else
            {
                await using var conn = await DbHelper.PostgreSql.OpenConnectionAsync();
                for (var c = 1; c <= InsertRecords; c++)
                    await conn.ExecuteAsync(
                        $"INSERT INTO {TableBenchInsert.ToLower()}(prop2) VALUES ({c})"
                    );
            }
        }
    }
}