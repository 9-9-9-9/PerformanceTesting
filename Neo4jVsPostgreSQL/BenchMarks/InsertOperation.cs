using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using SharedLib;

namespace Neo4jVsPostgreSQL.BenchMarks
{
    [DryJob]
    [RPlotExporter]
    public class InsertOperation
    {
        // ReSharper disable once UnassignedField.Global
        [Params(DbNeo4J, "PostgreSQL")] public string Db;

        private const int InsertRecords = 1000;
        private const string TableBenchInsert = "BenchInsert";
        private const string IndexNameOnProperty = "myidx1";
        private const string DbNeo4J = "Neo4J";

        [IterationSetup(Target = nameof(InsertWoIdx))]
        public void SetupInsertWithoutIndex()
        {
            Task.Run(async () =>
            {
                var asyncSession = DbHelper.Neo4J.OpenNeo4JAsyncSession();
                await asyncSession.TruncateAsync(TableBenchInsert);
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
                var asyncSession = DbHelper.Neo4J.OpenNeo4JAsyncSession();
                for (var c = 1; c <= InsertRecords; c++)
                    await asyncSession.ExecuteAsync(
                        $"CREATE (o:{TableBenchInsert}) " +
                        "SET o.prop2 = " + c
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

        [IterationSetup(Target = nameof(InsertWithIdx))]
        public void SetupInsertWithIndex()
        {
            Task.Run(async () =>
            {
                var asyncSession = DbHelper.Neo4J.OpenNeo4JAsyncSession();
                await asyncSession.TruncateAsync(TableBenchInsert);
                try
                {
                    await asyncSession.ExecuteAsync($"DROP INDEX ON :{TableBenchInsert}(prop2)");
                }
                catch
                {
                    //
                }

                await asyncSession.ExecuteAsync($"CREATE INDEX ON :{TableBenchInsert}(prop2)");
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
                var asyncSession = DbHelper.Neo4J.OpenNeo4JAsyncSession();
                for (var c = 1; c <= InsertRecords; c++)
                    await asyncSession.ExecuteAsync(
                        $"CREATE (o:{TableBenchInsert}) " +
                        "SET o.prop2 = " + c
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