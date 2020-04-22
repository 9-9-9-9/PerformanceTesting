using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Neo4j.Driver;
using SharedLib;
using SharedLib.Extensions;

namespace StopWatcher.TestCases
{
    public class QueryOfMatchMultipleRel : AbstractTestCase
    {
        private const string Label = "NBigRel3";
        private const string Rel = "HAS_REL2";

        private const int MaxNoOfRel = 500_000;
        private const int BatchSize = 1_000;
        private const int PoolSize = 3;

        protected override async Task DoWorkAsync()
        {
            await PrepareAsync();

            int noOfRel, relMaxNo;
            using (var conn = DbHelper.Neo4J.Connection)
            {
                var noOfNodes = await conn.ReadAsync<int>($"MATCH (n:{Label}) RETURN COUNT(n)");

                Console.WriteLine($"> {noOfNodes} nodes");

                if (noOfNodes < 2)
                    throw new InvalidDataException("Missing nodes");

                (noOfRel, relMaxNo) = await await conn.ReadAsync(
                    $"MATCH (n1:{Label} {{n: 1}})-[r:{Rel}]->(n2:{Label}) RETURN COUNT(r), MAX(r.n)",
                    async cursor =>
                    {
                        if (await cursor.FetchAsync())
                        {
                            return (cursor.Current[0].As<int>(), cursor.Current[1].As<int?>() ?? 0);
                        }

                        throw new NoDataException();
                    });
            }

            Console.WriteLine($"> {noOfRel} existing relation, max no = {relMaxNo}");

            if (noOfRel < MaxNoOfRel)
            {
                var toBeCreated = MaxNoOfRel - noOfRel;
                var nextNo = relMaxNo + 1;

                var numberOfBatches = (int) Math.Ceiling((double) toBeCreated / BatchSize);
                Console.WriteLine($"> {numberOfBatches} batches");

                foreach (var paged in Enumerable.Range(0, numberOfBatches).Paged(PoolSize))
                {
                    Console.WriteLine("Page starts");
                    var tasks = paged.Select(async b =>
                    {
                        Console.WriteLine($">> Batch no.{b}/{numberOfBatches} started at {DateTime.Now:HH:mm:ss}");
                        var from = b;
                        var to = b + BatchSize - 1;
                        if (to >= MaxNoOfRel)
                            to = MaxNoOfRel - 1;

                        var writeSession = DbHelper.Neo4J.OpenNeo4JAsyncSession();
                        var sb = new StringBuilder();

                        sb.Append($"MATCH (a:{Label}), (b:{Label}) WHERE a.n = 1 AND b.n = 2 ");
                        for (var i = from; i <= to; i++)
                        {
                            var idx = Interlocked.Increment(ref nextNo);
                            sb.Append($"CREATE (a)-[r{idx}:{Rel}]->(b) SET r{idx}.n={idx} ");
                        }

                        Console.WriteLine(
                            $">> Batch no.{b}/{numberOfBatches} executing cypher query at {DateTime.Now:HH:mm:ss}");

                        await writeSession.ExecuteAsync(sb.ToString());

                        Console.WriteLine(
                            $">> Batch no.{b}/{numberOfBatches} executed query successfully at {DateTime.Now:HH:mm:ss}");

                        await writeSession.CloseAsync();

                        Console.WriteLine($">> Batch no.{b}/{numberOfBatches} finished at {DateTime.Now:HH:mm:ss}");
                    }).ToArray();

                    await Task.WhenAll(tasks);
                    Console.WriteLine("Page done");
                }
            }

            Console.WriteLine(">> Verify");

            using (var conn = DbHelper.Neo4J.Connection)
            {
                (noOfRel, relMaxNo) = await await conn.ReadAsync(
                    $"MATCH (n1:{Label} {{n: 1}})-[r:{Rel}]->(n2:{Label}) RETURN COUNT(r), MAX(r.n)",
                    async cursor =>
                    {
                        if (await cursor.FetchAsync())
                        {
                            return (cursor.Current[0].As<int>(), cursor.Current[1].As<int?>() ?? 0);
                        }

                        throw new NoDataException();
                    });

                if (noOfRel < MaxNoOfRel)
                {
                    Console.WriteLine($"Un-expected number of rel had been found. Expected {MaxNoOfRel} but found {noOfRel}, offset is {MaxNoOfRel - noOfRel}");
                    Environment.Exit(1);
                }

                Console.WriteLine($"> {noOfRel} existing relation, max no = {relMaxNo}");

                var stopWatch = new Stopwatch();
                var best = long.MaxValue;
                var tries = 5;
                do
                {
                    await Task.Delay(5000);
                    stopWatch.Reset();
                    stopWatch.Start();
                    var found = await conn.ReadAsync<int>(
                        $"MATCH (n1:{Label} {{n: 1}})-[r:{Rel}]->(n2:{Label}) WHERE r.n={relMaxNo + 1} RETURN COUNT(r)"
                    );
                    stopWatch.Stop();
                    Console.WriteLine($"> Try {tries} found {found} record after {stopWatch.ElapsedMilliseconds} ms");

                    best = Math.Min(best, stopWatch.ElapsedMilliseconds);
                } while (--tries > 0);
                
                Console.WriteLine($"> Best record is {best} ms for {noOfRel} relations");
            }
        }

        protected override async Task PrepareAsync()
        {
            using (var conn = DbHelper.Neo4J.Connection)
            {
                await conn.WriteAsync($"MERGE (node:{Label} {{n:1}})");
                await conn.WriteAsync($"MERGE (node:{Label} {{n:2}})");
            }
        }

        public override Task PrintResultAsync()
        {
            return Task.CompletedTask;
        }

        public override Task SaveResultAsync()
        {
            return Task.CompletedTask;
        }
    }
}