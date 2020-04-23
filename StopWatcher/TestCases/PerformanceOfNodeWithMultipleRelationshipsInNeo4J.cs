using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Neo4j.Driver;
using SharedLib;
using SharedLib.Extensions;

namespace StopWatcher.TestCases
{
    /*
    public class PerformanceOfNodeWithMultipleRelationshipsInNeo4J : AbstractTestCase
    {
        private readonly ConcurrentDictionary<int, long> _result = new ConcurrentDictionary<int, long>();
        private readonly string _nodeLabel = "NBigRel";
        private readonly int[] _testCasesNumberOfRelationships = new[]
        {
            1,
            10,
            100,
            1_000,
            10_000, 
            100_000, 
            200_000, 
            300_000, 
            400_000, 
            500_000, 
            600_000, 
            700_000, 
            800_000, 
            900_000, 
            1_000_000
        };
        private const int BatchOfCreatingRelationships = 10_000;
        private const byte PoolSize = 3;

        public override async Task RunAsync()
        {
            try
            {
                Console.WriteLine($"> Using node label :{_nodeLabel}");
                Console.WriteLine("> Preparing data");
                await PrepareAsync();
                Console.WriteLine("> Finished in data preparation");
                Console.WriteLine($"> Before {nameof(DoWorkAsync)}");
                await DoWorkAsync();
                Console.WriteLine($"> After {nameof(DoWorkAsync)}");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        protected override async Task PrepareAsync()
        {
            var asyncSession = DbHelper.Neo4J.OpenNeo4JAsyncSession();
            
            Console.WriteLine(">> Remove all existing nodes of same label name");
            await asyncSession.ExecuteAsync($"MATCH (n1:{_nodeLabel})-[r:HAS_REL]->(n2:{_nodeLabel}) DELETE r");
            await asyncSession.ExecuteAsync($"MATCH (n:{_nodeLabel}) DELETE n");
            
            Console.WriteLine(">> Create 2 fresh nodes");
            await asyncSession.ExecuteAsync($"CREATE (a:{_nodeLabel}), (b:{_nodeLabel}) SET a.n = 1, b.n = 2");
            
            await asyncSession.CloseAsync();
        }

        protected override async Task DoWorkAsync()
        {
            var numberOfCreatedRelationship = 0;
            foreach (var numberOfRelationships in _testCasesNumberOfRelationships.OrderBy(n => n))
            {
                try
                {
                    Console.WriteLine($"> Before {nameof(DoTestWithInput)}({numberOfRelationships})");
                    await DoTestWithInput(numberOfRelationships, numberOfCreatedRelationship);
                    Console.WriteLine($"> After {nameof(DoTestWithInput)}({numberOfRelationships})");

                    numberOfCreatedRelationship = numberOfRelationships;
                    
                    try
                    {
                        await PrintResultAsync();
                        Console.WriteLine($"> After {nameof(PrintResultAsync)}");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        Console.WriteLine("Error occured while trying to print result");
                    }

                    await SaveResultAsync();
                    Console.WriteLine($"> After {nameof(SaveResultAsync)}");
                }
                catch
                {
                    Console.WriteLine($"> (ERR) {nameof(DoTestWithInput)}({numberOfRelationships})");
                    throw;
                }
            }
        }

        private async Task DoTestWithInput(int numberOfRelationships, int numberOfExistingRelationships)
        {
            var toBeCreated = numberOfRelationships - numberOfExistingRelationships;
            Console.WriteLine($"> Going to create {toBeCreated} more relationships");

            var numberOfBatches = (int)Math.Ceiling((double)toBeCreated / BatchOfCreatingRelationships);
            Console.WriteLine($"> {numberOfBatches} batches");

            foreach (var batch in Enumerable.Range(0, numberOfBatches).Paged(PoolSize))
            {
                var tasks = batch.Select(b =>
                {
                    Console.WriteLine($">> Batch no.{b} ({DateTime.Now:HH:mm:ss})");
                    var from = b;
                    var to = b + BatchOfCreatingRelationships - 1;
                    if (to > numberOfRelationships)
                        to = numberOfRelationships;

                    var writeSession = DbHelper.Neo4J.OpenNeo4JAsyncSession();
                    var sb = new StringBuilder();

                    sb.Append($"MATCH (a:{_nodeLabel}), (b:{_nodeLabel}) WHERE a.n = 1 AND b.n = 2 ");
                    for (var i = from; i <= to; i++)
                        sb.Append($"CREATE (a)-[r{i}:HAS_REL]->(b) SET r{i}.n={i}, r{i}.i={i} ");
                    
                    return writeSession.ExecuteAsync(sb.ToString())
                        .ContinueWith(task => writeSession.CloseAsync(), TaskContinuationOptions.ExecuteSynchronously);
                }).ToArray();

                await Task.WhenAll(tasks);
                await Task.WhenAll(tasks.Select(t => t.Result));
            }

            Console.WriteLine($"> Created {toBeCreated} relationships");
            
            var stopWatch = new Stopwatch();
            
            var verifySession = DbHelper.Neo4J.OpenNeo4JAsyncSession();
            stopWatch.Start();
            var result = await verifySession.ReadAsync($"MATCH (a:{_nodeLabel})-[r:HAS_REL]->(b:{_nodeLabel}) WHERE a.n=1 AND b.n=2 RETURN count(r)");
            stopWatch.Stop();
            Console.WriteLine($"> Verify step: {result[0].As<int>()} relationships ({stopWatch.ElapsedMilliseconds} ms)");

            var minimum = long.MaxValue;

            for (var turn = 1; turn <= 5; turn++)
            {
                if (turn > 1)
                    await Task.Delay(3_000);
                
                stopWatch.Reset();
                stopWatch.Start();
                var searchSession = DbHelper.Neo4J.OpenNeo4JAsyncSession();
                await searchSession.ReadAsync($"MATCH (a:{_nodeLabel})-[r:HAS_REL]->(b:{_nodeLabel}) WHERE a.n=1 AND b.n=2 AND r.n = {numberOfRelationships + 1} RETURN count(r)");
                stopWatch.Stop();

                minimum = Math.Min(minimum, stopWatch.ElapsedMilliseconds);
                
                Console.WriteLine($">> Turn {turn}: {stopWatch.ElapsedMilliseconds}/{minimum} ms");
                
                await searchSession.CloseAsync();
            }

            _result.AddOrUpdate(numberOfRelationships, 
                _ => minimum,
                (x, y) => minimum);
        }

        public override async Task PrintResultAsync()
        {
            Console.WriteLine(GetResultContents());
            await Task.CompletedTask;
        }

        public override async Task SaveResultAsync()
        {
            await WriteResultAsync(GetResultContents());
        }

        private string GetResultContents()
        {
            var sb = new StringBuilder();
            sb.AppendLine("******************");
            sb.AppendLine($"{"No of rel",15}\t{"Time (ms)",20}");
            foreach (var (k, v) in _result.OrderBy(x => x.Key))
            {
                sb.AppendLine($"{k,15}\t{v,17} ms");
            }

            sb.AppendLine("******************");
            return sb.ToString();
        }
    }
    */
}