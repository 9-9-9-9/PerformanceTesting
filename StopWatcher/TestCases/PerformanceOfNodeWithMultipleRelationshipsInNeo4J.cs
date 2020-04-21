using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SharedLib;

namespace StopWatcher.TestCases
{
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
            1_000_000, 
            100_000_000, 
            1_000_000_000
        };
        private const int BatchOfCreatingRelationships = 1_000;

        public override async Task RunAsync()
        {
            try
            {
                Console.WriteLine($"> Using node label :{_nodeLabel}");
                Console.WriteLine("> Preparing data");
                await PrepareDataAsync();
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

        private async Task PrepareDataAsync()
        {
            var asyncSession = DbHelper.Neo4JHelper.OpenNeo4JAsyncSession();
            
            Console.WriteLine(">> Remove all existing nodes of same label name");
            await asyncSession.ExecuteAsync($"MATCH (n1:{_nodeLabel})-[r:HAS_REL]->(n2:{_nodeLabel}) DELETE r");
            await asyncSession.ExecuteAsync($"MATCH (n:{_nodeLabel}) DELETE n");
            
            Console.WriteLine(">> Create 2 fresh nodes");
            await asyncSession.ExecuteAsync($"CREATE (a:{_nodeLabel}), (b:{_nodeLabel}) SET a.n = 1, b.n = 2");
            
            await asyncSession.CloseAsync();
        }

        protected override async Task DoWorkAsync()
        {
            int numberOfCreatedRelationship = 0;
            foreach (var numberOfRelationships in _testCasesNumberOfRelationships.OrderBy(n => n))
            {
                try
                {
                    Console.WriteLine($"> Before {nameof(DoTestWithInput)}({numberOfRelationships})");
                    await DoTestWithInput(numberOfRelationships, numberOfCreatedRelationship);
                    Console.WriteLine($"> After {nameof(DoTestWithInput)}({numberOfRelationships})");

                    numberOfCreatedRelationship += numberOfRelationships;
                    
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
                catch (Exception e)
                {
                    Console.WriteLine($"> (ERR) {nameof(DoTestWithInput)}({numberOfRelationships})");
                    throw;
                }
            }
        }

        private async Task DoTestWithInput(int numberOfRelationships, int numberOfExistingRelationships)
        {
            var toBeCreated = numberOfRelationships - numberOfExistingRelationships;
            Console.WriteLine($"> Going to create {toBeCreated} more relationship");

            var numberOfBatches = (int)Math.Ceiling((double)toBeCreated / BatchOfCreatingRelationships);
            Parallel.For(0L, numberOfBatches, new ParallelOptions { MaxDegreeOfParallelism = 3 },
                async b =>
                {
                    var from = b;
                    var to = b + BatchOfCreatingRelationships - 1;
                    if (to > numberOfRelationships)
                        to = numberOfRelationships;
                    
                    var writeSession = DbHelper.Neo4JHelper.OpenNeo4JAsyncSession();
                    var sb = new StringBuilder();

                    sb.Append($"MATCH (a:{_nodeLabel}), (b:{_nodeLabel}) WHERE a.n = 1 AND b.n = 2 ");
                    for (var i = from; i <= to; i++)
                        sb.Append($"CREATE (a)-[r{i}:HAS_REL]->(b) SET r{i}.n={i} ");

                    await writeSession.ExecuteAsync(sb.ToString());
                    await writeSession.CloseAsync();
                });

            Console.WriteLine($"> Created {toBeCreated} relationships");

            long minimum = long.MaxValue;

            for (var turn = 1; turn <= 5; turn++)
            {
                if (turn > 1)
                    await Task.Delay(10000);
                
                
                var stopWatch = new Stopwatch();
                stopWatch.Start();
                var searchSession = DbHelper.Neo4JHelper.OpenNeo4JAsyncSession();
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
}