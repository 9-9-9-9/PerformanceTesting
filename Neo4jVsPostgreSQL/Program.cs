using System.Threading.Tasks;
using BenchmarkDotNet.Running;
using Neo4jVsPostgreSQL.BenchMarks;

namespace Neo4jVsPostgreSQL
{
    internal static class Program
    {
        internal static async Task Main(string[] args)
        {
            //
            BenchmarkRunner.Run<InsertOperation>();
            //
            BenchmarkRunner.Run<CompareOperationsBetweenDBs>();
        }
    }
}