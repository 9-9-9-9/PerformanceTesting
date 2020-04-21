using BenchmarkDotNet.Running;
using Neo4jVsPostgreSQL.BenchMarks;

namespace Neo4jVsPostgreSQL
{
    internal static class Program
    {
        internal static void Main(string[] args)
        {
            // Example
            // BenchmarkRunner.Run<Samples.Md5VsSha256>();
            
            // Bench insert
            BenchmarkRunner.Run<InsertOperation>();
            // Bench Sum, Count, Order By
            BenchmarkRunner.Run<CompareOperationsBetweenDBs>();
        }
    }
}