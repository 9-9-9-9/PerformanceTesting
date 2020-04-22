using System.Threading.Tasks;
using SharedLib;
using StopWatcher.TestCases;

namespace StopWatcher
{
    internal static class Program
    {
        internal static async Task Main()
        {
            await RunTestAsync<QueryOfMatchMultipleRel>();
            await ConnectionManager.Neo4JDriver.CloseAsync();
        }

        private static async Task RunTestAsync<TTestCase>() where TTestCase : ITestCase, new()
        {
            await new TTestCase().RunAsync();   
        }
    }
}