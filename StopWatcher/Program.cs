﻿using System.Threading.Tasks;
using StopWatcher.TestCases;

namespace StopWatcher
{
    internal static class Program
    {
        internal static async Task Main()
        {
            await RunTestAsync<PerformanceOfNodeWithMultipleRelationshipsInNeo4J>();
        }

        private static async Task RunTestAsync<TTestCase>() where TTestCase : ITestCase, new()
        {
            await new TTestCase().RunAsync();   
        }
    }
}