using System;

namespace UnitTests
{
    public static class TestUtils
    {
        public static string RandomLabel(int len = 5) => $"L{Guid.NewGuid().ToString().Replace("-", "").Substring(0, len)}";

        public static void Print(object value)
        {
            Console.WriteLine(value);
        }
    }
}