using System;

namespace SharedLib
{
    public static class RandomUtils
    {
        public static string RandomLabel => $"L{Guid.NewGuid().ToString().Substring(0, 4)}";
    }
}