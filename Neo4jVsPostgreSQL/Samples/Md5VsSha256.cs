using System;
using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace Neo4jVsPostgreSQL.Samples
{
    [SimpleJob(RuntimeMoniker.NetCoreApp31)]
    [RPlotExporter]
    public class Md5VsSha256
    {
        [Params(1000, 10000)] public int N;
        private byte[] data;
        private byte[] data2;

        private readonly SHA256 sha256 = SHA256.Create();
        private readonly MD5 md5 = MD5.Create();

        [GlobalSetup]
        public void Setup()
        {
            data = new byte[N];
            data2 = new byte[N / 2];

            var rad = new Random(42);

            rad.NextBytes(data);
            rad.NextBytes(data2);
        }

        [Benchmark]
        public byte[] Sha256() => sha256.ComputeHash(data);

        [Benchmark]
        public byte[] Md5() => md5.ComputeHash(data);

        [Benchmark]
        public byte[] Md5_Half() => md5.ComputeHash(data2);
    }
}