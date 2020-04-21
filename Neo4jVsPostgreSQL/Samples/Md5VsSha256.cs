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
        // ReSharper disable once UnassignedField.Global
        [Params(1000, 10000)] public int N;
        private byte[] _data;
        private byte[] _data2;

        private readonly SHA256 _sha256 = SHA256.Create();
        private readonly MD5 _md5 = MD5.Create();

        [GlobalSetup]
        public void Setup()
        {
            _data = new byte[N];
            _data2 = new byte[N / 2];

            var rad = new Random(42);

            rad.NextBytes(_data);
            rad.NextBytes(_data2);
        }

        [Benchmark]
        public byte[] Sha256() => _sha256.ComputeHash(_data);

        [Benchmark]
        public byte[] Md5() => _md5.ComputeHash(_data);

        [Benchmark]
        public byte[] Md5_Half() => _md5.ComputeHash(_data2);
    }
}