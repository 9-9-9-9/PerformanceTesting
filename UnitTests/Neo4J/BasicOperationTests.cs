using System.Threading.Tasks;
using NUnit.Framework;
using SharedLib;
using static UnitTests.TestUtils;

namespace UnitTests.Neo4J
{
    public class BasicOperationTests
    {
        [Test]
        public async Task Create()
        {
            var label = RandomLabel();

            using (var conn = DbHelper.Neo4J.Connection)
            {
                await conn.WriteAsync($"CREATE (n:{label}) SET n.no = 1");
                
                Print("Read using same connection");
                Assert.AreEqual(1, await Count(conn));
            }
            
            using (var conn = DbHelper.Neo4J.Connection)
            {
                Print("Read using different connection");
                Assert.AreEqual(1, await Count(conn));
            }

            async Task<int> Count(DbHelper.Neo4J.Neo4JConnection conn)
            {
                return await conn.ReadAsync<int>($"MATCH (n:{label}) WHERE n.no = 1 RETURN COUNT(*)");
            }
        }
        
        [Test]
        public async Task Merge()
        {
            var label = RandomLabel();

            using (var conn = DbHelper.Neo4J.Connection)
            {
                await conn.WriteAsync($"CREATE (n:{label}) SET n.no = 1");
                Assert.AreEqual(1, await Count(conn));
            }

            using (var conn = DbHelper.Neo4J.Connection)
            {
                await conn.WriteAsync($"MERGE (n:{label} {{no: 1}})");
                await conn.WriteAsync($"MERGE (n:{label} {{no: 2}})");
                
                Print("Read using same connection");
                Assert.AreEqual(2, await Count(conn));
            }
            
            using (var conn = DbHelper.Neo4J.Connection)
            {
                Print("Read using different connection");
                Assert.AreEqual(2, await Count(conn));
            }

            async Task<int> Count(DbHelper.Neo4J.Neo4JConnection conn)
            {
                return await conn.ReadAsync<int>($"MATCH (n:{label}) RETURN COUNT(*)");
            }
        }
    }
}