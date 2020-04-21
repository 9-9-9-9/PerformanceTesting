using System.Data;
using System.Threading.Tasks;
using Neo4j.Driver;
using Npgsql;

namespace Neo4jVsPostgreSQL
{
    public static class ConnectionManager
    {
        public static readonly IDriver Neo4JDriver =
            GraphDatabase.Driver("bolt://localhost:7687", AuthTokens.Basic("neo4j", "neo4j"));

        public static readonly string PostgresConnectionString = "Host=localhost;Username=postgres;Database=postgres";
    }
}