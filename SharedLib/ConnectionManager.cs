using Neo4j.Driver;

namespace SharedLib
{
    public static class ConnectionManager
    {
        public static readonly IDriver Neo4JDriver =
            GraphDatabase.Driver(
                Neo4JHost,
                AuthTokens.Basic(Neo4JUser, Neo4JPass)
            );

        public const string Neo4JHost = "bolt://localhost:7687";
        public const string Neo4JUser = "neo4j";
        public const string Neo4JPass = "neo4j";

        public const string PostgresConnectionString = "Host=localhost;Username=postgres;Database=postgres";
    }
}