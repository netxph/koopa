using System.Diagnostics;
using System.Data;
using System;

namespace Koopa.Cli
{

    public class TraceConnector : IConnector
    {

        private readonly IConnector _connector;

        public TraceConnector(IConnector connector)
        {
            _connector = connector ?? throw new ArgumentNullException(nameof(connector));
        }

        public ColSchema ReadSchema(string table)
        {
            return _connector.ReadSchema(table);
        }

        public IDataReader Read(string query)
        {
            Console.WriteLine($"INFO: [{query}]");
            
            var watch = Stopwatch.StartNew();
            var reader =  _connector.Read(query);
            watch.Stop();

            Console.WriteLine($"INFO: Elapsed [{watch.ElapsedMilliseconds}ms]");

            return reader;
        }

        public T Execute<T>(string query)
        {
            return _connector.Execute<T>(query);
        }

        public void Dispose()
        {
            _connector.Dispose();
        }
    }

}
