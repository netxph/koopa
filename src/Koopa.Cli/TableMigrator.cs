using System.Data;
using System;
using System.Data.Common;

namespace Koopa.Cli
{
    public class TableMigrator : IMigrator
    {
        private readonly IConnector _connector;

        public string Table { get; }
        
        public TableMigrator(string table, IConnector connector)
        {
            _connector = connector ?? throw new ArgumentNullException(nameof(connector));

            if (string.IsNullOrEmpty(table))
            {
                throw new ArgumentNullException(nameof(table));
            }

            Table = table;
        }

        public Schema GetSchema()
        {
            return _connector.ReadSchema(Table);
        }
    }
}
