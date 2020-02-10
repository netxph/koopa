using System.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net;
using Parquet;
using Parquet.Data;
using Parquet.Data.Rows;

namespace Koopa.Cli
{
    public class TableMigrator : IMigrator, IDisposable
    {
        private readonly IConnector _connector;

        public string Table { get; }
        public int PageSize { get; }

        public TableMigrator(string table, IConnector connector)
            : this(table, 5000, connector)
        {
        }
        
        public TableMigrator(string table, int pageSize, IConnector connector)
        {
            _connector = connector ?? throw new ArgumentNullException(nameof(connector));

            if (string.IsNullOrEmpty(table))
            {
                throw new ArgumentNullException(nameof(table));
            }

            Table = table;

            if(pageSize < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(pageSize));
            }

            PageSize = pageSize;
        }

        public ColSchema GetSchema()
        {
            return _connector.ReadSchema(Table);
        }

        public void Migrate(string destination)
        {
            var schema = GetSchema();
            var fields = new List<Field>();

            foreach (var col in schema)
            {
                fields.Add(new DataField<string>(col.Name));
            }

            var table = new Table(
                new Schema(fields.ToArray()));

            using (var reader = _connector.Read($"SELECT * FROM {Table}"))
            {
                while (reader.Read())
                {
                    var values = new List<object>();

                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        values.Add(reader.GetValue(i).ToString());
                    }

                    table.Add(new Row(values.ToArray()));
                }
            }

            using (var fileStream = File.OpenWrite(destination))
            {
                using (var writer = new ParquetWriter(table.Schema, fileStream))
                {
                    writer.Write(table);
                }
            }
        }

        public void Dispose()
        {
            _connector?.Dispose();
        }
    }
}
