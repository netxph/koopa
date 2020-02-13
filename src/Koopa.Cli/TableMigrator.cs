using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Microsoft.VisualBasic;
using Parquet;
using Parquet.Data;
using Parquet.Data.Rows;

namespace Koopa.Cli
{
    public class TableMigrator : IMigrator, IDisposable
    {

        const int DEFAULT_PAGE_SIZE = 5000;

        private readonly IConnector _connector;

        public string Table { get; }
        public int PageSize { get; }
        public string Key { get; }
        public string PartitionKey { get; }
        public int NumberOfPartitions { get; }

        public TableMigrator(string table, string key, IConnector connector)
            : this(table, key, DEFAULT_PAGE_SIZE, string.Empty, connector)
        {
        }

        public TableMigrator(string table, IConnector connector)
            : this(table, "?auto", DEFAULT_PAGE_SIZE, string.Empty, connector)
        {
        }

        public TableMigrator(string table, string key, int pageSize, string partition, IConnector connector)
        {
            _connector = connector ?? throw new ArgumentNullException(nameof(connector));

            if (string.IsNullOrEmpty(table))
            {
                throw new ArgumentNullException(nameof(table));
            }

            Table = table;

            if (string.IsNullOrEmpty(key))
            {
                throw new ArgumentNullException(nameof(key));
            }

            Key = key;

            if (pageSize < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(pageSize));
            }

            PageSize = pageSize;

            PartitionKey = string.Empty;
            NumberOfPartitions = 1;

            if (!string.IsNullOrEmpty(partition))
            {
                var partRaw = partition.Split(":");
                PartitionKey = partRaw[0];
                NumberOfPartitions = int.Parse(partRaw[1]);
            }
        }

        public ColSchema GetSchema()
        {
            return _connector.ReadSchema(Table);
        }

        public void Migrate(string destination)
        {
            var minKey = _connector.Execute<long>($"SELECT MIN({PartitionKey}) FROM {Table}");
            var maxKey = _connector.Execute<long>($"SELECT MAX({PartitionKey}) FROM {Table}");

            Console.WriteLine($"INFO: MINKEY({minKey})");
            Console.WriteLine($"INFO: MAXKEY({maxKey})");
            Console.WriteLine($"INFO: Partitions({NumberOfPartitions})");

            var offsetSize =
                Convert.ToInt32(
                    Math.Ceiling(
                        Convert.ToDecimal(
                            (maxKey - minKey - 1) / NumberOfPartitions)));

            Console.WriteLine($"INFO: Offset Size {offsetSize}");

            var schema = GetSchema().ToList();
            var fields = new List<Field>();

            foreach (var col in schema)
            {
                fields.Add(new DataField(col.Name, col.GetParquetType(), true, false));
            }

            for (int part = 0; part < NumberOfPartitions; part++)
            {

                int page = 1;
                bool endPage = false;
                var offset = (minKey - 1) + (offsetSize * part);

                var file = $"{destination}.parquet";

                if (NumberOfPartitions > 1)
                {
                    file = $"{destination}-part{part}.parquet";
                }

                using (var ms = new FileStream(file, FileMode.Create))
                {
                    while (!endPage)
                    {
                        var table = new Table(
                            new Schema(fields.ToArray()));

                        try
                        {
                            using (var reader = _connector.Read(
                                new OptimizedQueryMaker(
                                    Table,
                                    page,
                                    PageSize,
                                    PartitionKey,
                                    offset,
                                    offset + offsetSize,
                                    Key.Split(",", StringSplitOptions.RemoveEmptyEntries))))
                            {
                                while (reader.Read())
                                {
                                    var values = new List<object>();

                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        values.Add(schema[i].Get(reader.GetValue(i)));
                                    }

                                    table.Add(new Row(values.ToArray()));
                                }

                            }
                        }
                        catch (SqlException sqex)
                        {
                            Console.WriteLine($"ERROR: {sqex.Message}");
                            continue;
                        }

                        if (table.Count > 0)
                        {
                            bool append = page > 1;

                            ms.Position = 0;

                            using (var writer = new ParquetWriter(table.Schema, ms, append: append))
                            {
                                writer.Write(table);
                            }
                        }

                        if (table.Count < PageSize)
                        {
                            endPage = true;
                        }
                        else
                        {
                            page++;
                        }

                        ms.Flush(true);
                    }

                }
            }

        }

        public void Dispose()
        {
            _connector?.Dispose();
        }
    }
}
