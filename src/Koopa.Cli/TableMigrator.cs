using System.Data;
using System;
using System.Data.Common;

namespace Koopa.Cli
{
    public class TableMigrator : IMigrator
    {
        private readonly IDbConnection _connection;

        public string Table { get; }
        
        public TableMigrator(string table, IDbConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _connection.Open();

            if (string.IsNullOrEmpty(table))
            {
                throw new ArgumentNullException(nameof(table));
            }

            Table = table;
        }

        public Schema GetSchema()
        {
            //HACK: .NET Core did not fixed about getting the table schema

            var split_tb = Table.Split(".");
            var tb = split_tb[1];
            var sch = split_tb[0];

            var command = _connection.CreateCommand();
            command.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tb}'";
            command.CommandType = CommandType.Text;
            var reader = command.ExecuteReader();

            var schema = new Schema();

            while (reader.Read())
            { 
                var col = reader.GetString(reader.GetOrdinal("COLUMN_NAME"));
                var colType = reader.GetString(reader.GetOrdinal("DATA_TYPE"));
                schema.AddColumn(col, colType);
            }

            return schema;
        }
    }

    public interface IMigrator
    {
        Schema GetSchema();
    }
}
