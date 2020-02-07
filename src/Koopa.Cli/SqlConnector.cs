﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Koopa.Cli
{
    public class SqlConnector : IConnector, IDisposable
    {
        private readonly IDbConnection _connection;

        public SqlConnector(string connectionString)
        {
            _connection = new SqlConnection(connectionString);
            _connection.Open();
        }

        public Schema ReadSchema(string table)
        {
            var split= table.Split(".");
            var tb = split[1];
            var sch = split[0];

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

        public void Dispose()
        {
            _connection?.Dispose();
        }
    }
}
