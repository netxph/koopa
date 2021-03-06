﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Koopa.Cli
{
    public class SqlConnector : IConnector
    {
        private readonly IDbConnection _connection;

        public SqlConnector(string connectionString)
        {
            _connection = new SqlConnection(connectionString);
            _connection.Open();
        }

        public ColSchema ReadSchema(string table)
        {
            var split= table.Split(".");
            var tb = split[1];
            var sch = split[0];

            var command = _connection.CreateCommand();
            command.CommandText = $"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{sch}' AND TABLE_NAME = '{tb}'";
            command.CommandType = CommandType.Text;
            command.CommandTimeout = 0;

            var schema = new ColSchema();

            using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
            {
                while (reader.Read())
                {
                    var col = reader.GetString(reader.GetOrdinal("COLUMN_NAME"));
                    var colType = reader.GetString(reader.GetOrdinal("DATA_TYPE"));
                    schema.AddColumn(col, colType);
                }
            }

            return schema;
        }

        public IDataReader Read(string query)
        {
            var command = _connection.CreateCommand();
            command.CommandText = query;
            command.CommandType = CommandType.Text;

            return command.ExecuteReader();
        }

        public T Execute<T>(string query)
        {
            var command = _connection.CreateCommand();
            command.CommandText = query;
            command.CommandType = CommandType.Text;

            return (T)Convert.ChangeType(command.ExecuteScalar(), typeof(T));
        }

        public void Dispose()
        {
            _connection?.Dispose();
        }
    }
}
