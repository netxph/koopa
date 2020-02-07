using System;
using System.Collections;
using System.Collections.Generic;
using McMaster.Extensions.CommandLineUtils;

namespace Koopa.Cli
{
    public class Program
    {
        public static int Main(string[] args)
        {
            var app = new CommandLineApplication();

            app.HelpOption(true);

            app.Command("schema", schemaCmd =>
            {
                var connection = schemaCmd.Option("-c|--connection <connection_string>", "The connection string.",
                    CommandOptionType.SingleValue);
                var table = schemaCmd.Option("-t|--table <table_name>", "The table.", CommandOptionType.SingleValue);

                schemaCmd.OnExecute(() =>
                {
                    var schemaView = new SchemaView();

                    schemaView.Connection = connection.Value();
                    schemaView.Table = table.Value();

                    schemaView.Show();
                });
            });

            app.OnExecute(() =>
            {
                Console.WriteLine("ERROR: Please specify a command.");
                app.ShowHelp();
            });

            return app.Execute(args);
        }

    }

    public class SchemaView
    {

        public string Connection { get; set; }
        public string Table { get; set; }

        public void Show()
        {
            var migrator = new TableMigrator(Connection, Table);
            foreach(var schema in migrator.GetSchema())
            {
                Console.WriteLine($"{schema.Name} ({schema.ColType})");
            }
        }
    }

    public class TableMigrator
    {

        public TableMigrator(string connection, string table)
        {
        }

        public Schema GetSchema()
        {
            var schema = new Schema();
            schema.AddColumn("col1", "varchar");
            schema.AddColumn("col2", "int");

            return schema;
        }
    }

    public class Schema : IEnumerable<Column>
    {
        private readonly List<Column> _columns;

        public Schema()
        {
            _columns = new List<Column>();
        }

        public void AddColumn(string name, string type)
        {
            _columns.Add(new Column(name, type));
        }

        public IEnumerator<Column> GetEnumerator()
        {
            return _columns.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

    }

    public class Column
    {
        public Column(string name, string type)
        {
            if(string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Name = name;

            if(string.IsNullOrEmpty(type))
            {
                throw new ArgumentNullException(nameof(type));
            }

            ColType = type;
        }

        public string Name { get; }
        public string ColType { get; }
    }
}
