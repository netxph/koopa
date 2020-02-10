using System;
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
                    var schemaView = new SchemaView(
                        new TableMigrator(table.Value(),
                            new SqlConnector(connection.Value())));

                    schemaView.Table = table.Value();

                    schemaView.Show();
                });
            });

            app.Command("export", exportCmd =>
            {
                var connection = exportCmd.Option("-c|--connection <connection_string>", "The connection string.",
                    CommandOptionType.SingleValue);
                var table = exportCmd.Option("-t|--table <table_name>", "The table.", CommandOptionType.SingleValue);
                var key = exportCmd.Option("-k|--key <key_column>", "The key column.", CommandOptionType.SingleValue);
                var destination = exportCmd.Option("-d|--destination <destination>", "The target destination.", CommandOptionType.SingleValue);

                exportCmd.OnExecute(() =>
                {
                    var exportView = new ExportView(
                        new TraceMigrator(
                        new TableMigrator(table.Value(), key.Value(),
                            new TraceConnector(
                            new SqlConnector(connection.Value())))));

                    exportView.Table = table.Value();
                    exportView.Destination = destination.Value();

                    exportView.Show();
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
}
