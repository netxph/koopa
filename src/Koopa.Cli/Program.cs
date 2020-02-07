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
}
