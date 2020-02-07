using System;

namespace Koopa.Cli
{
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
}