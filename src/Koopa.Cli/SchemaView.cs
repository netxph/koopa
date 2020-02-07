using System;
using System.Data;

namespace Koopa.Cli
{
    public class SchemaView
    {
        private readonly IMigrator _migrator;
        
        public SchemaView(IMigrator migrator)
        {
            _migrator = migrator ?? throw new ArgumentNullException(nameof(migrator));
        }

        public string Table { get; set; }

        public void Show()
        {
            foreach(var schema in _migrator.GetSchema())
            {
                Console.WriteLine($"{schema.Name.PadRight(40)} ({schema.ColType})");
            }
        }
    }
}
