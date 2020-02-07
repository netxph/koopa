using System;

namespace Koopa.Cli
{
    public class ExportView
    {
        private readonly IMigrator _migrator;

        public ExportView(IMigrator migrator)
        {
            _migrator = migrator ?? throw new ArgumentNullException(nameof(migrator));
        }

        public string Table { get; set; }
        public string Destination { get; set; }

        public void Show()
        {
            Console.WriteLine($"Exporting Table [{Table}]...");
            _migrator.Migrate(Destination);
            Console.WriteLine($"Exporting DONE.");
        }
    }
}