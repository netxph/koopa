using System;
using System.Diagnostics;

namespace Koopa.Cli
{

    public class TraceMigrator : IMigrator
    {

        private readonly IMigrator _migrator;

        public TraceMigrator(IMigrator migrator)
        {
            _migrator = migrator ?? throw new ArgumentNullException(nameof(migrator));
        }


        public ColSchema GetSchema()
        {
            return _migrator.GetSchema();
        }

        public void Migrate(string destination)
        {
            var watch = Stopwatch.StartNew();

            _migrator.Migrate(destination);

            watch.Stop();
            Console.WriteLine($"INFO: Migration DONE. [{watch.ElapsedMilliseconds}ms]");

        }

    }

}
