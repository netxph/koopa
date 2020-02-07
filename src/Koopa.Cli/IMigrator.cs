namespace Koopa.Cli
{
    public interface IMigrator
    {
        ColSchema GetSchema();
        void Migrate(string destination);
    }
}