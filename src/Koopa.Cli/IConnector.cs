namespace Koopa.Cli
{
    public interface IConnector
    {
        Schema ReadSchema(string table);
    }
}