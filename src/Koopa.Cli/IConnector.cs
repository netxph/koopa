using System;
using System.Data;

namespace Koopa.Cli
{
    public interface IConnector : IDisposable
    {
        ColSchema ReadSchema(string table);
        IDataReader Read(string query);
        T Execute<T>(string query);
    }
}
