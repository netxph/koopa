using System.Collections;
using System.Collections.Generic;

namespace Koopa.Cli
{
    public class ColSchema : IEnumerable<Column>
    {
        private readonly List<Column> _columns;

        public ColSchema()
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
}