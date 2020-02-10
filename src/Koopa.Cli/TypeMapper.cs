using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Koopa.Cli
{
    public class TypeMapper
    {

        private readonly Column _column;

        public TypeMapper(Column column)
        {
            _column = column ?? throw new ArgumentNullException(nameof(column));
        }

        public DataField Create()
        {
            return new DataField<string>(_column.Name);
        }

        public static implicit operator DataField(TypeMapper obj)
        {
            return obj.Create();
        }
    }
}
