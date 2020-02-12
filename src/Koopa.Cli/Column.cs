using System;
using System.Text;
using Parquet.Data;
using Parquet.Thrift;
using Type = System.Type;

namespace Koopa.Cli
{
    public class Column
    {
        public Column(string name, string type)
        {
            if(string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            Name = name;

            if(string.IsNullOrEmpty(type))
            {
                throw new ArgumentNullException(nameof(type));
            }

            ColType = type;
        }

        public string Name { get; }
        public string ColType { get; }

        public Type GetClrType()
        {
            return new TypeMapper(ColType); 
        }

        public DataType GetParquetType()
        {
            return new TypeMapper(ColType).GetDataType();
        }

        public object Get(object value)
        {
            if (value != null && value != DBNull.Value)
            {
                if (value is byte[])
                {
                    return Convert.ToBase64String((byte[])value);
                }

                return Convert.ChangeType(value, GetClrType());
            }

            return null;
        }
    }
}
