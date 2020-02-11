using System;

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

        public Type GetConversionType()
        {
            return new TypeMapper(ColType); 
        }

        public object Get(object value)
        {
            return Convert.ChangeType(value, GetConversionType());
        }
    }
}
