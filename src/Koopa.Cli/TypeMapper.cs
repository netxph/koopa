﻿using System;
using System.Collections.Generic;
using System.Text;
using Parquet.Data;

namespace Koopa.Cli
{
    public class TypeMapper
    {

        private readonly string _dbType;

        public TypeMapper(string dbType)
        {
            if(string.IsNullOrEmpty(dbType))
            {
                throw new ArgumentNullException(nameof(dbType));
            }

            _dbType = dbType;
        }

        public Type Create()
        {
            switch(_dbType)
            {
                case "bigint":
                    return typeof(long);
                case "smallint":
                    return typeof(int);
                case "money":
                case "decimal":
                    return typeof(decimal);
                default:
                    return typeof(string);
            }

        }

        public static implicit operator Type(TypeMapper obj)
        {
            return obj.Create();
        }
    }
}
