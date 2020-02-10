using System;
using System.Linq;

namespace Koopa.Cli
{

    public class OptimizedQueryMaker : QueryMaker
    {
        
        public OptimizedQueryMaker(string table, int page, int size, params string[] keys) 
            : base(table, page, size, keys)
        {
        }

        public override string Build()
        {
            var query =
                $"; WITH pg AS " +
                $"(" +
                $"SELECT {string.Join(',', Keys)} " +
                $"FROM {Table} " +
                $"ORDER BY {string.Join(',', Keys)} " +
                $"OFFSET {Size * (Page - 1)} ROWS " +
                $"FETCH NEXT {Size} ROWS ONLY " +
                $") " +
                $"SELECT * " +
                $"FROM {Table} AS t " +
                $"WHERE EXISTS(SELECT 1 FROM pg WHERE {makeWhere(Keys)}) " +
                $"ORDER BY t.TreatmentID OPTION(RECOMPILE);";

            return query;
        }

        private string makeWhere(string[] keys)
        {
            var tkeys = keys.Select(k => $"pg.{k} = t.{k}").ToArray();

            return string.Join(" AND ", tkeys);
        }

        public static implicit operator string(OptimizedQueryMaker obj)
        {
            return obj.Build();
        }

    }

    public class QueryMaker
    {
        public string Table { get; }
        public int Page { get; }
        public int Size { get; }
        public string[] Keys { get; }

        public QueryMaker(string table, int page, int size, params string[] keys)
        {
            if (string.IsNullOrEmpty(table))
            {
                throw new ArgumentNullException(nameof(table));
            }

            Table = table;

            if (page <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(page));
            }

            Page = page;

            if (size <= -0)
            {
                throw new ArgumentOutOfRangeException(nameof(size));
            }

            Size = size;

            if (keys.Length == 0)
            {
                throw new ArgumentNullException(nameof(keys));
            }

            Keys = keys;
        }

        public virtual string Build()
        {
            var query = 
                $"SELECT * FROM {Table} " +
                $"ORDER BY {string.Join(',', Keys)} " + 
                    $"OFFSET {Size * (Page - 1)} ROWS " + 
                    $"FETCH NEXT {Size} ROWS ONLY OPTION (RECOMPILE)";

            return query;
        }

        public static implicit operator string(QueryMaker obj)
        {
            return obj.Build();
        }
    }
}
