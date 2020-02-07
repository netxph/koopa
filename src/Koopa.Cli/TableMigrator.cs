namespace Koopa.Cli
{
    public class TableMigrator
    {

        public TableMigrator(string connection, string table)
        {
        }

        public Schema GetSchema()
        {
            var schema = new Schema();
            schema.AddColumn("col1", "varchar");
            schema.AddColumn("col2", "int");

            return schema;
        }
    }
}