namespace Exceptionless.LuceneQueryParser.Nodes {
    public class FieldExpression : QueryNode {
        public string Field { get; set; }
        public string Prefix { get; set; }
    }
}
