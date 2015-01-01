namespace Exceptionless.LuceneQueryParser.Nodes {
    public class QueryGroup : QueryNode {
        public QueryNode Left { get; set; }
        public QueryNode Right { get; set; }
        public string Operator { get; set; }
        public FieldExpression Field { get; set; }
    }
}