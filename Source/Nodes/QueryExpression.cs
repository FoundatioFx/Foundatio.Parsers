namespace Exceptionless.LuceneQueryParser.Nodes {
    public class QueryExpression : QueryNode {
        public FieldExpression Field { get; set; }
        public string TermMin { get; set; }
        public string TermMax { get; set; }
        public bool? Inclusive { get; set; }
        public string Term { get; set; }
        public double? Similarity { get; set; }
        public double? Boost { get; set; }
        public string Prefix { get; set; }
        public double? Proximity { get; set; }

        public QueryExpression CopyTo(QueryExpression target) {
            if (Field != null)
                target.Field = Field;

            if (TermMin != null)
                target.TermMin = TermMin;

            if (TermMax != null)
                target.TermMax = TermMax;

            if (Inclusive.HasValue)
                target.Inclusive = Inclusive;

            if (Term != null)
                target.Term = Term;

            if (Similarity.HasValue)
                target.Similarity = Similarity;

            if (Boost.HasValue)
                target.Boost = Boost;

            if (Prefix != null)
                target.Prefix = Prefix;

            if (Proximity.HasValue)
                target.Proximity = Proximity;

            return target;
        }
    }
}