using System;
using System.Collections.Generic;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries.Nodes;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParserConfiguration {
        private readonly SortedSet<QueryVisitorWithPriority> _visitors = new SortedSet<QueryVisitorWithPriority>(new QueryVisitorWithPriority.PriorityComparer());

        public string DefaultField { get; private set; }
        public Operator DefaultFilterOperator { get; private set; } = Operator.And;
        public Operator DefaultQueryOperator { get; private set; } = Operator.Or;
        public IList<IQueryNodeVisitorWithResult<IQueryNode>> Visitors => _visitors.Cast<IQueryNodeVisitorWithResult<IQueryNode>>().ToList();
        private Func<string, bool> AnalyzedFieldFunc { get; set; }

        public bool IsFieldAnalyzed(string field) {
            if (String.IsNullOrEmpty(field))
                return true;

            if (AnalyzedFieldFunc == null)
                return false;

            return AnalyzedFieldFunc(field);
        }

        public ElasticQueryParserConfiguration SetDefaultField(string field) {
            DefaultField = field;
            return this;
        }

        public ElasticQueryParserConfiguration SetDefaultFilterOperator(Operator op) {
            DefaultFilterOperator = op;
            return this;
        }

        public ElasticQueryParserConfiguration SetDefaultQueryOperator(Operator op) {
            DefaultQueryOperator = op;
            return this;
        }

        public ElasticQueryParserConfiguration SetAnalyzedFieldFunc(Func<string, bool> analyzedFieldFunc) {
            AnalyzedFieldFunc = analyzedFieldFunc;
            return this;
        }

        public ElasticQueryParserConfiguration AddVisitor(IChainableQueryVisitor visitor, int priority = 0) {
            _visitors.Add(new QueryVisitorWithPriority { Visitor = visitor, Priority = priority });
            return this;
        }

        public ElasticQueryParserConfiguration UseAliases(AliasMap aliasMap, int priority = 0) {
            return AddVisitor(new AliasedQueryVisitor(aliasMap), priority);
        }

        public ElasticQueryParserConfiguration UseGeo(IEnumerable<string> geoFields, Func<string, string> resolveGeoLocation, int priority = 10) {
            var fields = new HashSet<string>(geoFields, StringComparer.OrdinalIgnoreCase);
            return UseGeo(f => fields.Contains(f), resolveGeoLocation, priority);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, string> resolveGeoLocation, params string[] geoFields) {
            return UseGeo(geoFields.ToList(), resolveGeoLocation);
        }

        public ElasticQueryParserConfiguration UseGeo(int priority, Func<string, string> resolveGeoLocation, params string[] geoFields) {
            return UseGeo(geoFields.ToList(), resolveGeoLocation, priority);
        }

        public ElasticQueryParserConfiguration UseGeo(Func<string, bool> isGeoField, Func<string, string> resolveGeoLocation, int priority = 10) {
            return AddVisitor(new GeoVisitor(isGeoField, resolveGeoLocation), priority);
        }

        public ElasticQueryParserConfiguration UseNested(Func<string, bool> isNestedField, int priority = 1000) {
            return AddVisitor(new NestedVisitor(isNestedField), priority);
        }
    }
}