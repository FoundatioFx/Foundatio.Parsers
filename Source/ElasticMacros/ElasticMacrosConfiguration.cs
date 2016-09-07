using System;
using System.Collections.Generic;
using System.Linq;
using ElasticMacros.FilterMacros;
using ElasticMacros.QueryMacros;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Nest;

namespace ElasticMacros {
    public class ElasticMacrosConfiguration {
        private readonly SortedSet<ElasticFilterMacroWithPriority> _filterMacros = new SortedSet<ElasticFilterMacroWithPriority>(new ElasticFilterMacroWithPriority.PriorityComparer());
        private readonly SortedSet<ElasticQueryMacroWithPriority> _queryMacros = new SortedSet<ElasticQueryMacroWithPriority>(new ElasticQueryMacroWithPriority.PriorityComparer());
        private readonly SortedSet<ElasticVisitorWithPriority> _visitors = new SortedSet<ElasticVisitorWithPriority>(new ElasticVisitorWithPriority.PriorityComparer());

        public string DefaultField { get; private set; }
        public Operator DefaultFilterOperator { get; private set; } = Operator.And;
        public Operator DefaultQueryOperator { get; private set; } = Operator.Or;
        public IList<IElasticFilterMacro> FilterMacros => _filterMacros.Cast<IElasticFilterMacro>().ToList();
        public IList<IElasticQueryMacro> QueryMacros => _queryMacros.Cast<IElasticQueryMacro>().ToList();
        public IList<IQueryNodeVisitorWithResult<IQueryNode>> Visitors => _visitors.Cast<IQueryNodeVisitorWithResult<IQueryNode>>().ToList();
        private Func<string, bool> NestedFieldFunc { get; set; }
        private Func<string, bool> AnalyzedFieldFunc { get; set; }

        public bool IsFieldNested(string field) {
            if (String.IsNullOrEmpty(field))
                return false;

            if (NestedFieldFunc == null)
                return false;

            return NestedFieldFunc(field);
        }

        public bool IsFieldAnalyzed(string field) {
            if (String.IsNullOrEmpty(field))
                return true;

            if (AnalyzedFieldFunc == null)
                return false;

            return AnalyzedFieldFunc(field);
        }

        public ElasticMacrosConfiguration SetDefaultField(string field) {
            DefaultField = field;
            return this;
        }

        public ElasticMacrosConfiguration SetDefaultFilterOperator(Operator op) {
            DefaultFilterOperator = op;
            return this;
        }

        public ElasticMacrosConfiguration SetDefaultQueryOperator(Operator op) {
            DefaultQueryOperator = op;
            return this;
        }

        public ElasticMacrosConfiguration SetNestedFieldFunc(Func<string, bool> nestedFieldFunc) {
            NestedFieldFunc = nestedFieldFunc;
            return this;
        }

        public ElasticMacrosConfiguration SetAnalyzedFieldFunc(Func<string, bool> analyzedFieldFunc) {
            AnalyzedFieldFunc = analyzedFieldFunc;
            return this;
        }

        public ElasticMacrosConfiguration AddFilterMacro(IElasticFilterMacro filterMacro, int priority = 0) {
            _filterMacros.Add(new ElasticFilterMacroWithPriority { FilterMacro = filterMacro, Priority = priority });
            return this;
        }

        public ElasticMacrosConfiguration AddQueryMacro(IElasticQueryMacro queryMacro, int priority = 0) {
            _queryMacros.Add(new ElasticQueryMacroWithPriority { QueryMacro = queryMacro, Priority = priority });
            return this;
        }

        public ElasticMacrosConfiguration AddVisitor(IQueryNodeVisitorWithResult<IQueryNode> visitor, int priority = 0) {
            _visitors.Add(new ElasticVisitorWithPriority { Visitor = visitor, Priority = priority });
            return this;
        }

        public ElasticMacrosConfiguration UseAliases(AliasMap aliasMap, int priority = 0) {
            return AddVisitor(new AliasedQueryVisitor(aliasMap), priority);
        }

        public ElasticMacrosConfiguration UseGeo(IEnumerable<string> geoFields, Func<string, string> resolveGeoLocation, int priority = 0) {
            var fields = new HashSet<string>(geoFields, StringComparer.OrdinalIgnoreCase);
            return UseGeo(f => fields.Contains(f), resolveGeoLocation, priority);
        }

        public ElasticMacrosConfiguration UseGeo(Func<string, string> resolveGeoLocation, params string[] geoFields) {
            return UseGeo(geoFields.ToList(), resolveGeoLocation);
        }

        public ElasticMacrosConfiguration UseGeo(int priority, Func<string, string> resolveGeoLocation, params string[] geoFields) {
            return UseGeo(geoFields.ToList(), resolveGeoLocation, priority);
        }

        public ElasticMacrosConfiguration UseGeo(Func<string, bool> isGeoField, Func<string, string> resolveGeoLocation, int priority = 0) {
            AddQueryMacro(new GeoQueryMacro(isGeoField, resolveGeoLocation), priority);
            return AddFilterMacro(new GeoFilterMacro(isGeoField, resolveGeoLocation), priority);
        }
    }

    internal class ElasticFilterMacroWithPriority : IElasticFilterMacro {
        public int Priority { get; set; }
        public IElasticFilterMacro FilterMacro { get; set; }

        public void Expand(GroupNode node, ElasticFilterMacroContext context) {
            FilterMacro.Expand(node, context);
        }

        public void Expand(TermNode node, ElasticFilterMacroContext context) {
            FilterMacro.Expand(node, context);
        }

        public void Expand(TermRangeNode node, ElasticFilterMacroContext context) {
            FilterMacro.Expand(node, context);
        }

        public void Expand(MissingNode node, ElasticFilterMacroContext context) {
            FilterMacro.Expand(node, context);
        }

        public void Expand(ExistsNode node, ElasticFilterMacroContext context) { 
            FilterMacro.Expand(node, context);
        }

        internal class PriorityComparer : IComparer<ElasticFilterMacroWithPriority> {
            public int Compare(ElasticFilterMacroWithPriority x, ElasticFilterMacroWithPriority y) {
                return x.Priority.CompareTo(y.Priority);
            }
        }
    }

    internal class ElasticQueryMacroWithPriority : IElasticQueryMacro {
        public int Priority { get; set; }
        public IElasticQueryMacro QueryMacro { get; set; }

        public void Expand(GroupNode node, ElasticQueryMacroContext context) {
            QueryMacro.Expand(node, context);
        }

        public void Expand(TermNode node, ElasticQueryMacroContext context) {
            QueryMacro.Expand(node, context);
        }

        public void Expand(TermRangeNode node, ElasticQueryMacroContext context) {
            QueryMacro.Expand(node, context);
        }

        public void Expand(MissingNode node, ElasticQueryMacroContext context) {
            QueryMacro.Expand(node, context);
        }

        public void Expand(ExistsNode node, ElasticQueryMacroContext context) {
            QueryMacro.Expand(node, context);
        }

        internal class PriorityComparer : IComparer<ElasticQueryMacroWithPriority> {
            public int Compare(ElasticQueryMacroWithPriority x, ElasticQueryMacroWithPriority y) {
                return x.Priority.CompareTo(y.Priority);
            }
        }
    }

    internal class ElasticVisitorWithPriority : IQueryNodeVisitorWithResult<IQueryNode> {
        public int Priority { get; set; }
        public IQueryNodeVisitorWithResult<IQueryNode> Visitor { get; set; }

        public IQueryNode Accept(IQueryNode node) {
            return Visitor.Accept(node);
        }

        public void Visit(GroupNode node) {
            Visitor.Visit(node);
        }

        public void Visit(TermNode node) {
            Visitor.Visit(node);
        }

        public void Visit(TermRangeNode node) {
            Visitor.Visit(node);
        }

        public void Visit(ExistsNode node) {
            Visitor.Visit(node);
        }

        public void Visit(MissingNode node) {
            Visitor.Visit(node);
        }

        internal class PriorityComparer : IComparer<ElasticVisitorWithPriority> {
            public int Compare(ElasticVisitorWithPriority x, ElasticVisitorWithPriority y) {
                return x.Priority.CompareTo(y.Priority);
            }
        }
    }
}