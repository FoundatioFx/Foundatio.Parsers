using System;
using System.Collections.Generic;
using System.Linq;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Nest;

namespace ElasticMacros.Visitor {
    public class ElasticMacrosConfiguration {
        private readonly SortedSet<ElasticMacroWithPriority> _macros = new SortedSet<ElasticMacroWithPriority>(new ElasticMacroWithPriority.PriorityComparer());
        private readonly SortedSet<ElasticVisitorWithPriority> _visitors = new SortedSet<ElasticVisitorWithPriority>(new ElasticVisitorWithPriority.PriorityComparer());

        public string DefaultField { get; private set; }
        public Operator DefaultOperator { get; private set; } = Operator.And;
        public IList<IElasticMacro> Macros => _macros.Cast<IElasticMacro>().ToList();
        public IList<IQueryNodeVisitorWithResult<IQueryNode>> Visitors => _visitors.Cast<IQueryNodeVisitorWithResult<IQueryNode>>().ToList();

        public ElasticMacrosConfiguration SetDefaultField(string field) {
            DefaultField = field;
            return this;
        }

        public ElasticMacrosConfiguration SetDefaultOperator(Operator op) {
            DefaultOperator = op;
            return this;
        }

        public ElasticMacrosConfiguration AddMacro(IElasticMacro macro, int priority = 0) {
            _macros.Add(new ElasticMacroWithPriority { Macro = macro, Priority = priority });
            return this;
        }

        public ElasticMacrosConfiguration AddVisitor(IQueryNodeVisitorWithResult<IQueryNode> visitor, int priority = 0) {
            _visitors.Add(new ElasticVisitorWithPriority { Visitor = visitor, Priority = priority });
            return this;
        }

        public ElasticMacrosConfiguration UseAliases(Func<string, string> aliasFunc, int priority = 0) {
            return AddVisitor(new AliasedQueryVisitor(aliasFunc), priority);
        }

        public ElasticMacrosConfiguration UseAliases(IDictionary<string, string> aliasMap, int priority = 0) {
            return AddVisitor(new AliasedQueryVisitor(field => aliasMap.ContainsKey(field) ? aliasMap[field] : field), priority);
        }

        public ElasticMacrosConfiguration UseGeoRanges(IEnumerable<string> geoFields, int priority = 0) {
            var fields = new HashSet<string>(geoFields, StringComparer.OrdinalIgnoreCase);
            return AddMacro(new GeoRangeMacro(f => fields.Contains(f)), priority);
        }

        public ElasticMacrosConfiguration UseGeoRanges(params string[] geoFields) {
            return UseGeoRanges(geoFields.ToList());
        }

        public ElasticMacrosConfiguration UseGeoRanges(int priority, params string[] geoFields) {
            return UseGeoRanges(geoFields.ToList(), priority);
        }

        public ElasticMacrosConfiguration UseGeoRanges(Func<string, bool> isGeoField, int priority = 0) {
            return AddMacro(new GeoRangeMacro(isGeoField), priority);
        }
    }

    internal class ElasticMacroWithPriority : IElasticMacro {
        public int Priority { get; set; }
        public IElasticMacro Macro { get; set; }

        public PlainFilter Expand(GroupNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            return Macro.Expand(node, currentFilter, context);
        }

        public PlainFilter Expand(TermNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            return Macro.Expand(node, currentFilter, context);
        }

        public PlainFilter Expand(TermRangeNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            return Macro.Expand(node, currentFilter, context);
        }

        public PlainFilter Expand(MissingNode node, PlainFilter currentFilter, ElasticMacroContext context) {
            return Macro.Expand(node, currentFilter, context);
        }

        public PlainFilter Expand(ExistsNode node, PlainFilter currentFilter, ElasticMacroContext context) { 
            return Macro.Expand(node, currentFilter, context);
        }

        internal class PriorityComparer : IComparer<ElasticMacroWithPriority> {
            public int Compare(ElasticMacroWithPriority x, ElasticMacroWithPriority y) {
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