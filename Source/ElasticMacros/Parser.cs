using System;
using System.Linq;
using ElasticQueryParser;
using Exceptionless.ElasticQueryParser.Extensions;
using Exceptionless.ElasticQueryParser.Filter;
using Exceptionless.ElasticQueryParser.Filter.Nodes;
using Exceptionless.ElasticQueryParser.Query;
using Exceptionless.ElasticQueryParser.Query.Nodes;
using Exceptionless.LuceneQueryParser;
using Exceptionless.LuceneQueryParser.Nodes;
using Exceptionless.LuceneQueryParser.Visitor;
using Nest;

namespace Exceptionless.ElasticQueryParser {
    public class Parser {
        private readonly QueryParser _parser = new QueryParser();
        private readonly ChainedQueryVisitor _filterVisitor = new ChainedQueryVisitor();
        private readonly ChainedQueryVisitor _queryVisitor = new ChainedQueryVisitor();

        public Parser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);

            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _filterVisitor.AddVisitor(visitor);
            _filterVisitor.AddVisitor(new FilterContainerVisitor(config), 100);
            _filterVisitor.AddVisitor(new CombineFiltersVisitor(config), 100000);

            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _queryVisitor.AddVisitor(visitor);
            _queryVisitor.AddVisitor(new QueryContainerVisitor(config), 100);
            _queryVisitor.AddVisitor(new CombineQueriesVisitor(config), 100000);
        }

        public FilterContainer BuildFilter(string query) {
            var result = _parser.Parse(query);

            result = result.ToFilter() as FilterGroupNode;
            var filterNode = _filterVisitor.Accept(result) as IElasticFilterNode;

            return filterNode?.Filter ?? new MatchAllFilter();
        }

        public QueryContainer BuildQuery(string query) {
            var result = _parser.Parse(query);

            result = result.ToQuery() as QueryGroupNode;
            var queryNode = _queryVisitor.Accept(result) as IElasticQueryNode;

            return queryNode?.Query ?? new MatchAllQuery();
        }

        // parser query, generate filter, generate aggregations
        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
        // _exists_:field1
        // automatic field alias management
    }
}
