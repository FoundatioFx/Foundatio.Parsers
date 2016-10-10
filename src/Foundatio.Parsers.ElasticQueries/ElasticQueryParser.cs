using System;
using System.Linq;
using Foundatio.Parsers.ElasticQueries.Extensions;
using Foundatio.Parsers.ElasticQueries.Filter;
using Foundatio.Parsers.ElasticQueries.Query;
using Foundatio.Parsers.ElasticQueries.Visitors;
using Foundatio.Parsers.LuceneQueries;
using Foundatio.Parsers.LuceneQueries.Visitors;
using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class ElasticQueryParser {
        private readonly LuceneQueryParser _parser = new LuceneQueryParser();
        private readonly ChainedQueryVisitor _filterVisitors = new ChainedQueryVisitor();
        private readonly CombineFiltersVisitor _combineFiltersVisitor;
        private readonly ChainedQueryVisitor _queryVisitors = new ChainedQueryVisitor();
        private readonly CombineQueriesVisitor _combineQueriesVisitor;
        private readonly AliasResolver _defaultAliasResolver;

        public ElasticQueryParser(Action<ElasticQueryParserConfiguration> configure = null) {
            var config = new ElasticQueryParserConfiguration();
            configure?.Invoke(config);
            _defaultAliasResolver = config.DefaultAliasResolver;

            _filterVisitors.AddVisitor(new DefaultFilterVisitor(config), 100);
            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _filterVisitors.AddVisitor(visitor);

            _combineFiltersVisitor = new CombineFiltersVisitor(config);

            _queryVisitors.AddVisitor(new DefaultQueryVisitor(config), 100);
            foreach (var visitor in config.Visitors.OfType<QueryVisitorWithPriority>())
                _queryVisitors.AddVisitor(visitor);

            _combineQueriesVisitor = new CombineQueriesVisitor(config);
        }

        public FilterContainer BuildFilter(string filter, string aggregations = null, IQueryVisitorContext context = null)  {
            var result = _parser.Parse(filter);

            if (context == null)
                context = new ElasticQueryVisitorContext();

            if (_defaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_defaultAliasResolver);
            
            var filterNode = _filterVisitors.Accept(result, context);
            filterNode = _combineFiltersVisitor.Accept(filterNode, context);

            return filterNode?.GetFilterContainer() ?? new MatchAllFilter();
        }

        public QueryContainer BuildQuery(string query, string aggregations = null, IQueryVisitorContext context = null) {
            var result = _parser.Parse(query);

            if (context == null)
                context = new ElasticQueryVisitorContext();

            if (_defaultAliasResolver != null && context.GetRootAliasResolver() == null)
                context.SetRootAliasResolver(_defaultAliasResolver);

            var queryNode = _queryVisitors.Accept(result, context);
            queryNode = _combineQueriesVisitor.Accept(queryNode, context);

            return queryNode?.GetQueryContainer() ?? new MatchAllQuery();
        }

        // want to be able to support things like date macro expansion (now-1d/d), geo query string filters, etc
        // date:"last 30 days"
        // number ranges field:1..
        // automatic field alias management
    }
}
