using Nest;

namespace Foundatio.Parsers.ElasticQueries {
    public class NamedAggregationContainer {
        public NamedAggregationContainer() {
            Container = new AggregationContainer();
        }

        public NamedAggregationContainer(string name, IAggregationContainer container) {
            Name = name;
            Container = container;
        }

        public string Name { get; set; }
        public IAggregationContainer Container { get; set; }
    }
}