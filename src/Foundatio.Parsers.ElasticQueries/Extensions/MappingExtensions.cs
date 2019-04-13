using System;
using System.Collections.Generic;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class MappingExtensions {
        private const string ALIAS_KEY = "@Alias";

        public static TDescriptor IncludeInAll<TDescriptor>(this TDescriptor descriptor) where TDescriptor : ICoreProperty {
            descriptor.CopyTo = "_all";
            return descriptor;
        }
    }
}