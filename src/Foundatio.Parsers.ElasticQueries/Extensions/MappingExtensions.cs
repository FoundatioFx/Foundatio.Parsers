using System;
using System.Collections.Generic;
using Nest;

namespace Foundatio.Parsers.ElasticQueries.Extensions {
    public static class MappingExtensions {
        private const string ALIAS_KEY = "@@alias";

        public static TDescriptor Alias<TDescriptor>(this TDescriptor descriptor, string alias) where TDescriptor : IDescriptor {
            if (descriptor == null)
                throw new ArgumentNullException(nameof(descriptor));

            if (alias == null)
                throw new ArgumentNullException(nameof(alias));

            var property = descriptor as IProperty;
            if (property == null)
                throw new ArgumentException($"{nameof(descriptor)} must implement {nameof(IProperty)} to use aliases", nameof(descriptor));

            if (property.LocalMetadata == null)
                property.LocalMetadata = new Dictionary<string, object>();

            property.LocalMetadata.Add(ALIAS_KEY, alias);
            return descriptor;
        }

        public static string GetAliasFromDescriptor<TDescriptor>(this TDescriptor descriptor) where TDescriptor : IDescriptor {
            if (descriptor == null)
                throw new ArgumentNullException(nameof(descriptor));

            var property = descriptor as IProperty;
            return property?.GetAlias();
        }

        public static string GetAlias(this IProperty property) {
            if (property == null)
                throw new ArgumentNullException(nameof(property));

            if (property.LocalMetadata == null)
                return null;

            object alias;
            if (property.LocalMetadata.TryGetValue(ALIAS_KEY, out alias))
                return (string)alias;

            return null;
        }
    }
}