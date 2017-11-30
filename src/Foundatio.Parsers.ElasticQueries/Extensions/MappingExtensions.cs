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

        public static TDescriptor RootAlias<TDescriptor>(this TDescriptor descriptor, string alias) where TDescriptor : IDescriptor => AddAlias(descriptor, alias);

        public static TDescriptor Alias<TDescriptor>(this TDescriptor descriptor, string alias) where TDescriptor : IDescriptor => AddAlias(descriptor, alias, false);

        private static TDescriptor AddAlias<TDescriptor>(TDescriptor descriptor, string alias, bool isRoot = true) where TDescriptor : IDescriptor {
            if (descriptor == null)
                throw new ArgumentNullException(nameof(descriptor));

            if (alias == null)
                throw new ArgumentNullException(nameof(alias));

            var property = descriptor as IProperty;
            if (property == null)
                throw new ArgumentException($"{nameof(descriptor)} must implement {nameof(IProperty)} to use aliases", nameof(descriptor));

            if (property.LocalMetadata == null)
                property.LocalMetadata = new Dictionary<string, object>();

            property.LocalMetadata.Add(ALIAS_KEY, new KeyValuePair<string, bool>(alias, isRoot));
            return descriptor;
        }

        public static KeyValuePair<string, bool>? GetAliasFromDescriptor<TDescriptor>(this TDescriptor descriptor) where TDescriptor : IDescriptor {
            if (descriptor == null)
                throw new ArgumentNullException(nameof(descriptor));

            var property = descriptor as IProperty;
            return property?.GetAlias();
        }

        public static KeyValuePair<string, bool>? GetAlias(this IProperty property) {
            if (property == null)
                throw new ArgumentNullException(nameof(property));

            if (property.LocalMetadata == null)
                return null;

            if (property.LocalMetadata.TryGetValue(ALIAS_KEY, out object alias))
                return (KeyValuePair<string, bool>)alias;

            return null;
        }
    }
}