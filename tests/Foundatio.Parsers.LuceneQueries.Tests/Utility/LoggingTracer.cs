using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Extensions.Logging;
using Pegasus.Common;
using Pegasus.Common.Tracing;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class LoggingTracer : ITracer
{
    private readonly ILogger _logger;
    private readonly bool _reportPerformance;
    private readonly RuleStats _cacheHitStats = new();
    private readonly Stack<RuleStackEntry> _ruleStack = new();
    private readonly Dictionary<string, RuleStats> _stats = new();
    private int _indentLevel;

    public LoggingTracer(ILogger logger, bool reportPerformance = false)
    {
        _logger = logger;
        _reportPerformance = reportPerformance;
    }

    public void TraceCacheHit<T>(string ruleName, Cursor cursor, CacheKey cacheKey, IParseResult<T> parseResult)
    {
        _ruleStack.Peek().CacheHit = true;
        _stats[ruleName].CacheHits++;
        TraceInfo(ruleName, cursor, "Cache hit.");
    }

    public void TraceCacheMiss(string ruleName, Cursor cursor, CacheKey cacheKey)
    {
        _ruleStack.Peek().CacheHit = false;
        _stats[ruleName].CacheMisses++;
        TraceInfo(ruleName, cursor, "Cache miss.");
    }

    public void TraceInfo(string ruleName, Cursor cursor, string info) => _logger.LogInformation($"{GetIndent()}{ruleName} at ({cursor.Line},{cursor.Column}): {info}");

    private string GetIndent()
    {
        return new string(' ', _indentLevel * 3);
    }

    public void TraceRuleEnter(string ruleName, Cursor cursor)
    {
        _ruleStack.Push(new RuleStackEntry
        {
            RuleName = ruleName,
            Cursor = cursor,
            Stopwatch = Stopwatch.StartNew(),
        });

        if (!_stats.TryGetValue(ruleName, out var ruleStats))
        {
            ruleStats = _stats[ruleName] = new RuleStats();
        }

        ruleStats.Invocations++;
        var key = new CacheKey(ruleName, cursor.StateKey, cursor.Location);
        ruleStats.Locations.TryGetValue(key, out int count);
        ruleStats.Locations[key] = count + 1;

        _logger.LogInformation("{Indent}Start \'{RuleName}\' at ({CursorLine},{CursorColumn}) with state key {CursorStateKey}", GetIndent(), ruleName, cursor.Line, cursor.Column, cursor.StateKey);
        _indentLevel++;
    }

    public void TraceRuleExit<T>(string ruleName, Cursor cursor, IParseResult<T> parseResult)
    {
        bool success = parseResult != null;
        var entry = _ruleStack.Pop();
        entry.Stopwatch.Stop();
        long ticks = entry.Stopwatch.ElapsedTicks;
        _indentLevel--;

        if (entry.CacheHit ?? false)
        {
            _cacheHitStats.Invocations += 1;
            _cacheHitStats.TotalTicks += ticks;
        }
        else
        {
            _stats[ruleName].TotalTicks += ticks;
        }

        _logger.LogInformation("{Indent}End \'{RuleName}\' with {Success} at ({CursorLine},{CursorColumn}) with state key {CursorStateKey}", GetIndent(), ruleName, (success ? "success" : "failure"), cursor.Line, cursor.Column, cursor.StateKey);

        if (!_reportPerformance)
            return;

        if (_ruleStack.Count == 0)
        {
            double cacheHitTicks = _cacheHitStats.Invocations == 0
                ? 1.35
                : (double)_cacheHitStats.TotalTicks / _cacheHitStats.Invocations;

            ReportPerformance(TimeSpan.FromTicks((long)Math.Round(cacheHitTicks)), _stats.Select(stat =>
            {
                var stats = stat.Value;
                bool isCached = stats.CacheMisses > 0;
                int cacheHits = isCached ? stats.CacheHits : stats.Locations.Values.Where(v => v > 1).Select(v => v - 1).Sum();
                int cacheMisses = isCached ? stats.CacheMisses : stats.Locations.Count;
                double averageTicks = (double)stats.TotalTicks / (isCached ? stats.CacheMisses : stats.Invocations);

                double estimatedTimeWithoutCache = (cacheHits + cacheMisses) * averageTicks;
                double estimatedTimeWithCache = (cacheMisses * averageTicks) + ((cacheHits + cacheMisses) * cacheHitTicks);
                double estimatedTimeSaved = estimatedTimeWithoutCache - estimatedTimeWithCache;

                return new RulePerformanceInfo
                {
                    Name = stat.Key,
                    Invocations = stats.Invocations,
                    AverageTime = TimeSpan.FromTicks((long)Math.Round(averageTicks)),
                    IsCached = isCached,
                    CacheHits = cacheHits,
                    CacheMisses = cacheMisses,
                    EstimatedTotalTimeSaved = TimeSpan.FromTicks((long)Math.Round(estimatedTimeSaved)),
                };
            }).ToArray());
        }
    }

    protected virtual void ReportPerformance(TimeSpan averageCacheHitDuration, RulePerformanceInfo[] stats)
    {
        _logger.LogInformation("Average Cache Hit Duration: {AverageCacheHitDuration}", averageCacheHitDuration);
        foreach (var stat in stats)
        {
            _logger.LogInformation("Rule: {StatName}", stat.Name);

            _logger.LogInformation("  Invocations: {StatInvocations}", stat.Invocations);
            _logger.LogInformation("  Average Duration: {StatAverageTime}", stat.AverageTime);
            _logger.LogInformation("  Is Cached: {StatIsCached}", stat.IsCached);

            if (stat.IsCached)
            {
                _logger.LogInformation("    Cache Hits: {StatCacheHits}", stat.CacheHits);
                _logger.LogInformation("    Cache Misses: {StatCacheMisses}", stat.CacheMisses);
            }
            else
            {
                _logger.LogInformation("    Redundant Invocations: {StatCacheHits}", stat.CacheHits);
            }

            if (stat.IsCached || stat.CacheHits > 0)
            {
                _logger.LogInformation("  Estimated Time Saved: {StatEstimatedTotalTimeSaved}", stat.EstimatedTotalTimeSaved);
            }

            if (!stat.IsCached && stat.EstimatedTotalTimeSaved > TimeSpan.Zero)
            {
                _logger.LogInformation("  Recommendation: Add the -memoize flag to `{StatName}`. (Saves {StatEstimatedTotalTimeSaved})", stat.Name, stat.EstimatedTotalTimeSaved);
            }
            else if (stat.IsCached && stat.EstimatedTotalTimeSaved < -TimeSpan.FromMilliseconds(10))
            {
                _logger.LogInformation("  Recommendation: Remove -memoize flag from `{StatName}`. (Saves {Negate})", stat.Name, stat.EstimatedTotalTimeSaved.Negate());
            }
        }
    }

    /// <summary>
    /// Summarizes the performance of a specific rule.
    /// </summary>
    protected class RulePerformanceInfo
    {
        /// <summary>
        /// Gets the average duration of each invocation.
        /// </summary>
        public TimeSpan AverageTime { get; internal set; }

        /// <summary>
        /// Gets the total number of invocations that were a cache hit, or the total number of redundant invocations if the rule is not cached.
        /// </summary>
        public int CacheHits { get; internal set; }

        /// <summary>
        /// Gets the total number of invocations that were a cache miss, or the total number of unique invocations if the rule is not cached.
        /// </summary>
        public int CacheMisses { get; internal set; }

        /// <summary>
        /// Gets the estimated total time saved by memoizing this rule.
        /// </summary>
        public TimeSpan EstimatedTotalTimeSaved { get; internal set; }

        /// <summary>
        /// Gets the total number of invocations.
        /// </summary>
        public int Invocations { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether or not this rule is memoized.
        /// </summary>
        public bool IsCached { get; internal set; }

        /// <summary>
        /// Gets the name of the rule.
        /// </summary>
        public string Name { get; internal set; }
    }

    private class RuleStackEntry
    {
        public bool? CacheHit { get; set; }

        public Cursor Cursor { get; set; }

        public string RuleName { get; set; }

        public Stopwatch Stopwatch { get; set; }
    }

    private class RuleStats
    {
        public int CacheHits { get; set; }

        public int CacheMisses { get; set; }

        public int Invocations { get; set; }

        public Dictionary<CacheKey, int> Locations { get; } = new Dictionary<CacheKey, int>();

        public long TotalTicks { get; set; }
    }
}
