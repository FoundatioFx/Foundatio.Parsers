using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Extensions.Logging;
using Pegasus.Common;
using Pegasus.Common.Tracing;

namespace Foundatio.Parsers.LuceneQueries.Tests;

public class LoggingTracer : ITracer {
    private readonly ILogger _logger;
    private readonly bool _reportPerformance;
    private readonly RuleStats _cacheHitStats = new();
    private readonly Stack<RuleStackEntry> _ruleStack = new();
    private readonly Dictionary<string, RuleStats> _stats = new();
    private int _indentLevel;

    public LoggingTracer(ILogger logger, bool reportPerformance = false) {
        _logger = logger;
        _reportPerformance = reportPerformance;
    }

    public void TraceCacheHit<T>(string ruleName, Cursor cursor, CacheKey cacheKey, IParseResult<T> parseResult) {
        _ruleStack.Peek().CacheHit = true;
        _stats[ruleName].CacheHits++;
        TraceInfo(ruleName, cursor, "Cache hit.");
    }

    public void TraceCacheMiss(string ruleName, Cursor cursor, CacheKey cacheKey) {
        _ruleStack.Peek().CacheHit = false;
        _stats[ruleName].CacheMisses++;
        TraceInfo(ruleName, cursor, "Cache miss.");
    }

    public void TraceInfo(string ruleName, Cursor cursor, string info) => _logger.LogInformation($"{GetIndent()}{ruleName} at ({cursor.Line},{cursor.Column}): {info}");

    private string GetIndent() {
        return new string(' ', _indentLevel * 3);
    }

    public void TraceRuleEnter(string ruleName, Cursor cursor) {
        _ruleStack.Push(new RuleStackEntry {
            RuleName = ruleName,
            Cursor = cursor,
            Stopwatch = Stopwatch.StartNew(),
        });

        if (!_stats.TryGetValue(ruleName, out var ruleStats)) {
            ruleStats = _stats[ruleName] = new RuleStats();
        }

        ruleStats.Invocations++;
        var key = new CacheKey(ruleName, cursor.StateKey, cursor.Location);
        ruleStats.Locations.TryGetValue(key, out var count);
        ruleStats.Locations[key] = count + 1;

        _logger.LogInformation($"{GetIndent()}Start '{ruleName}' at ({cursor.Line},{cursor.Column}) with state key {cursor.StateKey}");
        _indentLevel++;
    }

    public void TraceRuleExit<T>(string ruleName, Cursor cursor, IParseResult<T> parseResult) {
        var success = parseResult != null;
        var entry = _ruleStack.Pop();
        entry.Stopwatch.Stop();
        var ticks = entry.Stopwatch.ElapsedTicks;
        _indentLevel--;

        if (entry.CacheHit ?? false) {
            _cacheHitStats.Invocations += 1;
            _cacheHitStats.TotalTicks += ticks;
        } else {
            _stats[ruleName].TotalTicks += ticks;
        }

        _logger.LogInformation($"{GetIndent()}End '{ruleName}' with {(success ? "success" : "failure")} at ({cursor.Line},{cursor.Column}) with state key {cursor.StateKey}");

        if (!_reportPerformance)
            return;

        if (_ruleStack.Count == 0) {
            var cacheHitTicks = _cacheHitStats.Invocations == 0
                ? 1.35
                : (double)_cacheHitStats.TotalTicks / _cacheHitStats.Invocations;

            ReportPerformance(TimeSpan.FromTicks((long)Math.Round(cacheHitTicks)), _stats.Select(stat => {
                var stats = stat.Value;
                var isCached = stats.CacheMisses > 0;
                var cacheHits = isCached ? stats.CacheHits : stats.Locations.Values.Where(v => v > 1).Select(v => v - 1).Sum();
                var cacheMisses = isCached ? stats.CacheMisses : stats.Locations.Count;
                var averageTicks = (double)stats.TotalTicks / (isCached ? stats.CacheMisses : stats.Invocations);

                var estimatedTimeWithoutCache = (cacheHits + cacheMisses) * averageTicks;
                var estimatedTimeWithCache = (cacheMisses * averageTicks) + ((cacheHits + cacheMisses) * cacheHitTicks);
                var estimatedTimeSaved = estimatedTimeWithoutCache - estimatedTimeWithCache;

                return new RulePerformanceInfo {
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

    protected virtual void ReportPerformance(TimeSpan averageCacheHitDuration, RulePerformanceInfo[] stats) {
        _logger.LogInformation($"Average Cache Hit Duration: {averageCacheHitDuration}");
        foreach (var stat in stats) {
            _logger.LogInformation($"Rule: {stat.Name}");

            _logger.LogInformation($"  Invocations: {stat.Invocations}");
            _logger.LogInformation($"  Average Duration: {stat.AverageTime}");
            _logger.LogInformation($"  Is Cached: {stat.IsCached}");

            if (stat.IsCached) {
                _logger.LogInformation($"    Cache Hits: {stat.CacheHits}");
                _logger.LogInformation($"    Cache Misses: {stat.CacheMisses}");
            } else {
                _logger.LogInformation($"    Redundant Invocations: {stat.CacheHits}");
            }

            if (stat.IsCached || stat.CacheHits > 0) {
                _logger.LogInformation($"  Estimated Time Saved: {stat.EstimatedTotalTimeSaved}");
            }

            if (!stat.IsCached && stat.EstimatedTotalTimeSaved > TimeSpan.Zero) {
                _logger.LogInformation($"  Recommendation: Add the -memoize flag to `{stat.Name}`. (Saves {stat.EstimatedTotalTimeSaved})");
            } else if (stat.IsCached && stat.EstimatedTotalTimeSaved < -TimeSpan.FromMilliseconds(10)) {
                _logger.LogInformation($"  Recommendation: Remove -memoize flag from `{stat.Name}`. (Saves {stat.EstimatedTotalTimeSaved.Negate()})");
            }
        }
    }

    /// <summary>
    /// Summarizes the performance of a specific rule.
    /// </summary>
    protected class RulePerformanceInfo {
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

    private class RuleStackEntry {
        public bool? CacheHit { get; set; }

        public Cursor Cursor { get; set; }

        public string RuleName { get; set; }

        public Stopwatch Stopwatch { get; set; }
    }

    private class RuleStats {
        public int CacheHits { get; set; }

        public int CacheMisses { get; set; }

        public int Invocations { get; set; }

        public Dictionary<CacheKey, int> Locations { get; } = new Dictionary<CacheKey, int>();

        public long TotalTicks { get; set; }
    }
}
