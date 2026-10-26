// Compares two BenchmarkDotNet JSON reports (the "full-compressed" exporter output) and fails when
// any benchmark in the head report is slower than the same benchmark in the base report by more
// than the threshold AND the slowdown is outside the noise: the head's 99.9% confidence interval
// (BenchmarkDotNet's default) must lie entirely above the base's. A ratio alone is not enough: two
// runs of identical code on the same runner differed by up to 1.45x on the sub-millisecond cases.
//
//   node scripts/compare-benchmarks.mjs <base.json> <head.json> [threshold-percent=125]
//
// Used by .github/workflows/benchmark-pr.yml, which runs the base commit and the PR head back to back
// on the same runner. Medians are compared rather than means: these benchmarks are short, and a
// single GC pause or noisy-neighbour spike moves the mean far more than the median.
//
// Writes a markdown table to GITHUB_STEP_SUMMARY when set, and to stdout otherwise.
import { readFileSync, appendFileSync } from "node:fs";

const [basePath, headPath, thresholdArg = "125"] = process.argv.slice(2);
if (!basePath || !headPath) {
  console.error("usage: node scripts/compare-benchmarks.mjs <base.json> <head.json> [threshold-percent]");
  process.exit(2);
}
const threshold = Number(thresholdArg) / 100;

function statistics(path) {
  const report = JSON.parse(readFileSync(path, "utf8"));
  return new Map(report.Benchmarks.map((b) => [`${b.Method}(${b.Parameters})`, b.Statistics]));
}

function formatNs(ns) {
  if (ns >= 1e6) return `${(ns / 1e6).toFixed(2)} ms`;
  if (ns >= 1e3) return `${(ns / 1e3).toFixed(2)} µs`;
  return `${ns.toFixed(0)} ns`;
}

const base = statistics(basePath);
const head = statistics(headPath);

const rows = [];
const regressions = [];
for (const [name, headStats] of head) {
  const baseStats = base.get(name);
  if (baseStats?.Median == null || headStats?.Median == null) {
    rows.push(`| \`${name}\` | n/a | ${headStats?.Median == null ? "n/a" : formatNs(headStats.Median)} | new | |`);
    continue;
  }
  const ratio = headStats.Median / baseStats.Median;
  const separated = headStats.ConfidenceInterval.Lower > baseStats.ConfidenceInterval.Upper;
  const regressed = ratio > threshold && separated;
  if (regressed) regressions.push(name);
  const verdict = regressed ? "❌ regression" : ratio > threshold ? "noise (intervals overlap)" : "";
  rows.push(`| \`${name}\` | ${formatNs(baseStats.Median)} | ${formatNs(headStats.Median)} | ${ratio.toFixed(2)} | ${verdict} |`);
}

const markdown = [
  `## Regression benchmarks: PR vs base (same runner, median)`,
  ``,
  `Threshold: ${thresholdArg}%, outside the 99.9% confidence interval. ${regressions.length === 0 ? "No regressions." : `${regressions.length} regression(s).`}`,
  ``,
  `| Benchmark | Base | PR | Ratio | |`,
  `|:---|---:|---:|---:|:---|`,
  ...rows,
  ``,
].join("\n");

if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);
console.log(markdown);

if (regressions.length > 0) {
  for (const name of regressions) {
    console.log(`::error::${name} is more than ${thresholdArg}% of its base time, outside the confidence interval`);
  }
  process.exit(1);
}
