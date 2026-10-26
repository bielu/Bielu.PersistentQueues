// Compares two BenchmarkDotNet JSON reports (the "full-compressed" exporter output) and fails when
// any benchmark in the head report is slower than the same benchmark in the base report by more
// than the threshold.
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

function medians(path) {
  const report = JSON.parse(readFileSync(path, "utf8"));
  return new Map(report.Benchmarks.map((b) => [`${b.Method}(${b.Parameters})`, b.Statistics?.Median]));
}

function formatNs(ns) {
  if (ns >= 1e6) return `${(ns / 1e6).toFixed(2)} ms`;
  if (ns >= 1e3) return `${(ns / 1e3).toFixed(2)} µs`;
  return `${ns.toFixed(0)} ns`;
}

const base = medians(basePath);
const head = medians(headPath);

const rows = [];
const regressions = [];
for (const [name, headMedian] of head) {
  const baseMedian = base.get(name);
  if (baseMedian == null || headMedian == null) {
    rows.push(`| \`${name}\` | n/a | ${headMedian == null ? "n/a" : formatNs(headMedian)} | new |`);
    continue;
  }
  const ratio = headMedian / baseMedian;
  const flag = ratio > threshold ? " ❌" : "";
  if (flag) regressions.push(name);
  rows.push(`| \`${name}\` | ${formatNs(baseMedian)} | ${formatNs(headMedian)} | ${ratio.toFixed(2)}${flag} |`);
}

const markdown = [
  `## Regression benchmarks: PR vs base (same runner, median)`,
  ``,
  `Threshold: ${thresholdArg}%. ${regressions.length === 0 ? "No regressions." : `${regressions.length} regression(s).`}`,
  ``,
  `| Benchmark | Base | PR | Ratio |`,
  `|:---|---:|---:|---:|`,
  ...rows,
  ``,
].join("\n");

if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);
console.log(markdown);

if (regressions.length > 0) {
  for (const name of regressions) console.log(`::error::${name} is more than ${thresholdArg}% of its base time`);
  process.exit(1);
}
