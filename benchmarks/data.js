window.BENCHMARK_DATA = {
  "lastUpdate": 1776199318328,
  "repoUrl": "https://github.com/bielu/Bielu.PersistentQueues",
  "entries": {
    "Regression Benchmarks": [
      {
        "commit": {
          "author": {
            "name": "Arkadiusz Biel",
            "username": "bielu",
            "email": "2244074+bielu@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "101c9c4e46f49d5b2d744a0fbfae88f95d0a2ca1",
          "message": "Merge pull request #38 from bielu/copilot/investigate-benchmark-issues\n\nFix benchmark PR comments and add weekly performance tracking",
          "timestamp": "2026-04-14T20:30:02Z",
          "url": "https://github.com/bielu/Bielu.PersistentQueues/commit/101c9c4e46f49d5b2d744a0fbfae88f95d0a2ca1"
        },
        "date": 1776199317702,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 22192381.9,
            "unit": "ns",
            "range": "± 1353844.978583885"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 22549418.5,
            "unit": "ns",
            "range": "± 1084183.9878884335"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3898436.5,
            "unit": "ns",
            "range": "± 805663.4038688241"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 23941217,
            "unit": "ns",
            "range": "± 1334799.2814419703"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 24644887.9,
            "unit": "ns",
            "range": "± 779431.0530732017"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3959855.4,
            "unit": "ns",
            "range": "± 578854.8514302182"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 22952878.3,
            "unit": "ns",
            "range": "± 1568771.405796332"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 22260122.9,
            "unit": "ns",
            "range": "± 203165.78288506164"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 3833018.6,
            "unit": "ns",
            "range": "± 738216.4729916964"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 25152687.25,
            "unit": "ns",
            "range": "± 1325033.866574329"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 26561467.4,
            "unit": "ns",
            "range": "± 2418837.883592098"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4089954,
            "unit": "ns",
            "range": "± 320466.7130202449"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 244394316,
            "unit": "ns",
            "range": "± 10904307.614753515"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 254544803.6,
            "unit": "ns",
            "range": "± 8491979.950260999"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 75470231,
            "unit": "ns",
            "range": "± 3828375.287176054"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 427707960.6,
            "unit": "ns",
            "range": "± 85471086.66384985"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 357412005.5,
            "unit": "ns",
            "range": "± 1613723.8469514127"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 84873695.3,
            "unit": "ns",
            "range": "± 6318704.259545204"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 289411343.6,
            "unit": "ns",
            "range": "± 10401560.103274282"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 333199410.5,
            "unit": "ns",
            "range": "± 6724728.773518676"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 80968064,
            "unit": "ns",
            "range": "± 3795850.22043222"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 470865894.5,
            "unit": "ns",
            "range": "± 120862139.69756684"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 483663819.5,
            "unit": "ns",
            "range": "± 2766622.920473563"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 90421265.5,
            "unit": "ns",
            "range": "± 4055009.655944977"
          }
        ]
      }
    ]
  }
}