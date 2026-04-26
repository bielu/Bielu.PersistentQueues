window.BENCHMARK_DATA = {
  "lastUpdate": 1777173100629,
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
      },
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
          "id": "f8b318394ab8d15ef662075dd5e2dc0e723b4e08",
          "message": "Update benchmark results in README [no-ci]\n\nUpdated benchmark results and added a link to the latest benchmarks.",
          "timestamp": "2026-04-14T20:49:04Z",
          "url": "https://github.com/bielu/Bielu.PersistentQueues/commit/f8b318394ab8d15ef662075dd5e2dc0e723b4e08"
        },
        "date": 1776568258001,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 22566234.5,
            "unit": "ns",
            "range": "± 439853.91058282065"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 24856653.2,
            "unit": "ns",
            "range": "± 2590009.8107035616"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 4064344.8,
            "unit": "ns",
            "range": "± 767992.8550101882"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 25924742.25,
            "unit": "ns",
            "range": "± 1571041.9065444807"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 24975235,
            "unit": "ns",
            "range": "± 846017.5667486659"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 4750674,
            "unit": "ns",
            "range": "± 842083.3851742356"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 23564199.5,
            "unit": "ns",
            "range": "± 2773235.0057439236"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 23343030.9,
            "unit": "ns",
            "range": "± 1111462.938877091"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4679277.5,
            "unit": "ns",
            "range": "± 1325333.6514185776"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 27023889.5,
            "unit": "ns",
            "range": "± 229180.77807922722"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 27805928.4,
            "unit": "ns",
            "range": "± 960899.9341670807"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4665179.9,
            "unit": "ns",
            "range": "± 1258447.650159632"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 244585731.2,
            "unit": "ns",
            "range": "± 8601526.001244733"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 261118936.4,
            "unit": "ns",
            "range": "± 1610948.929403195"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 76600829,
            "unit": "ns",
            "range": "± 3393889.990088512"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 441677441.3,
            "unit": "ns",
            "range": "± 85766460.0089492"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 364387546.5,
            "unit": "ns",
            "range": "± 5122138.938688863"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 83459487.5,
            "unit": "ns",
            "range": "± 1592056.678186008"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 294822780.4,
            "unit": "ns",
            "range": "± 9283072.846723778"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 339400243,
            "unit": "ns",
            "range": "± 2299291.1371218306"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 89741729.5,
            "unit": "ns",
            "range": "± 1998738.4283029633"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 520612500.5,
            "unit": "ns",
            "range": "± 120320486.0227407"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 480697761.8,
            "unit": "ns",
            "range": "± 12319382.874906952"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 95794932,
            "unit": "ns",
            "range": "± 2768485.664572421"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Arkadiusz biel",
            "username": "bielu",
            "email": "bielu@bielu.com.pl"
          },
          "committer": {
            "name": "Arkadiusz biel",
            "username": "bielu",
            "email": "bielu@bielu.com.pl"
          },
          "id": "02a0a8bd61ffb7ea18c86d9d479d270cebbe25a3",
          "message": "bump packages",
          "timestamp": "2026-04-22T18:24:24Z",
          "url": "https://github.com/bielu/Bielu.PersistentQueues/commit/02a0a8bd61ffb7ea18c86d9d479d270cebbe25a3"
        },
        "date": 1777173098982,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 20078653,
            "unit": "ns",
            "range": "± 1035405.546864609"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 20970985.4,
            "unit": "ns",
            "range": "± 1943732.208932676"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3290230.2,
            "unit": "ns",
            "range": "± 710500.6822753233"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 21958380.4,
            "unit": "ns",
            "range": "± 481428.5523027275"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 21314720.75,
            "unit": "ns",
            "range": "± 515332.3923788574"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3773860.3,
            "unit": "ns",
            "range": "± 593233.8753174838"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 20451896.25,
            "unit": "ns",
            "range": "± 320472.6060392422"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 20728001.75,
            "unit": "ns",
            "range": "± 1239369.7949485928"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 3465980.8,
            "unit": "ns",
            "range": "± 861868.9554277959"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24422824.4,
            "unit": "ns",
            "range": "± 2657578.5179376733"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24934433.75,
            "unit": "ns",
            "range": "± 2584950.3625812465"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4188401,
            "unit": "ns",
            "range": "± 368420.97793217475"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 230133552.1,
            "unit": "ns",
            "range": "± 12378387.08157203"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 266112495.9,
            "unit": "ns",
            "range": "± 3036353.6590270246"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 74686997.25,
            "unit": "ns",
            "range": "± 881740.182405367"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 349293886,
            "unit": "ns",
            "range": "± 118803305.65568173"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 365307272.6,
            "unit": "ns",
            "range": "± 13876819.845373427"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 76560138.5,
            "unit": "ns",
            "range": "± 1996459.2974928557"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 291113003.4,
            "unit": "ns",
            "range": "± 13509977.399557346"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 334653143.25,
            "unit": "ns",
            "range": "± 5689864.558155456"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 78545437.75,
            "unit": "ns",
            "range": "± 1564277.5451511953"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 518571284.7,
            "unit": "ns",
            "range": "± 97499666.39704284"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 493730868.2,
            "unit": "ns",
            "range": "± 9760546.9862432"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 83271016,
            "unit": "ns",
            "range": "± 1973950.4543962935"
          }
        ]
      }
    ]
  }
}