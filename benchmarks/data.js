window.BENCHMARK_DATA = {
  "lastUpdate": 1783176551122,
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
        "date": 1777777960462,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 22356663.8,
            "unit": "ns",
            "range": "± 1686996.9875441983"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 22389983.3,
            "unit": "ns",
            "range": "± 631664.063998974"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3949331.2,
            "unit": "ns",
            "range": "± 712322.797655459"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 25715872.7,
            "unit": "ns",
            "range": "± 1066466.5114447805"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 24529591.5,
            "unit": "ns",
            "range": "± 270240.9159126969"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3919739.25,
            "unit": "ns",
            "range": "± 249049.98814478322"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 24395493.5,
            "unit": "ns",
            "range": "± 2043843.8431569326"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 23644578,
            "unit": "ns",
            "range": "± 924473.8431003154"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4184510.6,
            "unit": "ns",
            "range": "± 841827.9337942523"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24975879,
            "unit": "ns",
            "range": "± 895284.1333169041"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 25684275.25,
            "unit": "ns",
            "range": "± 357447.93780668254"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4454474.7,
            "unit": "ns",
            "range": "± 431006.1083670393"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 246801777.75,
            "unit": "ns",
            "range": "± 6650737.659761979"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 264860572.5,
            "unit": "ns",
            "range": "± 4610826.030659655"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 76405277,
            "unit": "ns",
            "range": "± 3775289.666807237"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 415274807.9,
            "unit": "ns",
            "range": "± 92856889.28797871"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 366215512.3,
            "unit": "ns",
            "range": "± 6019968.036919481"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 81926102.25,
            "unit": "ns",
            "range": "± 2295496.4676171727"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 292004672.3,
            "unit": "ns",
            "range": "± 12516023.39672658"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 329276004.75,
            "unit": "ns",
            "range": "± 2242289.3586654356"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 83112678.75,
            "unit": "ns",
            "range": "± 1589054.6000232077"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 507652537.75,
            "unit": "ns",
            "range": "± 107701861.83720447"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 477987249.5,
            "unit": "ns",
            "range": "± 16297793.804063682"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 93100774.75,
            "unit": "ns",
            "range": "± 1396173.9988703353"
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
        "date": 1778382787256,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 238196996.8,
            "unit": "ns",
            "range": "± 300695685.47275776"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 23156469.2,
            "unit": "ns",
            "range": "± 1576786.3418035433"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3649322,
            "unit": "ns",
            "range": "± 842601.474336771"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 25333336.25,
            "unit": "ns",
            "range": "± 1075251.3407042327"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 26205420.5,
            "unit": "ns",
            "range": "± 1320268.3066823955"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 5540065.1,
            "unit": "ns",
            "range": "± 1876151.9118123937"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 23130939.6,
            "unit": "ns",
            "range": "± 1523358.061480885"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 23250034.5,
            "unit": "ns",
            "range": "± 1154385.557899093"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4682470.2,
            "unit": "ns",
            "range": "± 1494144.3075122295"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24943332.9,
            "unit": "ns",
            "range": "± 1453182.5116222668"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 26640271.1,
            "unit": "ns",
            "range": "± 1579352.4673423283"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4642845.4,
            "unit": "ns",
            "range": "± 577355.4634216256"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 247276857.8,
            "unit": "ns",
            "range": "± 4693447.235250302"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 259614283.9,
            "unit": "ns",
            "range": "± 10317578.459567599"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 77332552,
            "unit": "ns",
            "range": "± 2215114.4803735693"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 342242893.7,
            "unit": "ns",
            "range": "± 126134790.87699041"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 358172600,
            "unit": "ns",
            "range": "± 3802145.9767099507"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 83810750.5,
            "unit": "ns",
            "range": "± 3076009.4909262983"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 287926432,
            "unit": "ns",
            "range": "± 6110547.761771771"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 334867150,
            "unit": "ns",
            "range": "± 3955077.682738229"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 88230955.5,
            "unit": "ns",
            "range": "± 2397598.6112965196"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 500068344.5,
            "unit": "ns",
            "range": "± 117517428.09539676"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 483885189.6,
            "unit": "ns",
            "range": "± 10628213.421300557"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 97456577.25,
            "unit": "ns",
            "range": "± 4700314.13830887"
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
        "date": 1778987603402,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 20956378.6,
            "unit": "ns",
            "range": "± 984557.8471813122"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 21782602.1,
            "unit": "ns",
            "range": "± 540060.5003004756"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3652477.1,
            "unit": "ns",
            "range": "± 658573.4013792996"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 23142922.8,
            "unit": "ns",
            "range": "± 825508.7181754654"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 25858357.6,
            "unit": "ns",
            "range": "± 2638497.199475262"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 4206683.8,
            "unit": "ns",
            "range": "± 784791.9429072906"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 20943969.25,
            "unit": "ns",
            "range": "± 411530.5483042338"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 21492496.8,
            "unit": "ns",
            "range": "± 607475.1963584193"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4176215,
            "unit": "ns",
            "range": "± 1282925.46687444"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 23646763.4,
            "unit": "ns",
            "range": "± 740291.129279083"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24606569.3,
            "unit": "ns",
            "range": "± 784943.8375703704"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4475847.3,
            "unit": "ns",
            "range": "± 272407.66160058713"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 239839065.6,
            "unit": "ns",
            "range": "± 9358939.929281777"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 253974301,
            "unit": "ns",
            "range": "± 4614245.203937368"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 73365364.75,
            "unit": "ns",
            "range": "± 1064890.2339222182"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 394620474.6,
            "unit": "ns",
            "range": "± 118901531.04419214"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 348821690.75,
            "unit": "ns",
            "range": "± 4409548.9177988395"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 80847203.75,
            "unit": "ns",
            "range": "± 2436556.0395746254"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 291074864.2,
            "unit": "ns",
            "range": "± 11232243.242090832"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 339351474.75,
            "unit": "ns",
            "range": "± 2458043.8077528747"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 89365208,
            "unit": "ns",
            "range": "± 1568184.1629240063"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 473800817,
            "unit": "ns",
            "range": "± 131562703.56200847"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 489101572.7,
            "unit": "ns",
            "range": "± 5850968.5594936935"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 91864031.75,
            "unit": "ns",
            "range": "± 2586466.629028667"
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
        "date": 1779592458253,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 26090060.25,
            "unit": "ns",
            "range": "± 1584057.5712802391"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 27084685.5,
            "unit": "ns",
            "range": "± 515223.3557705823"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3936287.5,
            "unit": "ns",
            "range": "± 903451.8046597727"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 22891004.7,
            "unit": "ns",
            "range": "± 995613.1217627658"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 26168672.75,
            "unit": "ns",
            "range": "± 1010327.2263589901"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 4136831.2,
            "unit": "ns",
            "range": "± 568052.2274357349"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 26172380.9,
            "unit": "ns",
            "range": "± 1176607.625920298"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 25878094.8,
            "unit": "ns",
            "range": "± 2031280.2484347648"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4405227.2,
            "unit": "ns",
            "range": "± 616154.8488660946"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 27063580,
            "unit": "ns",
            "range": "± 531374.0783917359"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 27277650.5,
            "unit": "ns",
            "range": "± 2017619.4835638606"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 5002275.5,
            "unit": "ns",
            "range": "± 730628.7777866405"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 274863401,
            "unit": "ns",
            "range": "± 14553731.259671573"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 294105846.9,
            "unit": "ns",
            "range": "± 16599549.06299285"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 76081932.25,
            "unit": "ns",
            "range": "± 3205898.9396685995"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 477279573.8,
            "unit": "ns",
            "range": "± 107860562.97378317"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 364265894,
            "unit": "ns",
            "range": "± 6748892.067915296"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 82027961.5,
            "unit": "ns",
            "range": "± 5074476.032249142"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 307634247,
            "unit": "ns",
            "range": "± 10586517.729489641"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 338260584.5,
            "unit": "ns",
            "range": "± 5974950.078261101"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 85005368.25,
            "unit": "ns",
            "range": "± 1951128.0779551393"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 510856562,
            "unit": "ns",
            "range": "± 107198825.86638567"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 490617522.5,
            "unit": "ns",
            "range": "± 5261061.091820166"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 94598595.75,
            "unit": "ns",
            "range": "± 2263803.34364676"
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
        "date": 1780197317217,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 15604341.3,
            "unit": "ns",
            "range": "± 652217.713154281"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 15946471.75,
            "unit": "ns",
            "range": "± 317122.0245808386"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3393854.9,
            "unit": "ns",
            "range": "± 992376.2771793267"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 17405651.4,
            "unit": "ns",
            "range": "± 668544.7674346872"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 20079703.25,
            "unit": "ns",
            "range": "± 2956877.1322042197"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3445378.4,
            "unit": "ns",
            "range": "± 309141.33868313377"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 15591647,
            "unit": "ns",
            "range": "± 841680.5605117261"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 16932941.6,
            "unit": "ns",
            "range": "± 1438638.1609028033"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 3260782.9,
            "unit": "ns",
            "range": "± 833569.3173688676"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 18296186.25,
            "unit": "ns",
            "range": "± 1010543.9414112167"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 28126854.2,
            "unit": "ns",
            "range": "± 7795850.386929171"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 3761459.8,
            "unit": "ns",
            "range": "± 560421.9593892623"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 220170599.25,
            "unit": "ns",
            "range": "± 2596519.797872192"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 256351095,
            "unit": "ns",
            "range": "± 1988803.8585282024"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 66673998,
            "unit": "ns",
            "range": "± 1291029.538485855"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 406791095.7,
            "unit": "ns",
            "range": "± 94825887.9826842"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 354571111.6,
            "unit": "ns",
            "range": "± 7882100.969433169"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 87452062.6,
            "unit": "ns",
            "range": "± 24966704.626687054"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 296974470.3,
            "unit": "ns",
            "range": "± 9664563.026162522"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 342591421.75,
            "unit": "ns",
            "range": "± 4032041.1945074317"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 82092158.5,
            "unit": "ns",
            "range": "± 11796381.338413255"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 465616644.8,
            "unit": "ns",
            "range": "± 102287266.70149639"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 493091218.4,
            "unit": "ns",
            "range": "± 11008274.43772857"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 83033404.75,
            "unit": "ns",
            "range": "± 4421113.476466336"
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
        "date": 1780802133860,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 22692937.8,
            "unit": "ns",
            "range": "± 1354672.412848287"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 21594570,
            "unit": "ns",
            "range": "± 1212962.165891418"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3038017.75,
            "unit": "ns",
            "range": "± 130632.52384551865"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 23346405.8,
            "unit": "ns",
            "range": "± 552662.1099186193"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 25510455.9,
            "unit": "ns",
            "range": "± 3440108.600855255"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3793952.5,
            "unit": "ns",
            "range": "± 121449.13394092195"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 20338223.25,
            "unit": "ns",
            "range": "± 59707.616177363285"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 21387867.8,
            "unit": "ns",
            "range": "± 965850.570893966"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 3756134.8,
            "unit": "ns",
            "range": "± 780448.5597210748"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24217082.7,
            "unit": "ns",
            "range": "± 886297.0959394485"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24085841.75,
            "unit": "ns",
            "range": "± 334290.12436841446"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 3859505,
            "unit": "ns",
            "range": "± 368991.9456203166"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 240543687.6,
            "unit": "ns",
            "range": "± 9836653.069911754"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 261014321,
            "unit": "ns",
            "range": "± 664923.8925325715"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 72167800,
            "unit": "ns",
            "range": "± 753408.0521209384"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 427429136.4,
            "unit": "ns",
            "range": "± 97353487.73849475"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 369105080.7,
            "unit": "ns",
            "range": "± 11314184.81143795"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 80119854.25,
            "unit": "ns",
            "range": "± 3948979.1416148194"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 286199345.5,
            "unit": "ns",
            "range": "± 12345019.33813721"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 335879256,
            "unit": "ns",
            "range": "± 5516538.396646895"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 82486193,
            "unit": "ns",
            "range": "± 1172173.4760441817"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 527684132.8,
            "unit": "ns",
            "range": "± 111435889.82491541"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 481018498.5,
            "unit": "ns",
            "range": "± 1837044.3827955455"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 86492899,
            "unit": "ns",
            "range": "± 3992745.6806872054"
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
        "date": 1781406979534,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 21508408.25,
            "unit": "ns",
            "range": "± 305879.28439105407"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 22982088.25,
            "unit": "ns",
            "range": "± 361907.2159991425"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3705116.7,
            "unit": "ns",
            "range": "± 935217.3107557409"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 26458912.4,
            "unit": "ns",
            "range": "± 2789518.085830024"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 24330346,
            "unit": "ns",
            "range": "± 529471.0238181122"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 4088963.4,
            "unit": "ns",
            "range": "± 557131.8960141306"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 21649165.25,
            "unit": "ns",
            "range": "± 605828.3860904148"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 22613534.25,
            "unit": "ns",
            "range": "± 676513.011832182"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 3817524.1,
            "unit": "ns",
            "range": "± 717514.5328502413"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 25921399.9,
            "unit": "ns",
            "range": "± 393673.9860141892"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24575691.75,
            "unit": "ns",
            "range": "± 727640.2709358404"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4456831.3,
            "unit": "ns",
            "range": "± 361896.27023734304"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 243299780.6,
            "unit": "ns",
            "range": "± 5992839.611512751"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 253277038,
            "unit": "ns",
            "range": "± 7998074.12244902"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 77120838.5,
            "unit": "ns",
            "range": "± 6389540.985858178"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 390956339.2,
            "unit": "ns",
            "range": "± 81007740.6665117"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 355795413.5,
            "unit": "ns",
            "range": "± 3203708.2472594683"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 83894064.1,
            "unit": "ns",
            "range": "± 3314234.9542493667"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 296488610.5,
            "unit": "ns",
            "range": "± 1841898.6350524288"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 336106182.1,
            "unit": "ns",
            "range": "± 3185508.8758833804"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 80798057,
            "unit": "ns",
            "range": "± 2032383.0362580933"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 500934916.75,
            "unit": "ns",
            "range": "± 108468992.83433981"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 486213021.2,
            "unit": "ns",
            "range": "± 12667794.061361304"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 91307575.7,
            "unit": "ns",
            "range": "± 4963516.12182757"
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
        "date": 1782011809098,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 23026069.5,
            "unit": "ns",
            "range": "± 1687085.065460097"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 23813365,
            "unit": "ns",
            "range": "± 501284.2849601412"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3704347.4,
            "unit": "ns",
            "range": "± 776116.8021105458"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 25313561.25,
            "unit": "ns",
            "range": "± 755852.1646646585"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 24545721.75,
            "unit": "ns",
            "range": "± 681113.937947426"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 4234396.9,
            "unit": "ns",
            "range": "± 504765.8561450051"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 22579322.5,
            "unit": "ns",
            "range": "± 792544.2954378007"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 23323530.25,
            "unit": "ns",
            "range": "± 332360.1158636768"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 3872410.1,
            "unit": "ns",
            "range": "± 954578.2563641914"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 26755091.5,
            "unit": "ns",
            "range": "± 528209.2506286374"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 26259373.2,
            "unit": "ns",
            "range": "± 2074393.540747319"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4354674.5,
            "unit": "ns",
            "range": "± 105214.96485608246"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 242920269.5,
            "unit": "ns",
            "range": "± 7564770.848085122"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 258004228.5,
            "unit": "ns",
            "range": "± 8816586.833642485"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 76736798,
            "unit": "ns",
            "range": "± 1336111.4598258636"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 417566435.1,
            "unit": "ns",
            "range": "± 87201675.18499412"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 352793574.25,
            "unit": "ns",
            "range": "± 3741257.927124367"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 83216033.25,
            "unit": "ns",
            "range": "± 3719778.072970427"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 293025398.5,
            "unit": "ns",
            "range": "± 5463628.592649577"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 345746224.4,
            "unit": "ns",
            "range": "± 5839730.054742711"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 92920459.4,
            "unit": "ns",
            "range": "± 9805498.95887011"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 492657130.8,
            "unit": "ns",
            "range": "± 126172041.34142648"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 487845307.8,
            "unit": "ns",
            "range": "± 7874186.228396919"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 89908371,
            "unit": "ns",
            "range": "± 2359122.630635028"
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
        "date": 1782616471045,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 23319136.4,
            "unit": "ns",
            "range": "± 1863659.4509302927"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 22692280.9,
            "unit": "ns",
            "range": "± 843306.6592549831"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 3823961.2,
            "unit": "ns",
            "range": "± 709370.8733544815"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 25496614,
            "unit": "ns",
            "range": "± 2606549.3114392636"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 23945705.4,
            "unit": "ns",
            "range": "± 2326312.491602386"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 4256120.6,
            "unit": "ns",
            "range": "± 829533.7139979906"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 25478110.25,
            "unit": "ns",
            "range": "± 2329063.1744754333"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24507534.5,
            "unit": "ns",
            "range": "± 2230268.001504752"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4177756.25,
            "unit": "ns",
            "range": "± 701115.3678667418"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24456918,
            "unit": "ns",
            "range": "± 1858981.4383528687"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 25628208.9,
            "unit": "ns",
            "range": "± 1157079.9662021203"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4193985.25,
            "unit": "ns",
            "range": "± 432746.249470653"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 250407997.6,
            "unit": "ns",
            "range": "± 6675301.960181674"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 259244487,
            "unit": "ns",
            "range": "± 1774795.4176235825"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 84648860.1,
            "unit": "ns",
            "range": "± 14306191.946204599"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 386967086.8,
            "unit": "ns",
            "range": "± 117835718.93885243"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 365993067.9,
            "unit": "ns",
            "range": "± 14210649.191306964"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 80230699.75,
            "unit": "ns",
            "range": "± 4788729.840013485"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 289344245,
            "unit": "ns",
            "range": "± 5045504.27416811"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 342602379.8,
            "unit": "ns",
            "range": "± 7473941.799122314"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 84103960,
            "unit": "ns",
            "range": "± 2612870.5397215276"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 516485166,
            "unit": "ns",
            "range": "± 115028623.28376487"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 486530057.4,
            "unit": "ns",
            "range": "± 10573876.145469068"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 94533226.5,
            "unit": "ns",
            "range": "± 3189072.363646154"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2244074+bielu@users.noreply.github.com",
            "name": "Arkadiusz Biel",
            "username": "bielu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4c1c41286838a61f30f4969d936fd07b83df2f62",
          "message": "Merge pull request #51 from bielu/refactor/improve-usage-of-zone-tree\n\nRefactor/improve usage of zone tree",
          "timestamp": "2026-07-04T15:45:47+01:00",
          "tree_id": "e8b724878305c7102648b169779b221092aa0a91",
          "url": "https://github.com/bielu/Bielu.PersistentQueues/commit/4c1c41286838a61f30f4969d936fd07b83df2f62"
        },
        "date": 1783176549116,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 64)",
            "value": 24214829.2,
            "unit": "ns",
            "range": "± 2849040.6573751625"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 24711068.4,
            "unit": "ns",
            "range": "± 2987830.8068333287"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 4202879.1,
            "unit": "ns",
            "range": "± 993719.9292030425"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 25513878.4,
            "unit": "ns",
            "range": "± 2899877.147058096"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 24822550.25,
            "unit": "ns",
            "range": "± 1176502.0217079597"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 64)",
            "value": 4374834.75,
            "unit": "ns",
            "range": "± 699436.1922512422"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 100, MessageDataSize: 512)",
            "value": 23158865.6,
            "unit": "ns",
            "range": "± 1394933.2878336154"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 23363846.6,
            "unit": "ns",
            "range": "± 1325242.5879671238"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4587379.4,
            "unit": "ns",
            "range": "± 1180379.2058916066"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 27911680.25,
            "unit": "ns",
            "range": "± 1952170.514253024"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 24815372.5,
            "unit": "ns",
            "range": "± 537913.2333998362"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 100, MessageDataSize: 512)",
            "value": 4346941.6,
            "unit": "ns",
            "range": "± 528086.9426593505"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 64)",
            "value": 243287379.6,
            "unit": "ns",
            "range": "± 13783769.284112792"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 260940227.25,
            "unit": "ns",
            "range": "± 996186.708523516"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 76084373.25,
            "unit": "ns",
            "range": "± 804571.1332137865"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 439457865.2,
            "unit": "ns",
            "range": "± 98162561.9763074"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 363725979.9,
            "unit": "ns",
            "range": "± 14239675.453961663"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 64)",
            "value": 88833427.2,
            "unit": "ns",
            "range": "± 5245190.485573875"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.Enqueue(MessageCount: 1000, MessageDataSize: 512)",
            "value": 298109813.75,
            "unit": "ns",
            "range": "± 4225724.262990615"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 333232913.75,
            "unit": "ns",
            "range": "± 6039915.5873829"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchReceiveAndAcknowledgeAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 92800349.2,
            "unit": "ns",
            "range": "± 8052413.453212502"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.ReceiveLaterAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 483118704.4,
            "unit": "ns",
            "range": "± 99462342.72319633"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.MoveToAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 490876300.3,
            "unit": "ns",
            "range": "± 7948548.827978772"
          },
          {
            "name": "Bielu.PersistentQueues.Benchmarks.RegressionBenchmark.BatchMixedOperationsAsync(MessageCount: 1000, MessageDataSize: 512)",
            "value": 92432180.75,
            "unit": "ns",
            "range": "± 4459008.335600072"
          }
        ]
      }
    ]
  }
}