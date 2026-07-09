
### [mariadb] SINGLE-WORST (per-request, cold, batch=1)

- queries: **200**, mismatches: **0**, wall: **52.772s**, throughput: **4 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 199 misses)
- peak RSS: **706 MB**

| stage | p50 | p99 | mean |
|---|---|---|---|
| lookup (IP→candidates 3-hop, per query) | 5.341ms | 38.922ms | 7.981ms |
| resolve (host→gw, per query) | 22µs | 88µs | 24µs |
| scopedfetch (CH config, per gw) | 14.59ms | 222.231ms | 35.107ms |
| translate (istiod, per gw) | 19.906ms | 110.783ms | 24.729ms |
| check (router_check_tool, per gw) | 155.855ms | 616.507ms | 196.282ms |
| total (per gw batch) | 211.513ms | 778.796ms | 263.844ms |

Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost.


### [postgres] SINGLE-WORST (per-request, cold, batch=1)

- queries: **200**, mismatches: **0**, wall: **47.376s**, throughput: **4 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 199 misses)
- peak RSS: **746 MB**

| stage | p50 | p99 | mean |
|---|---|---|---|
| lookup (IP→candidates 3-hop, per query) | 7.842ms | 21.396ms | 7.235ms |
| resolve (host→gw, per query) | 24µs | 62µs | 25µs |
| scopedfetch (CH config, per gw) | 41.12ms | 112.518ms | 41.25ms |
| translate (istiod, per gw) | 17.031ms | 55.028ms | 19.075ms |
| check (router_check_tool, per gw) | 130.61ms | 480.498ms | 169.564ms |
| total (per gw batch) | 196.259ms | 563.23ms | 236.866ms |

Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost.


### [clickhouse] SINGLE-WORST (per-request, cold, batch=1)

- queries: **200**, mismatches: **0**, wall: **1m8.664s**, throughput: **3 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 199 misses)
- peak RSS: **705 MB**

| stage | p50 | p99 | mean |
|---|---|---|---|
| lookup (IP→candidates 3-hop, per query) | 37.349ms | 138.972ms | 45.252ms |
| resolve (host→gw, per query) | 25µs | 77µs | 31µs |
| scopedfetch (CH config, per gw) | 68.429ms | 185.741ms | 70.701ms |
| translate (istiod, per gw) | 20.071ms | 52.87ms | 23.049ms |
| check (router_check_tool, per gw) | 146.249ms | 634.521ms | 204.72ms |
| total (per gw batch) | 278.063ms | 865.961ms | 343.305ms |

Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost.

