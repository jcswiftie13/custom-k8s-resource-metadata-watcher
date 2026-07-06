
### WARM (cache, steady state)

- queries: **109**, mismatches: **0**, wall: **1.98s**, throughput: **55 q/s** (serialized single replica)
- cache hit rate: **100.0%** (11 hits / 0 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 0s | 0s | 0s |
| check (router_check_tool, per gw) | 177.9ms | 190.861ms | 179.961ms |
| total (per gw batch) | 177.914ms | 190.874ms | 179.978ms |


### COLD (re-translate every gateway)

- queries: **106**, mismatches: **0**, wall: **2.029s**, throughput: **52 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 11 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 0s | 0s | 0s |
| translate (scoped, per gw) | 669µs | 1.468ms | 903µs |
| check (router_check_tool, per gw) | 181.682ms | 185.4ms | 183.452ms |
| total (per gw batch) | 182.376ms | 186.273ms | 184.476ms |

COLD is a worst-case bound, NOT a production mode; WARM is the steady state. COLD-vs-WARM delta = scoped translation cost.


### WARM + churn 0%

- queries: **108**, mismatches: **0**, wall: **2.009s**, throughput: **54 q/s** (serialized single replica)
- cache hit rate: **100.0%** (11 hits / 0 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 0s | 0s | 0s |
| check (router_check_tool, per gw) | 181.359ms | 191.496ms | 182.569ms |
| total (per gw batch) | 181.375ms | 191.51ms | 182.584ms |


### WARM + churn 1%

- queries: **108**, mismatches: **0**, wall: **2.026s**, throughput: **53 q/s** (serialized single replica)
- cache hit rate: **100.0%** (11 hits / 0 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 0s | 0s | 0s |
| check (router_check_tool, per gw) | 185.769ms | 194.91ms | 184.117ms |
| total (per gw batch) | 185.79ms | 194.93ms | 184.134ms |


### WARM + churn 5%

- queries: **108**, mismatches: **0**, wall: **1.927s**, throughput: **56 q/s** (serialized single replica)
- cache hit rate: **100.0%** (11 hits / 0 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 0s | 0s | 0s |
| check (router_check_tool, per gw) | 176.527ms | 186.786ms | 175.192ms |
| total (per gw batch) | 176.545ms | 186.797ms | 175.208ms |


### WARM + churn 25%

- queries: **108**, mismatches: **0**, wall: **2.029s**, throughput: **53 q/s** (serialized single replica)
- cache hit rate: **63.6%** (7 hits / 4 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 0s | 0s | 0s |
| translate (scoped, per gw) | 1.013ms | 1.152ms | 1.112ms |
| check (router_check_tool, per gw) | 187.003ms | 198.186ms | 183.958ms |
| total (per gw batch) | 188.329ms | 198.212ms | 184.442ms |


### WARM + churn 100%

- queries: **108**, mismatches: **0**, wall: **2.051s**, throughput: **53 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 11 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 0s | 0s | 0s |
| translate (scoped, per gw) | 1.221ms | 1.342ms | 1.133ms |
| check (router_check_tool, per gw) | 183.684ms | 199.095ms | 185.136ms |
| total (per gw batch) | 185.138ms | 200.877ms | 186.457ms |


### WARM (cache, steady state)

- queries: **10575**, mismatches: **0**, wall: **20.698s**, throughput: **511 q/s** (serialized single replica)
- cache hit rate: **100.0%** (101 hits / 0 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 1µs | 1µs | 1µs |
| check (router_check_tool, per gw) | 204.929ms | 220.345ms | 204.732ms |
| total (per gw batch) | 205.121ms | 220.433ms | 204.855ms |


### SINGLE-WORST (per-request, cold, batch=1)

- queries: **40**, mismatches: **0**, wall: **7.893s**, throughput: **5 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 39 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 1µs | 5µs | 1µs |
| translate (scoped, per gw) | 1.961ms | 3.853ms | 2.201ms |
| check (router_check_tool, per gw) | 194.587ms | 278.395ms | 194.686ms |
| total (per gw batch) | 196.881ms | 280.495ms | 197.314ms |

Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost.


### WARM (cache, steady state)

- queries: **109**, mismatches: **0**, wall: **1.967s**, throughput: **55 q/s** (serialized single replica)
- cache hit rate: **100.0%** (11 hits / 0 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 0s | 0s | 0s |
| check (router_check_tool, per gw) | 182.983ms | 190.416ms | 178.789ms |
| total (per gw batch) | 183.001ms | 190.43ms | 178.803ms |

BULK/BATCH throughput: one router_check_tool invocation per gateway amortizes tool startup over that gateway's queries. This is the offline route-simulation number, NOT single-request latency — for worst-case online cost run TestResolveSingleWorst.


### SINGLE-WORST (per-request, cold, batch=1)

- queries: **200**, mismatches: **0**, wall: **40.859s**, throughput: **5 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 200 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 5µs | 11µs | 5µs |
| translate (scoped, per gw) | 7.005ms | 10.861ms | 7.144ms |
| check (router_check_tool, per gw) | 195.212ms | 226.281ms | 195.919ms |
| total (per gw batch) | 203.159ms | 234.353ms | 204.288ms |

Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost.


### COLD (re-translate every gateway)

- queries: **3000**, mismatches: **0**, wall: **2m8.41s**, throughput: **23 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 596 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 1µs | 3µs | 1µs |
| translate (scoped, per gw) | 7.222ms | 11.929ms | 6.897ms |
| check (router_check_tool, per gw) | 205.21ms | 282.975ms | 207.362ms |
| total (per gw batch) | 213.024ms | 293.756ms | 215.441ms |

BULK/BATCH throughput: one router_check_tool invocation per gateway amortizes tool startup over that gateway's queries. This is the offline route-simulation number, NOT single-request latency — for worst-case online cost run TestResolveSingleWorst. COLD-vs-WARM delta = scoped translation cost.


### WARM (cache, steady state)

- queries: **109**, mismatches: **0**, wall: **1.934s**, throughput: **56 q/s** (serialized single replica)
- cache hit rate: **100.0%** (11 hits / 0 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 0s | 0s | 0s |
| check (router_check_tool, per gw) | 177.252ms | 181.151ms | 175.814ms |
| total (per gw batch) | 177.265ms | 181.167ms | 175.834ms |

BULK/BATCH throughput: one router_check_tool invocation per gateway amortizes tool startup over that gateway's queries. This is the offline route-simulation number, NOT single-request latency — for worst-case online cost run TestResolveSingleWorst.


### SINGLE-WORST (per-request, cold, batch=1)

- queries: **40**, mismatches: **0**, wall: **7.261s**, throughput: **6 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 39 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 1µs | 3µs | 1µs |
| translate (scoped, per gw) | 2.198ms | 3.982ms | 2.398ms |
| check (router_check_tool, per gw) | 182.027ms | 209.269ms | 178.808ms |
| total (per gw batch) | 185.61ms | 211.19ms | 181.52ms |

Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost.


### WARM (cache, steady state)

- queries: **63353**, mismatches: **0**, wall: **2m16.861s**, throughput: **463 q/s** (serialized single replica)
- cache hit rate: **100.0%** (601 hits / 0 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 1µs | 2µs | 1µs |
| check (router_check_tool, per gw) | 224.8ms | 296.55ms | 227.351ms |
| total (per gw batch) | 225.054ms | 296.747ms | 227.603ms |

BULK/BATCH throughput: one router_check_tool invocation per gateway amortizes tool startup over that gateway's queries. This is the offline route-simulation number, NOT single-request latency — for worst-case online cost run TestResolveSingleWorst.


### SINGLE-WORST (per-request, cold, batch=1)

- queries: **8**, mismatches: **0**, wall: **1.619s**, throughput: **5 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 8 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 1µs | 2µs | 1µs |
| translate (scoped, per gw) | 1.116ms | 3.402ms | 1.637ms |
| check (router_check_tool, per gw) | 192.549ms | 202.874ms | 200.551ms |
| total (per gw batch) | 193.824ms | 204.119ms | 202.295ms |

Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost.


### SINGLE-WORST (per-request, cold, batch=1)

- queries: **200**, mismatches: **0**, wall: **40.668s**, throughput: **5 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 200 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 5µs | 18µs | 6µs |
| translate (scoped, per gw) | 7.364ms | 12.55ms | 6.921ms |
| check (router_check_tool, per gw) | 194.516ms | 221.071ms | 195.22ms |
| total (per gw batch) | 202.984ms | 229.595ms | 203.326ms |

Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost.


### SINGLE-WORST (per-request, cold, batch=1)

- queries: **200**, mismatches: **0**, wall: **40.869s**, throughput: **5 q/s** (serialized single replica)
- cache hit rate: **0.0%** (0 hits / 200 misses)

| stage | p50 | p99 | mean |
|---|---|---|---|
| resolve (host→gw, per query) | 5µs | 14µs | 6µs |
| translate (scoped, per gw) | 7.633ms | 12.065ms | 7.828ms |
| check (router_check_tool, per gw) | 195.244ms | 222.985ms | 195.133ms |
| total (per gw batch) | 204.353ms | 231.051ms | 204.256ms |

Worst-case ONLINE latency: full pipeline per single host+path (resolve + translate + ONE router_check_tool invocation). The total-per-query p50/p99 below is the real single-request cost.

