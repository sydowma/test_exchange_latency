# test_exchange_latency

## 结论 / Findings（Bybit Spot WS）

以下结论来自你提供的实际跑分输出（`--market spot`，订阅 `tickers.BTCUSDT`）。指标解释与脚本输出一致：`ping_rtt_ms` 代表 RTT，`ticker_latency_ms` 代表基于 Bybit `/v5/market/time` 校时后的单向延迟估计。

### 1) 不同 Region：AWS SG vs AWS Japan

**结论**：**SG 显著更近**（RTT 与 ticker 延迟低一个数量级），更适合延迟敏感策略。

| 指标 | AWS SG | AWS Japan |
|---|---:|---:|
| `time_sync.best_rtt_ms` | 6.327 | 72.234 |
| `ws.connect_ms` | 16.880 | 282.134 |
| `ws.subscribe_ack_ms` | 2.202 | 68.372 |
| `ping_rtt_ms.p50` | 2.083 | 68.582 |
| `ticker_latency_ms.p50` | 4.181 | 35.967 |
| `ticker_latency_ms.p95` | 6.935 | 36.861 |

### 2) 同 Region（SG）不同区：A / B / C 对比

**结论**（综合“常态速度 + 尖刺风险”）：推荐 **SG B区**。  
若你极度关注 **p99 尾部稳定性**，SG C区的 p99 更低，但其 p50 更慢且仍存在大尖刺。

| 指标 | SG A区 | SG B区 | SG C区 |
|---|---:|---:|---:|
| `ws.connect_ms` | 16.880 | **14.286** | 19.270 |
| `ws.subscribe_ack_ms` | 2.202 | **1.451** | 2.277 |
| `ping_rtt_ms.p50` | 2.083 | **1.682** | 2.221 |
| `ticker_latency_ms.p50` | 4.181 | **3.052** | 5.525 |
| `ticker_latency_ms.p99` | 22.685 | 18.292 | **8.474** |
| `ticker_latency_ms.max` | 426.518 | **55.073** | 335.671 |

### 3) 备注（避免误判）

- `ping_interval=10s` 在 60s 里只有 ~6 次 ping，p95/p99 的统计不够稳；建议把 `--ping-interval` 调到 **1-2s** 或把 `--duration` 拉长到 **10-30 分钟**再看尾部。
- SG A区的 `ticker` 计数明显偏少（143 vs 250+），建议同参数再跑一次确认是否偶发。
