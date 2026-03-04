# DBLogCDCImplement
Netflix 经典论文《DBLog: A Watermark Based Change-Data-Capture Framework》Go代码模拟

## 代码说明

(注意：Go 的 map 遍历是无序的，所以最终打印的 [全量] k2/k4/k5/k6 顺序可能略有不同，但它们都紧密地夹在高水位 H 的位置上 。)

代码与论文机制的完美对应：无锁交织： 你可以看到，

- 增量事件（UPDATE）一直顺畅地流入 outputChan，哪怕是在开启了清洗窗口期间，遇到普通日志也是立马发往输出端（outputChan <- e）。这就实现了论文里强调的“日志处理不会长时间停滞” 。
- 状态清洗（delete(chunk, e.Key)）： 这是防止时间错乱的绝对核心。因为在这个窗口内 k1 和 k3 更新了，所以我们果断丢弃从数据库里捞出来的、可能已经过时的全量 k1 和 k3 。
- 合并时机： 只有在遇到 H 时，幸存的全量数据（k2, k4, k5, k6）才会被安全地追加到 outputChan 。下游先拿到之前的增量日志，再拿到这批干净的存量，最后继续消费未来的日志，逻辑滴水不漏 。通过几行简单的 Channel 和 Goroutine 逻辑，DBLog 极其巧妙的水印思想就原形毕露了。

