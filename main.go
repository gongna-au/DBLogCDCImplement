package main

import (
	"fmt"
)

// Event 模拟数据库日志事件
type Event struct {
	Type string // 事件类型："UPDATE" 表示普通更新，"WATERMARK" 表示打水印
	Key  string // 业务主键，如 "k1"；或者是水印值 "L" / "H"
}

func main() {
	// 1. 初始化两个核心数据流 Channel
	// binlogChan 模拟 MySQL 源源不断发来的增量日志
	binlogChan := make(chan Event, 20)
	// outputChan 模拟输出到 Kafka 的最终数据流
	outputChan := make(chan Event, 30)

	// 2. 模拟全量抓取器：获取存量 Chunk，并压入测试日志序列
	// 这是步骤 3 中从数据库 SELECT 出来的全量初始数据 [cite: 245]
	chunk := map[string]bool{
		"k1": true, "k2": true, "k3": true, "k4": true, "k5": true, "k6": true,
	}

	// 按论文图例，向 MySQL 事务日志流中按顺序打入事件 [cite: 246]
	events := []Event{
		{"UPDATE", "k2"}, {"UPDATE", "k3"}, {"UPDATE", "k4"}, {"UPDATE", "k1"}, // 水印前的正常更新
		{"WATERMARK", "L"},                                                     // 步骤 2：生成低水位 [cite: 240]
		{"UPDATE", "k3"}, {"UPDATE", "k1"}, {"UPDATE", "k1"}, {"UPDATE", "k3"}, // L 和 H 之间的业务更新 (导致 k1, k3 被清洗)
		{"WATERMARK", "H"},                                   // 步骤 4：生成高水位 [cite: 243]
		{"UPDATE", "k1"}, {"UPDATE", "k2"}, {"UPDATE", "k6"}, // 水印后的正常更新
	}
	for _, e := range events {
		binlogChan <- e
	}
	close(binlogChan) // 数据发送完毕，关闭源头

	// 3. 启动日志流处理器 (核心逻辑：参考论文 Algorithm 1)
	inWindow := false // 标记当前是否处于 [L, H] 窗口内

	for e := range binlogChan { // 不断从 MySQL 消费日志 [cite: 211, 212]
		if !inWindow {
			// 在窗口外
			if e.Type != "WATERMARK" {
				outputChan <- e // 正常事件，直接输出 [cite: 216]
			} else if e.Key == "L" {
				inWindow = true // 遇到低水位，打开窗口 [cite: 217, 218]
				fmt.Println(">>> 遇到低水位 L，开启清洗窗口！")
			}
		} else {
			// 在窗口内 [L, H] [cite: 213, 214]
			if e.Type != "WATERMARK" {
				// 发生在这个区间的修改，说明内存 Chunk 里的数据旧了，需要清洗剔除！
				if _, exists := chunk[e.Key]; exists {
					delete(chunk, e.Key) // 从内存结果集中移除重叠的行 [cite: 259, 260]
					fmt.Printf("--- 清洗：从 Chunk 中剔除脏数据 %s\n", e.Key)
				}
				outputChan <- e // 增量日志本身是最新的，放入输出流 [cite: 222]

			} else if e.Key == "H" {
				fmt.Println("<<< 遇到高水位 H，窗口关闭！开始合并剩下的 Chunk...")
				// 遇到高水位，说明清洗完毕。把 Chunk 中幸存的存量数据一把推入输出流
				for k := range chunk {
					outputChan <- Event{Type: "CHUNK_DATA", Key: k} // [cite: 224, 225]
				}
				inWindow = false // 关闭窗口 [cite: 191]
			}
		}
	}
	close(outputChan)

	// 4. 模拟下游 (Kafka) 消费最终结果
	fmt.Println("\n=== 最终发送到 Kafka 的数据顺序 ===")
	for e := range outputChan {
		if e.Type == "CHUNK_DATA" {
			fmt.Printf("[全量] %s ", e.Key)
		} else {
			fmt.Printf("[增量] %s ", e.Key)
		}
	}
	fmt.Println()
}
