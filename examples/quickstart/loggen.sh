#!/bin/sh
# 每秒写 5 条 JSON 日志到 /logs/app.log，模拟两个服务的访问日志。
mkdir -p /logs
while true; do
  awk 'BEGIN {
    srand() # 按当前秒取种子：每秒一批不同的日志
    split("order_api payment_api", svc, " ")
    for (i = 0; i < 5; i++) {
      r = rand(); level = "INFO"; status = 200; msg = "request done"
      if (r > 0.95) { level = "ERROR"; status = 500; msg = "upstream timeout" }
      else if (r > 0.85) { level = "WARN"; status = 429; msg = "rate limited" }
      trace = ""
      for (j = 0; j < 32; j++) trace = trace sprintf("%x", int(rand() * 16))
      printf "{\"level\":\"%s\",\"msg\":\"%s\",\"service_name\":\"%s\",\"trace_id\":\"%s\",\"status\":%d,\"cost_ms\":%d}\n",
        level, msg, svc[int(rand() * 2) + 1], trace, status, int(rand() * 300)
    }
  }' >> /logs/app.log
  sleep 1
done
