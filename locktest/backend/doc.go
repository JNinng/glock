// Package backend 汇集真实后端（Redis、Postgres）的契约测试入口。
// 容器（testcontainers）或本机常驻服务不可用时自动 skip；
// miniredis 套件在无 Docker 环境提供 Redis 语义兜底。
package backend
