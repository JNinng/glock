module github.com/jninng/glock/example

go 1.26.2

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/jninng/glock v0.0.0-00010101000000-000000000000
	github.com/jninng/glock/postgrec v0.0.0-00010101000000-000000000000
	github.com/jninng/glock/redisc v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/jninng/glock => ../

replace github.com/jninng/glock/postgrec => ../postgrec

replace github.com/jninng/glock/redisc => ../redisc
