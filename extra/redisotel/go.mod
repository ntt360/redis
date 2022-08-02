module github.com/ntt360/redis/extra/redisotel/v8

go 1.15

replace github.com/ntt360/redis/v8 => ../..

replace github.com/ntt360/redis/extra/rediscmd/v8 => ../rediscmd

require (
	github.com/ntt360/redis/extra/rediscmd/v8 v8.11.3004
	github.com/ntt360/redis/v8 v8.11.3004
	go.opentelemetry.io/otel v1.0.0-RC2
	go.opentelemetry.io/otel/trace v1.0.0-RC2
)
