# OpenTelemetry instrumentation for go-redis

## Installation

```bash
go get github.com/ntt360/redis/extra/rediscmd/redisotel/v8
```

## Usage

Tracing is enabled by adding a hook:

```go
import (
    "github.com/ntt360/redis/v8"
    "github.com/ntt360/redis/extra/redisotel"
)

rdb := rdb.NewClient(&rdb.Options{...})

rdb.AddHook(redisotel.NewTracingHook())
```

See [example](example) and [documentation](https://redis.uptrace.dev/tracing/) for more details.
