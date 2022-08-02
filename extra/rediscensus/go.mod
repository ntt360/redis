module github.com/ntt360/redis/extra/rediscensus/v8

go 1.15

replace github.com/ntt360/redis/v8 => ../..

replace github.com/ntt360/redis/extra/rediscmd/v8 => ../rediscmd

require (
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/ntt360/redis/extra/rediscmd/v8 v8.11.3004
	github.com/ntt360/redis/v8 v8.11.3004
	go.opencensus.io v0.23.0
)
