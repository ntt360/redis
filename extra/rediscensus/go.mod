module github.com/ntt360/redis/extra/rediscmd/rediscensus/v8

go 1.15

replace github.com/ntt360/redis/v8 => ../..

replace github.com/ntt360/redis/extra/rediscmd/rediscmd/v8 => ../rediscmd

require (
	github.com/ntt360/redis/extra/rediscmd/rediscmd/v8 v8.11.3
	github.com/ntt360/redis/v8 v8.11.3002
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	go.opencensus.io v0.23.0
)
