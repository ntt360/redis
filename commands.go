package redis

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ntt360/redis/v8/internal"
)

// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
// For example:
//
//    rdb.Set(ctx, key, value, redis.KeepTTL)
const KeepTTL = -1

func usePrecise(dur time.Duration) bool {
	return dur < time.Second || dur%time.Second != 0
}

func formatMs(ctx context.Context, dur time.Duration) int64 {
	if dur > 0 && dur < time.Millisecond {
		internal.Logger.Printf(
			ctx,
			"specified duration is %s, but minimal supported value is %s - truncating to 1ms",
			dur, time.Millisecond,
		)
		return 1
	}
	return int64(dur / time.Millisecond)
}

func formatSec(ctx context.Context, dur time.Duration) int64 {
	if dur > 0 && dur < time.Second {
		internal.Logger.Printf(
			ctx,
			"specified duration is %s, but minimal supported value is %s - truncating to 1s",
			dur, time.Second,
		)
		return 1
	}
	return int64(dur / time.Second)
}

func appendArgs(dst, src []interface{}) []interface{} {
	if len(src) == 1 {
		return appendArg(dst, src[0])
	}

	dst = append(dst, src...)
	return dst
}

// PATCH
func unwrapCtx(c interface{}) context.Context {
	switch c.(type) {
	case *gin.Context:
		jctx, ok := c.(*gin.Context).Get("traceCtx")
		if ok {
			return jctx.(context.Context)
		}
	case context.Context:
		return c.(context.Context)
	default:
		panic("JaegerContext Only Support *gin.Context or context.Context")
	}

	return context.Background()
}

func appendArg(dst []interface{}, arg interface{}) []interface{} {
	switch arg := arg.(type) {
	case []string:
		for _, s := range arg {
			dst = append(dst, s)
		}
		return dst
	case []interface{}:
		dst = append(dst, arg...)
		return dst
	case map[string]interface{}:
		for k, v := range arg {
			dst = append(dst, k, v)
		}
		return dst
	case map[string]string:
		for k, v := range arg {
			dst = append(dst, k, v)
		}
		return dst
	default:
		return append(dst, arg)
	}
}

type Cmdable interface {
	Pipeline() Pipeliner
	Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error)

	TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error)
	TxPipeline() Pipeliner

	Command(context interface{}) *CommandsInfoCmd
	ClientGetName(context interface{}) *StringCmd
	Echo(context interface{}, message interface{}) *StringCmd
	Ping(context interface{}) *StatusCmd
	Quit(context interface{}) *StatusCmd
	Del(context interface{}, keys ...string) *IntCmd
	Unlink(context interface{}, keys ...string) *IntCmd
	Dump(context interface{}, key string) *StringCmd
	Exists(context interface{}, keys ...string) *IntCmd
	Expire(context interface{}, key string, expiration time.Duration) *BoolCmd
	ExpireAt(context interface{}, key string, tm time.Time) *BoolCmd
	Keys(context interface{}, pattern string) *StringSliceCmd
	Migrate(context interface{}, host, port, key string, db int, timeout time.Duration) *StatusCmd
	Move(context interface{}, key string, db int) *BoolCmd
	ObjectRefCount(context interface{}, key string) *IntCmd
	ObjectEncoding(context interface{}, key string) *StringCmd
	ObjectIdleTime(context interface{}, key string) *DurationCmd
	Persist(context interface{}, key string) *BoolCmd
	PExpire(context interface{}, key string, expiration time.Duration) *BoolCmd
	PExpireAt(context interface{}, key string, tm time.Time) *BoolCmd
	PTTL(context interface{}, key string) *DurationCmd
	RandomKey(context interface{}) *StringCmd
	Rename(context interface{}, key, newkey string) *StatusCmd
	RenameNX(context interface{}, key, newkey string) *BoolCmd
	Restore(context interface{}, key string, ttl time.Duration, value string) *StatusCmd
	RestoreReplace(context interface{}, key string, ttl time.Duration, value string) *StatusCmd
	Sort(context interface{}, key string, sort *Sort) *StringSliceCmd
	SortStore(context interface{}, key, store string, sort *Sort) *IntCmd
	SortInterfaces(context interface{}, key string, sort *Sort) *SliceCmd
	Touch(context interface{}, keys ...string) *IntCmd
	TTL(context interface{}, key string) *DurationCmd
	Type(context interface{}, key string) *StatusCmd
	Append(context interface{}, key, value string) *IntCmd
	Decr(context interface{}, key string) *IntCmd
	DecrBy(context interface{}, key string, decrement int64) *IntCmd
	Get(ctx interface{}, key string) *StringCmd
	GetRange(context interface{}, key string, start, end int64) *StringCmd
	GetSet(context interface{}, key string, value interface{}) *StringCmd
	GetEx(context interface{}, key string, expiration time.Duration) *StringCmd
	GetDel(context interface{}, key string) *StringCmd
	Incr(context interface{}, key string) *IntCmd
	IncrBy(context interface{}, key string, value int64) *IntCmd
	IncrByFloat(context interface{}, key string, value float64) *FloatCmd
	MGet(context interface{}, keys ...string) *SliceCmd
	MSet(context interface{}, values ...interface{}) *StatusCmd
	MSetNX(context interface{}, values ...interface{}) *BoolCmd
	Set(context interface{}, key string, value interface{}, expiration time.Duration) *StatusCmd
	SetArgs(context interface{}, key string, value interface{}, a SetArgs) *StatusCmd
	// TODO: rename to SetEx
	SetEX(context interface{}, key string, value interface{}, expiration time.Duration) *StatusCmd
	SetNX(context interface{}, key string, value interface{}, expiration time.Duration) *BoolCmd
	SetXX(context interface{}, key string, value interface{}, expiration time.Duration) *BoolCmd
	SetRange(context interface{}, key string, offset int64, value string) *IntCmd
	StrLen(context interface{}, key string) *IntCmd

	GetBit(context interface{}, key string, offset int64) *IntCmd
	SetBit(context interface{}, key string, offset int64, value int) *IntCmd
	BitCount(context interface{}, key string, bitCount *BitCount) *IntCmd
	BitOpAnd(context interface{}, destKey string, keys ...string) *IntCmd
	BitOpOr(context interface{}, destKey string, keys ...string) *IntCmd
	BitOpXor(context interface{}, destKey string, keys ...string) *IntCmd
	BitOpNot(context interface{}, destKey string, key string) *IntCmd
	BitPos(context interface{}, key string, bit int64, pos ...int64) *IntCmd
	BitField(context interface{}, key string, args ...interface{}) *IntSliceCmd

	Scan(context interface{}, cursor uint64, match string, count int64) *ScanCmd
	ScanType(context interface{}, cursor uint64, match string, count int64, keyType string) *ScanCmd
	SScan(context interface{}, key string, cursor uint64, match string, count int64) *ScanCmd
	HScan(context interface{}, key string, cursor uint64, match string, count int64) *ScanCmd
	ZScan(context interface{}, key string, cursor uint64, match string, count int64) *ScanCmd

	HDel(context interface{}, key string, fields ...string) *IntCmd
	HExists(context interface{}, key, field string) *BoolCmd
	HGet(context interface{}, key, field string) *StringCmd
	HGetAll(context interface{}, key string) *StringStringMapCmd
	HIncrBy(context interface{}, key, field string, incr int64) *IntCmd
	HIncrByFloat(context interface{}, key, field string, incr float64) *FloatCmd
	HKeys(context interface{}, key string) *StringSliceCmd
	HLen(context interface{}, key string) *IntCmd
	HMGet(context interface{}, key string, fields ...string) *SliceCmd
	HSet(context interface{}, key string, values ...interface{}) *IntCmd
	HMSet(context interface{}, key string, values ...interface{}) *BoolCmd
	HSetNX(context interface{}, key, field string, value interface{}) *BoolCmd
	HVals(context interface{}, key string) *StringSliceCmd
	HRandField(context interface{}, key string, count int, withValues bool) *StringSliceCmd

	BLPop(context interface{}, timeout time.Duration, keys ...string) *StringSliceCmd
	BRPop(context interface{}, timeout time.Duration, keys ...string) *StringSliceCmd
	BRPopLPush(context interface{}, source, destination string, timeout time.Duration) *StringCmd
	LIndex(context interface{}, key string, index int64) *StringCmd
	LInsert(context interface{}, key, op string, pivot, value interface{}) *IntCmd
	LInsertBefore(context interface{}, key string, pivot, value interface{}) *IntCmd
	LInsertAfter(context interface{}, key string, pivot, value interface{}) *IntCmd
	LLen(context interface{}, key string) *IntCmd
	LPop(context interface{}, key string) *StringCmd
	LPopCount(context interface{}, key string, count int) *StringSliceCmd
	LPos(context interface{}, key string, value string, args LPosArgs) *IntCmd
	LPosCount(context interface{}, key string, value string, count int64, args LPosArgs) *IntSliceCmd
	LPush(context interface{}, key string, values ...interface{}) *IntCmd
	LPushX(context interface{}, key string, values ...interface{}) *IntCmd
	LRange(context interface{}, key string, start, stop int64) *StringSliceCmd
	LRem(context interface{}, key string, count int64, value interface{}) *IntCmd
	LSet(context interface{}, key string, index int64, value interface{}) *StatusCmd
	LTrim(context interface{}, key string, start, stop int64) *StatusCmd
	RPop(context interface{}, key string) *StringCmd
	RPopCount(context interface{}, key string, count int) *StringSliceCmd
	RPopLPush(context interface{}, source, destination string) *StringCmd
	RPush(context interface{}, key string, values ...interface{}) *IntCmd
	RPushX(context interface{}, key string, values ...interface{}) *IntCmd
	LMove(context interface{}, source, destination, srcpos, destpos string) *StringCmd

	SAdd(context interface{}, key string, members ...interface{}) *IntCmd
	SCard(context interface{}, key string) *IntCmd
	SDiff(context interface{}, keys ...string) *StringSliceCmd
	SDiffStore(context interface{}, destination string, keys ...string) *IntCmd
	SInter(context interface{}, keys ...string) *StringSliceCmd
	SInterStore(context interface{}, destination string, keys ...string) *IntCmd
	SIsMember(context interface{}, key string, member interface{}) *BoolCmd
	SMIsMember(context interface{}, key string, members ...interface{}) *BoolSliceCmd
	SMembers(context interface{}, key string) *StringSliceCmd
	SMembersMap(context interface{}, key string) *StringStructMapCmd
	SMove(context interface{}, source, destination string, member interface{}) *BoolCmd
	SPop(context interface{}, key string) *StringCmd
	SPopN(context interface{}, key string, count int64) *StringSliceCmd
	SRandMember(context interface{}, key string) *StringCmd
	SRandMemberN(context interface{}, key string, count int64) *StringSliceCmd
	SRem(context interface{}, key string, members ...interface{}) *IntCmd
	SUnion(context interface{}, keys ...string) *StringSliceCmd
	SUnionStore(context interface{}, destination string, keys ...string) *IntCmd

	XAdd(context interface{}, a *XAddArgs) *StringCmd
	XDel(context interface{}, stream string, ids ...string) *IntCmd
	XLen(context interface{}, stream string) *IntCmd
	XRange(context interface{}, stream, start, stop string) *XMessageSliceCmd
	XRangeN(context interface{}, stream, start, stop string, count int64) *XMessageSliceCmd
	XRevRange(context interface{}, stream string, start, stop string) *XMessageSliceCmd
	XRevRangeN(context interface{}, stream string, start, stop string, count int64) *XMessageSliceCmd
	XRead(context interface{}, a *XReadArgs) *XStreamSliceCmd
	XReadStreams(context interface{}, streams ...string) *XStreamSliceCmd
	XGroupCreate(context interface{}, stream, group, start string) *StatusCmd
	XGroupCreateMkStream(context interface{}, stream, group, start string) *StatusCmd
	XGroupSetID(context interface{}, stream, group, start string) *StatusCmd
	XGroupDestroy(context interface{}, stream, group string) *IntCmd
	XGroupCreateConsumer(context interface{}, stream, group, consumer string) *IntCmd
	XGroupDelConsumer(context interface{}, stream, group, consumer string) *IntCmd
	XReadGroup(context interface{}, a *XReadGroupArgs) *XStreamSliceCmd
	XAck(context interface{}, stream, group string, ids ...string) *IntCmd
	XPending(context interface{}, stream, group string) *XPendingCmd
	XPendingExt(context interface{}, a *XPendingExtArgs) *XPendingExtCmd
	XClaim(context interface{}, a *XClaimArgs) *XMessageSliceCmd
	XClaimJustID(context interface{}, a *XClaimArgs) *StringSliceCmd
	XAutoClaim(context interface{}, a *XAutoClaimArgs) *XAutoClaimCmd
	XAutoClaimJustID(context interface{}, a *XAutoClaimArgs) *XAutoClaimJustIDCmd

	// TODO: XTrim and XTrimApprox remove in v9.
	XTrim(context interface{}, key string, maxLen int64) *IntCmd
	XTrimApprox(context interface{}, key string, maxLen int64) *IntCmd
	XTrimMaxLen(context interface{}, key string, maxLen int64) *IntCmd
	XTrimMaxLenApprox(context interface{}, key string, maxLen, limit int64) *IntCmd
	XTrimMinID(context interface{}, key string, minID string) *IntCmd
	XTrimMinIDApprox(context interface{}, key string, minID string, limit int64) *IntCmd
	XInfoGroups(context interface{}, key string) *XInfoGroupsCmd
	XInfoStream(context interface{}, key string) *XInfoStreamCmd
	XInfoStreamFull(context interface{}, key string, count int) *XInfoStreamFullCmd
	XInfoConsumers(context interface{}, key string, group string) *XInfoConsumersCmd

	BZPopMax(context interface{}, timeout time.Duration, keys ...string) *ZWithKeyCmd
	BZPopMin(context interface{}, timeout time.Duration, keys ...string) *ZWithKeyCmd

	// TODO: remove
	//		ZAddCh
	//		ZIncr
	//		ZAddNXCh
	//		ZAddXXCh
	//		ZIncrNX
	//		ZIncrXX
	// 	in v9.
	// 	use ZAddArgs and ZAddArgsIncr.

	ZAdd(context interface{}, key string, members ...*Z) *IntCmd
	ZAddNX(context interface{}, key string, members ...*Z) *IntCmd
	ZAddXX(context interface{}, key string, members ...*Z) *IntCmd
	ZAddCh(context interface{}, key string, members ...*Z) *IntCmd
	ZAddNXCh(context interface{}, key string, members ...*Z) *IntCmd
	ZAddXXCh(context interface{}, key string, members ...*Z) *IntCmd
	ZAddArgs(context interface{}, key string, args ZAddArgs) *IntCmd
	ZAddArgsIncr(context interface{}, key string, args ZAddArgs) *FloatCmd
	ZIncr(context interface{}, key string, member *Z) *FloatCmd
	ZIncrNX(context interface{}, key string, member *Z) *FloatCmd
	ZIncrXX(context interface{}, key string, member *Z) *FloatCmd
	ZCard(context interface{}, key string) *IntCmd
	ZCount(context interface{}, key, min, max string) *IntCmd
	ZLexCount(context interface{}, key, min, max string) *IntCmd
	ZIncrBy(context interface{}, key string, increment float64, member string) *FloatCmd
	ZInter(context interface{}, store *ZStore) *StringSliceCmd
	ZInterWithScores(context interface{}, store *ZStore) *ZSliceCmd
	ZInterStore(context interface{}, destination string, store *ZStore) *IntCmd
	ZMScore(context interface{}, key string, members ...string) *FloatSliceCmd
	ZPopMax(context interface{}, key string, count ...int64) *ZSliceCmd
	ZPopMin(context interface{}, key string, count ...int64) *ZSliceCmd
	ZRange(context interface{}, key string, start, stop int64) *StringSliceCmd
	ZRangeWithScores(context interface{}, key string, start, stop int64) *ZSliceCmd
	ZRangeByScore(context interface{}, key string, opt *ZRangeBy) *StringSliceCmd
	ZRangeByLex(context interface{}, key string, opt *ZRangeBy) *StringSliceCmd
	ZRangeByScoreWithScores(context interface{}, key string, opt *ZRangeBy) *ZSliceCmd
	ZRangeArgs(context interface{}, z ZRangeArgs) *StringSliceCmd
	ZRangeArgsWithScores(context interface{}, z ZRangeArgs) *ZSliceCmd
	ZRangeStore(context interface{}, dst string, z ZRangeArgs) *IntCmd
	ZRank(context interface{}, key, member string) *IntCmd
	ZRem(context interface{}, key string, members ...interface{}) *IntCmd
	ZRemRangeByRank(context interface{}, key string, start, stop int64) *IntCmd
	ZRemRangeByScore(context interface{}, key, min, max string) *IntCmd
	ZRemRangeByLex(context interface{}, key, min, max string) *IntCmd
	ZRevRange(context interface{}, key string, start, stop int64) *StringSliceCmd
	ZRevRangeWithScores(context interface{}, key string, start, stop int64) *ZSliceCmd
	ZRevRangeByScore(context interface{}, key string, opt *ZRangeBy) *StringSliceCmd
	ZRevRangeByLex(context interface{}, key string, opt *ZRangeBy) *StringSliceCmd
	ZRevRangeByScoreWithScores(context interface{}, key string, opt *ZRangeBy) *ZSliceCmd
	ZRevRank(context interface{}, key, member string) *IntCmd
	ZScore(context interface{}, key, member string) *FloatCmd
	ZUnionStore(context interface{}, dest string, store *ZStore) *IntCmd
	ZUnion(context interface{}, store ZStore) *StringSliceCmd
	ZUnionWithScores(context interface{}, store ZStore) *ZSliceCmd
	ZRandMember(context interface{}, key string, count int, withScores bool) *StringSliceCmd
	ZDiff(context interface{}, keys ...string) *StringSliceCmd
	ZDiffWithScores(context interface{}, keys ...string) *ZSliceCmd
	ZDiffStore(context interface{}, destination string, keys ...string) *IntCmd

	PFAdd(context interface{}, key string, els ...interface{}) *IntCmd
	PFCount(context interface{}, keys ...string) *IntCmd
	PFMerge(context interface{}, dest string, keys ...string) *StatusCmd

	BgRewriteAOF(context interface{}) *StatusCmd
	BgSave(context interface{}) *StatusCmd
	ClientKill(context interface{}, ipPort string) *StatusCmd
	ClientKillByFilter(context interface{}, keys ...string) *IntCmd
	ClientList(context interface{}) *StringCmd
	ClientPause(context interface{}, dur time.Duration) *BoolCmd
	ClientID(context interface{}) *IntCmd
	ConfigGet(context interface{}, parameter string) *SliceCmd
	ConfigResetStat(context interface{}) *StatusCmd
	ConfigSet(context interface{}, parameter, value string) *StatusCmd
	ConfigRewrite(context interface{}) *StatusCmd
	DBSize(context interface{}) *IntCmd
	FlushAll(context interface{}) *StatusCmd
	FlushAllAsync(context interface{}) *StatusCmd
	FlushDB(context interface{}) *StatusCmd
	FlushDBAsync(context interface{}) *StatusCmd
	Info(context interface{}, section ...string) *StringCmd
	LastSave(context interface{}) *IntCmd
	Save(context interface{}) *StatusCmd
	Shutdown(context interface{}) *StatusCmd
	ShutdownSave(context interface{}) *StatusCmd
	ShutdownNoSave(context interface{}) *StatusCmd
	SlaveOf(context interface{}, host, port string) *StatusCmd
	Time(context interface{}) *TimeCmd
	DebugObject(context interface{}, key string) *StringCmd
	ReadOnly(context interface{}) *StatusCmd
	ReadWrite(context interface{}) *StatusCmd
	MemoryUsage(context interface{}, key string, samples ...int) *IntCmd

	Eval(context interface{}, script string, keys []string, args ...interface{}) *Cmd
	EvalSha(context interface{}, sha1 string, keys []string, args ...interface{}) *Cmd
	ScriptExists(context interface{}, hashes ...string) *BoolSliceCmd
	ScriptFlush(context interface{}) *StatusCmd
	ScriptKill(context interface{}) *StatusCmd
	ScriptLoad(context interface{}, script string) *StringCmd

	Publish(context interface{}, channel string, message interface{}) *IntCmd
	PubSubChannels(context interface{}, pattern string) *StringSliceCmd
	PubSubNumSub(context interface{}, channels ...string) *StringIntMapCmd
	PubSubNumPat(context interface{}) *IntCmd

	ClusterSlots(context interface{}) *ClusterSlotsCmd
	ClusterNodes(context interface{}) *StringCmd
	ClusterMeet(context interface{}, host, port string) *StatusCmd
	ClusterForget(context interface{}, nodeID string) *StatusCmd
	ClusterReplicate(context interface{}, nodeID string) *StatusCmd
	ClusterResetSoft(context interface{}) *StatusCmd
	ClusterResetHard(context interface{}) *StatusCmd
	ClusterInfo(context interface{}) *StringCmd
	ClusterKeySlot(context interface{}, key string) *IntCmd
	ClusterGetKeysInSlot(context interface{}, slot int, count int) *StringSliceCmd
	ClusterCountFailureReports(context interface{}, nodeID string) *IntCmd
	ClusterCountKeysInSlot(context interface{}, slot int) *IntCmd
	ClusterDelSlots(context interface{}, slots ...int) *StatusCmd
	ClusterDelSlotsRange(context interface{}, min, max int) *StatusCmd
	ClusterSaveConfig(context interface{}) *StatusCmd
	ClusterSlaves(context interface{}, nodeID string) *StringSliceCmd
	ClusterFailover(context interface{}) *StatusCmd
	ClusterAddSlots(context interface{}, slots ...int) *StatusCmd
	ClusterAddSlotsRange(context interface{}, min, max int) *StatusCmd

	GeoAdd(context interface{}, key string, geoLocation ...*GeoLocation) *IntCmd
	GeoPos(context interface{}, key string, members ...string) *GeoPosCmd
	GeoRadius(context interface{}, key string, longitude, latitude float64, query *GeoRadiusQuery) *GeoLocationCmd
	GeoRadiusStore(context interface{}, key string, longitude, latitude float64, query *GeoRadiusQuery) *IntCmd
	GeoRadiusByMember(context interface{}, key, member string, query *GeoRadiusQuery) *GeoLocationCmd
	GeoRadiusByMemberStore(context interface{}, key, member string, query *GeoRadiusQuery) *IntCmd
	GeoSearch(context interface{}, key string, q *GeoSearchQuery) *StringSliceCmd
	GeoSearchLocation(context interface{}, key string, q *GeoSearchLocationQuery) *GeoSearchLocationCmd
	GeoSearchStore(context interface{}, key, store string, q *GeoSearchStoreQuery) *IntCmd
	GeoDist(context interface{}, key string, member1, member2, unit string) *FloatCmd
	GeoHash(context interface{}, key string, members ...string) *StringSliceCmd
}

type StatefulCmdable interface {
	Cmdable
	Auth(ctx context.Context, password string) *StatusCmd
	AuthACL(ctx context.Context, username, password string) *StatusCmd
	Select(ctx context.Context, index int) *StatusCmd
	SwapDB(ctx context.Context, index1, index2 int) *StatusCmd
	ClientSetName(ctx context.Context, name string) *BoolCmd
}

var (
	_ Cmdable = (*Client)(nil)
	_ Cmdable = (*Tx)(nil)
	_ Cmdable = (*Ring)(nil)
	_ Cmdable = (*ClusterClient)(nil)
)

type cmdable func(ctx context.Context, cmd Cmder) error

type statefulCmdable func(ctx context.Context, cmd Cmder) error

//------------------------------------------------------------------------------

func (c statefulCmdable) Auth(ctx context.Context, password string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "auth", password)
	_ = c(ctx, cmd)
	return cmd
}

// AuthACL Perform an AUTH command, using the given user and pass.
// Should be used to authenticate the current connection with one of the connections defined in the ACL list
// when connecting to a Redis 6.0 instance, or greater, that is using the Redis ACL system.
func (c statefulCmdable) AuthACL(ctx context.Context, username, password string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "auth", username, password)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Wait(ctx context.Context, numSlaves int, timeout time.Duration) *IntCmd {
	cmd := NewIntCmd(ctx, "wait", numSlaves, int(timeout/time.Millisecond))
	_ = c(ctx, cmd)
	return cmd
}

func (c statefulCmdable) Select(ctx context.Context, index int) *StatusCmd {
	cmd := NewStatusCmd(ctx, "select", index)
	_ = c(ctx, cmd)
	return cmd
}

func (c statefulCmdable) SwapDB(ctx context.Context, index1, index2 int) *StatusCmd {
	cmd := NewStatusCmd(ctx, "swapdb", index1, index2)
	_ = c(ctx, cmd)
	return cmd
}

// ClientSetName assigns a name to the connection.
func (c statefulCmdable) ClientSetName(ctx context.Context, name string) *BoolCmd {
	cmd := NewBoolCmd(ctx, "client", "setname", name)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) Command(context interface{}) *CommandsInfoCmd {
	ctx := unwrapCtx(context)
	cmd := NewCommandsInfoCmd(ctx, "command")
	_ = c(ctx, cmd)
	return cmd
}

// ClientGetName returns the name of the connection.
func (c cmdable) ClientGetName(context interface{}) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "client", "getname")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Echo(context interface{}, message interface{}) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "echo", message)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Ping(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "ping")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Quit(ctx interface{}) *StatusCmd {
	panic("not implemented")
}

func (c cmdable) Del(context interface{}, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys))
	args[0] = "del"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Unlink(context interface{}, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys))
	args[0] = "unlink"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Dump(context interface{}, key string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "dump", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Exists(context interface{}, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys))
	args[0] = "exists"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Expire(context interface{}, key string, expiration time.Duration) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "expire", key, formatSec(ctx, expiration))
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ExpireAt(context interface{}, key string, tm time.Time) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "expireat", key, tm.Unix())
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Keys(context interface{}, pattern string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "keys", pattern)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Migrate(context interface{}, host, port, key string, db int, timeout time.Duration) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(
		ctx,
		"migrate",
		host,
		port,
		key,
		db,
		formatMs(ctx, timeout),
	)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Move(context interface{}, key string, db int) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "move", key, db)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ObjectRefCount(context interface{}, key string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "object", "refcount", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ObjectEncoding(context interface{}, key string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "object", "encoding", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ObjectIdleTime(context interface{}, key string) *DurationCmd {
	ctx := unwrapCtx(context)
	cmd := NewDurationCmd(ctx, time.Second, "object", "idletime", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Persist(context interface{}, key string) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "persist", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) PExpire(context interface{}, key string, expiration time.Duration) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "pexpire", key, formatMs(ctx, expiration))
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) PExpireAt(context interface{}, key string, tm time.Time) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(
		ctx,
		"pexpireat",
		key,
		tm.UnixNano()/int64(time.Millisecond),
	)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) PTTL(context interface{}, key string) *DurationCmd {
	ctx := unwrapCtx(context)
	cmd := NewDurationCmd(ctx, time.Millisecond, "pttl", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RandomKey(context interface{}) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "randomkey")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Rename(context interface{}, key, newkey string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "rename", key, newkey)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RenameNX(context interface{}, key, newkey string) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "renamenx", key, newkey)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Restore(context interface{}, key string, ttl time.Duration, value string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(
		ctx,
		"restore",
		key,
		formatMs(ctx, ttl),
		value,
	)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RestoreReplace(context interface{}, key string, ttl time.Duration, value string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(
		ctx,
		"restore",
		key,
		formatMs(ctx, ttl),
		value,
		"replace",
	)
	_ = c(ctx, cmd)
	return cmd
}

type Sort struct {
	By            string
	Offset, Count int64
	Get           []string
	Order         string
	Alpha         bool
}

func (sort *Sort) args(key string) []interface{} {
	args := []interface{}{"sort", key}
	if sort.By != "" {
		args = append(args, "by", sort.By)
	}
	if sort.Offset != 0 || sort.Count != 0 {
		args = append(args, "limit", sort.Offset, sort.Count)
	}
	for _, get := range sort.Get {
		args = append(args, "get", get)
	}
	if sort.Order != "" {
		args = append(args, sort.Order)
	}
	if sort.Alpha {
		args = append(args, "alpha")
	}
	return args
}

func (c cmdable) Sort(context interface{}, key string, sort *Sort) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, sort.args(key)...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SortStore(context interface{}, key, store string, sort *Sort) *IntCmd {
	ctx := unwrapCtx(context)
	args := sort.args(key)
	if store != "" {
		args = append(args, "store", store)
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SortInterfaces(context interface{}, key string, sort *Sort) *SliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewSliceCmd(ctx, sort.args(key)...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Touch(context interface{}, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, len(keys)+1)
	args[0] = "touch"
	for i, key := range keys {
		args[i+1] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TTL(context interface{}, key string) *DurationCmd {
	ctx := unwrapCtx(context)
	cmd := NewDurationCmd(ctx, time.Second, "ttl", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Type(context interface{}, key string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "type", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Append(context interface{}, key, value string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "append", key, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Decr(context interface{}, key string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "decr", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) DecrBy(context interface{}, key string, decrement int64) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "decrby", key, decrement)
	_ = c(ctx, cmd)
	return cmd
}

// Get Redis `GET key` command. It returns redis.Nil error when key does not exist.
func (c cmdable) Get(context interface{}, key string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "get", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) GetRange(context interface{}, key string, start, end int64) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "getrange", key, start, end)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) GetSet(context interface{}, key string, value interface{}) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "getset", key, value)
	_ = c(ctx, cmd)
	return cmd
}

// GetEx An expiration of zero removes the TTL associated with the key (i.e. GETEX key persist).
// Requires Redis >= 6.2.0.
func (c cmdable) GetEx(context interface{}, key string, expiration time.Duration) *StringCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 4)
	args = append(args, "getex", key)
	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == 0 {
		args = append(args, "persist")
	}

	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// GetDel redis-server version >= 6.2.0.
func (c cmdable) GetDel(context interface{}, key string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "getdel", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Incr(context interface{}, key string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "incr", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) IncrBy(context interface{}, key string, value int64) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "incrby", key, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) IncrByFloat(context interface{}, key string, value float64) *FloatCmd {
	ctx := unwrapCtx(context)
	cmd := NewFloatCmd(ctx, "incrbyfloat", key, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) MGet(context interface{}, keys ...string) *SliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys))
	args[0] = "mget"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// MSet is like Set but accepts multiple values:
//   - MSet("key1", "value1", "key2", "value2")
//   - MSet([]string{"key1", "value1", "key2", "value2"})
//   - MSet(map[string]interface{}{"key1": "value1", "key2": "value2"})
func (c cmdable) MSet(context interface{}, values ...interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1, 1+len(values))
	args[0] = "mset"
	args = appendArgs(args, values)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// MSetNX is like SetNX but accepts multiple values:
//   - MSetNX("key1", "value1", "key2", "value2")
//   - MSetNX([]string{"key1", "value1", "key2", "value2"})
//   - MSetNX(map[string]interface{}{"key1": "value1", "key2": "value2"})
func (c cmdable) MSetNX(context interface{}, values ...interface{}) *BoolCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1, 1+len(values))
	args[0] = "msetnx"
	args = appendArgs(args, values)
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// Set Redis `SET key value [expiration]` command.
// Use expiration for `SETEX`-like behavior.
//
// Zero expiration means the key has no expiration time.
// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
func (c cmdable) Set(context interface{}, key string, value interface{}, expiration time.Duration) *StatusCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 3, 5)
	args[0] = "set"
	args[1] = key
	args[2] = value
	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SetArgs provides arguments for the SetArgs function.
type SetArgs struct {
	// Mode can be `NX` or `XX` or empty.
	Mode string

	// Zero `TTL` or `Expiration` means that the key has no expiration time.
	TTL      time.Duration
	ExpireAt time.Time

	// When Get is true, the command returns the old value stored at key, or nil when key did not exist.
	Get bool

	// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
	// otherwise you will receive an error: (error) ERR syntax error.
	KeepTTL bool
}

// SetArgs supports all the options that the SET command supports.
// It is the alternative to the Set function when you want
// to have more control over the options.
func (c cmdable) SetArgs(context interface{}, key string, value interface{}, a SetArgs) *StatusCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"set", key, value}

	if a.KeepTTL {
		args = append(args, "keepttl")
	}

	if !a.ExpireAt.IsZero() {
		args = append(args, "exat", a.ExpireAt.Unix())
	}
	if a.TTL > 0 {
		if usePrecise(a.TTL) {
			args = append(args, "px", formatMs(ctx, a.TTL))
		} else {
			args = append(args, "ex", formatSec(ctx, a.TTL))
		}
	}

	if a.Mode != "" {
		args = append(args, a.Mode)
	}

	if a.Get {
		args = append(args, "get")
	}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SetEX Redis `SETEX key expiration value` command.
func (c cmdable) SetEX(context interface{}, key string, value interface{}, expiration time.Duration) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "setex", key, formatSec(ctx, expiration), value)
	_ = c(ctx, cmd)
	return cmd
}

// SetNX Redis `SET key value [expiration] NX` command.
//
// Zero expiration means the key has no expiration time.
// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
func (c cmdable) SetNX(context interface{}, key string, value interface{}, expiration time.Duration) *BoolCmd {
	ctx := unwrapCtx(context)
	var cmd *BoolCmd
	switch expiration {
	case 0:
		// Use old `SETNX` to support old Redis versions.
		cmd = NewBoolCmd(ctx, "setnx", key, value)
	case KeepTTL:
		cmd = NewBoolCmd(ctx, "set", key, value, "keepttl", "nx")
	default:
		if usePrecise(expiration) {
			cmd = NewBoolCmd(ctx, "set", key, value, "px", formatMs(ctx, expiration), "nx")
		} else {
			cmd = NewBoolCmd(ctx, "set", key, value, "ex", formatSec(ctx, expiration), "nx")
		}
	}

	_ = c(ctx, cmd)
	return cmd
}

// SetXX Redis `SET key value [expiration] XX` command.
//
// Zero expiration means the key has no expiration time.
// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
func (c cmdable) SetXX(context interface{}, key string, value interface{}, expiration time.Duration) *BoolCmd {
	ctx := unwrapCtx(context)
	var cmd *BoolCmd
	switch expiration {
	case 0:
		cmd = NewBoolCmd(ctx, "set", key, value, "xx")
	case KeepTTL:
		cmd = NewBoolCmd(ctx, "set", key, value, "keepttl", "xx")
	default:
		if usePrecise(expiration) {
			cmd = NewBoolCmd(ctx, "set", key, value, "px", formatMs(ctx, expiration), "xx")
		} else {
			cmd = NewBoolCmd(ctx, "set", key, value, "ex", formatSec(ctx, expiration), "xx")
		}
	}

	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SetRange(context interface{}, key string, offset int64, value string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "setrange", key, offset, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) StrLen(context interface{}, key string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "strlen", key)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) GetBit(context interface{}, key string, offset int64) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "getbit", key, offset)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SetBit(context interface{}, key string, offset int64, value int) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(
		ctx,
		"setbit",
		key,
		offset,
		value,
	)
	_ = c(ctx, cmd)
	return cmd
}

type BitCount struct {
	Start, End int64
}

func (c cmdable) BitCount(context interface{}, key string, bitCount *BitCount) *IntCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"bitcount", key}
	if bitCount != nil {
		args = append(
			args,
			bitCount.Start,
			bitCount.End,
		)
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) bitOp(ctx context.Context, op, destKey string, keys ...string) *IntCmd {
	args := make([]interface{}, 3+len(keys))
	args[0] = "bitop"
	args[1] = op
	args[2] = destKey
	for i, key := range keys {
		args[3+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BitOpAnd(context interface{}, destKey string, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	return c.bitOp(ctx, "and", destKey, keys...)
}

func (c cmdable) BitOpOr(context interface{}, destKey string, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	return c.bitOp(ctx, "or", destKey, keys...)
}

func (c cmdable) BitOpXor(context interface{}, destKey string, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	return c.bitOp(ctx, "xor", destKey, keys...)
}

func (c cmdable) BitOpNot(context interface{}, destKey string, key string) *IntCmd {
	ctx := unwrapCtx(context)
	return c.bitOp(ctx, "not", destKey, key)
}

func (c cmdable) BitPos(context interface{}, key string, bit int64, pos ...int64) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 3+len(pos))
	args[0] = "bitpos"
	args[1] = key
	args[2] = bit
	switch len(pos) {
	case 0:
	case 1:
		args[3] = pos[0]
	case 2:
		args[3] = pos[0]
		args[4] = pos[1]
	default:
		panic("too many arguments")
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BitField(context interface{}, key string, args ...interface{}) *IntSliceCmd {
	ctx := unwrapCtx(context)
	a := make([]interface{}, 0, 2+len(args))
	a = append(a, "bitfield")
	a = append(a, key)
	a = append(a, args...)
	cmd := NewIntSliceCmd(ctx, a...)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) Scan(context interface{}, cursor uint64, match string, count int64) *ScanCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"scan", cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(ctx, c, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ScanType(context interface{}, cursor uint64, match string, count int64, keyType string) *ScanCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"scan", cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	if keyType != "" {
		args = append(args, "type", keyType)
	}
	cmd := NewScanCmd(ctx, c, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SScan(context interface{}, key string, cursor uint64, match string, count int64) *ScanCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"sscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(ctx, c, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HScan(context interface{}, key string, cursor uint64, match string, count int64) *ScanCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"hscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(ctx, c, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZScan(context interface{}, key string, cursor uint64, match string, count int64) *ScanCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"zscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(ctx, c, args...)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) HDel(context interface{}, key string, fields ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(fields))
	args[0] = "hdel"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HExists(context interface{}, key, field string) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "hexists", key, field)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HGet(context interface{}, key, field string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "hget", key, field)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HGetAll(context interface{}, key string) *StringStringMapCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringStringMapCmd(ctx, "hgetall", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HIncrBy(context interface{}, key, field string, incr int64) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "hincrby", key, field, incr)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HIncrByFloat(context interface{}, key, field string, incr float64) *FloatCmd {
	ctx := unwrapCtx(context)
	cmd := NewFloatCmd(ctx, "hincrbyfloat", key, field, incr)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HKeys(context interface{}, key string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "hkeys", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HLen(context interface{}, key string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "hlen", key)
	_ = c(ctx, cmd)
	return cmd
}

// HMGet returns the values for the specified fields in the hash stored at key.
// It returns an interface{} to distinguish between empty string and nil value.
func (c cmdable) HMGet(context interface{}, key string, fields ...string) *SliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(fields))
	args[0] = "hmget"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HSet accepts values in following formats:
//   - HSet("myhash", "key1", "value1", "key2", "value2")
//   - HSet("myhash", []string{"key1", "value1", "key2", "value2"})
//   - HSet("myhash", map[string]interface{}{"key1": "value1", "key2": "value2"})
//
// Note that it requires Redis v4 for multiple field/value pairs support.
func (c cmdable) HSet(context interface{}, key string, values ...interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "hset"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HMSet is a deprecated version of HSet left for compatibility with Redis 3.
func (c cmdable) HMSet(context interface{}, key string, values ...interface{}) *BoolCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "hmset"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HSetNX(context interface{}, key, field string, value interface{}) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "hsetnx", key, field, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HVals(context interface{}, key string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "hvals", key)
	_ = c(ctx, cmd)
	return cmd
}

// HRandField redis-server version >= 6.2.0.
func (c cmdable) HRandField(context interface{}, key string, count int, withValues bool) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 4)

	// Although count=0 is meaningless, redis accepts count=0.
	args = append(args, "hrandfield", key, count)
	if withValues {
		args = append(args, "withvalues")
	}

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) BLPop(context interface{}, timeout time.Duration, keys ...string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys)+1)
	args[0] = "blpop"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(args)-1] = formatSec(ctx, timeout)
	cmd := NewStringSliceCmd(ctx, args...)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BRPop(context interface{}, timeout time.Duration, keys ...string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys)+1)
	args[0] = "brpop"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(keys)+1] = formatSec(ctx, timeout)
	cmd := NewStringSliceCmd(ctx, args...)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BRPopLPush(context interface{}, source, destination string, timeout time.Duration) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(
		ctx,
		"brpoplpush",
		source,
		destination,
		formatSec(ctx, timeout),
	)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LIndex(context interface{}, key string, index int64) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "lindex", key, index)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LInsert(context interface{}, key, op string, pivot, value interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "linsert", key, op, pivot, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LInsertBefore(context interface{}, key string, pivot, value interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "linsert", key, "before", pivot, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LInsertAfter(context interface{}, key string, pivot, value interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "linsert", key, "after", pivot, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LLen(context interface{}, key string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "llen", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPop(context interface{}, key string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "lpop", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPopCount(context interface{}, key string, count int) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "lpop", key, count)
	_ = c(ctx, cmd)
	return cmd
}

type LPosArgs struct {
	Rank, MaxLen int64
}

func (c cmdable) LPos(context interface{}, key string, value string, a LPosArgs) *IntCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"lpos", key, value}
	if a.Rank != 0 {
		args = append(args, "rank", a.Rank)
	}
	if a.MaxLen != 0 {
		args = append(args, "maxlen", a.MaxLen)
	}

	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPosCount(context interface{}, key string, value string, count int64, a LPosArgs) *IntSliceCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"lpos", key, value, "count", count}
	if a.Rank != 0 {
		args = append(args, "rank", a.Rank)
	}
	if a.MaxLen != 0 {
		args = append(args, "maxlen", a.MaxLen)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPush(context interface{}, key string, values ...interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "lpush"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPushX(context interface{}, key string, values ...interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "lpushx"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LRange(context interface{}, key string, start, stop int64) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(
		ctx,
		"lrange",
		key,
		start,
		stop,
	)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LRem(context interface{}, key string, count int64, value interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "lrem", key, count, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LSet(context interface{}, key string, index int64, value interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "lset", key, index, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LTrim(context interface{}, key string, start, stop int64) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(
		ctx,
		"ltrim",
		key,
		start,
		stop,
	)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPop(context interface{}, key string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "rpop", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPopCount(context interface{}, key string, count int) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "rpop", key, count)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPopLPush(context interface{}, source, destination string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "rpoplpush", source, destination)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPush(context interface{}, key string, values ...interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "rpush"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPushX(context interface{}, key string, values ...interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "rpushx"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LMove(context interface{}, source, destination, srcpos, destpos string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "lmove", source, destination, srcpos, destpos)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) SAdd(context interface{}, key string, members ...interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(members))
	args[0] = "sadd"
	args[1] = key
	args = appendArgs(args, members)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SCard(context interface{}, key string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "scard", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SDiff(context interface{}, keys ...string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys))
	args[0] = "sdiff"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SDiffStore(context interface{}, destination string, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(keys))
	args[0] = "sdiffstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SInter(context interface{}, keys ...string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys))
	args[0] = "sinter"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SInterStore(context interface{}, destination string, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(keys))
	args[0] = "sinterstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SIsMember(context interface{}, key string, member interface{}) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "sismember", key, member)
	_ = c(ctx, cmd)
	return cmd
}

// SMIsMember Redis `SMISMEMBER key member [member ...]` command.
func (c cmdable) SMIsMember(context interface{}, key string, members ...interface{}) *BoolSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(members))
	args[0] = "smismember"
	args[1] = key
	args = appendArgs(args, members)
	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SMembers Redis `SMEMBERS key` command output as a slice.
func (c cmdable) SMembers(context interface{}, key string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "smembers", key)
	_ = c(ctx, cmd)
	return cmd
}

// SMembersMap Redis `SMEMBERS key` command output as a map.
func (c cmdable) SMembersMap(context interface{}, key string) *StringStructMapCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringStructMapCmd(ctx, "smembers", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SMove(context interface{}, source, destination string, member interface{}) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "smove", source, destination, member)
	_ = c(ctx, cmd)
	return cmd
}

// SPop Redis `SPOP key` command.
func (c cmdable) SPop(context interface{}, key string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "spop", key)
	_ = c(ctx, cmd)
	return cmd
}

// SPopN Redis `SPOP key count` command.
func (c cmdable) SPopN(context interface{}, key string, count int64) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "spop", key, count)
	_ = c(ctx, cmd)
	return cmd
}

// SRandMember Redis `SRANDMEMBER key` command.
func (c cmdable) SRandMember(context interface{}, key string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "srandmember", key)
	_ = c(ctx, cmd)
	return cmd
}

// SRandMemberN Redis `SRANDMEMBER key count` command.
func (c cmdable) SRandMemberN(context interface{}, key string, count int64) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "srandmember", key, count)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SRem(context interface{}, key string, members ...interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(members))
	args[0] = "srem"
	args[1] = key
	args = appendArgs(args, members)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SUnion(context interface{}, keys ...string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys))
	args[0] = "sunion"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SUnionStore(context interface{}, destination string, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(keys))
	args[0] = "sunionstore"
	args[1] = destination
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

// XAddArgs accepts values in the following formats:
//   - XAddArgs.Values = []interface{}{"key1", "value1", "key2", "value2"}
//   - XAddArgs.Values = []string("key1", "value1", "key2", "value2")
//   - XAddArgs.Values = map[string]interface{}{"key1": "value1", "key2": "value2"}
//
// Note that map will not preserve the order of key-value pairs.
// MaxLen/MaxLenApprox and MinID are in conflict, only one of them can be used.
type XAddArgs struct {
	Stream     string
	NoMkStream bool
	MaxLen     int64 // MAXLEN N

	// Deprecated: use MaxLen+Approx, remove in v9.
	MaxLenApprox int64 // MAXLEN ~ N

	MinID string
	// Approx causes MaxLen and MinID to use "~" matcher (instead of "=").
	Approx bool
	Limit  int64
	ID     string
	Values interface{}
}

// XAdd a.Limit has a bug, please confirm it and use it.
// issue: https://github.com/redis/redis/issues/9046
func (c cmdable) XAdd(context interface{}, a *XAddArgs) *StringCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 11)
	args = append(args, "xadd", a.Stream)
	if a.NoMkStream {
		args = append(args, "nomkstream")
	}
	switch {
	case a.MaxLen > 0:
		if a.Approx {
			args = append(args, "maxlen", "~", a.MaxLen)
		} else {
			args = append(args, "maxlen", a.MaxLen)
		}
	case a.MaxLenApprox > 0:
		// TODO remove in v9.
		args = append(args, "maxlen", "~", a.MaxLenApprox)
	case a.MinID != "":
		if a.Approx {
			args = append(args, "minid", "~", a.MinID)
		} else {
			args = append(args, "minid", a.MinID)
		}
	}
	if a.Limit > 0 {
		args = append(args, "limit", a.Limit)
	}
	if a.ID != "" {
		args = append(args, a.ID)
	} else {
		args = append(args, "*")
	}
	args = appendArg(args, a.Values)

	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XDel(context interface{}, stream string, ids ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"xdel", stream}
	for _, id := range ids {
		args = append(args, id)
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XLen(context interface{}, stream string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "xlen", stream)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XRange(context interface{}, stream, start, stop string) *XMessageSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewXMessageSliceCmd(ctx, "xrange", stream, start, stop)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XRangeN(context interface{}, stream, start, stop string, count int64) *XMessageSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewXMessageSliceCmd(ctx, "xrange", stream, start, stop, "count", count)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XRevRange(context interface{}, stream, start, stop string) *XMessageSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewXMessageSliceCmd(ctx, "xrevrange", stream, start, stop)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XRevRangeN(context interface{}, stream, start, stop string, count int64) *XMessageSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewXMessageSliceCmd(ctx, "xrevrange", stream, start, stop, "count", count)
	_ = c(ctx, cmd)
	return cmd
}

type XReadArgs struct {
	Streams []string // list of streams and ids, e.g. stream1 stream2 id1 id2
	Count   int64
	Block   time.Duration
}

func (c cmdable) XRead(context interface{}, a *XReadArgs) *XStreamSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 5+len(a.Streams))
	args = append(args, "xread")

	keyPos := int8(1)
	if a.Count > 0 {
		args = append(args, "count")
		args = append(args, a.Count)
		keyPos += 2
	}
	if a.Block >= 0 {
		args = append(args, "block")
		args = append(args, int64(a.Block/time.Millisecond))
		keyPos += 2
	}
	args = append(args, "streams")
	keyPos++
	for _, s := range a.Streams {
		args = append(args, s)
	}

	cmd := NewXStreamSliceCmd(ctx, args...)
	if a.Block >= 0 {
		cmd.setReadTimeout(a.Block)
	}
	cmd.setFirstKeyPos(keyPos)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XReadStreams(context interface{}, streams ...string) *XStreamSliceCmd {
	ctx := unwrapCtx(context)
	return c.XRead(ctx, &XReadArgs{
		Streams: streams,
		Block:   -1,
	})
}

func (c cmdable) XGroupCreate(context interface{}, stream, group, start string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "xgroup", "create", stream, group, start)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XGroupCreateMkStream(context interface{}, stream, group, start string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "xgroup", "create", stream, group, start, "mkstream")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XGroupSetID(context interface{}, stream, group, start string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "xgroup", "setid", stream, group, start)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XGroupDestroy(context interface{}, stream, group string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "xgroup", "destroy", stream, group)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XGroupCreateConsumer(context interface{}, stream, group, consumer string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "xgroup", "createconsumer", stream, group, consumer)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XGroupDelConsumer(context interface{}, stream, group, consumer string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "xgroup", "delconsumer", stream, group, consumer)
	_ = c(ctx, cmd)
	return cmd
}

type XReadGroupArgs struct {
	Group    string
	Consumer string
	Streams  []string // list of streams and ids, e.g. stream1 stream2 id1 id2
	Count    int64
	Block    time.Duration
	NoAck    bool
}

func (c cmdable) XReadGroup(context interface{}, a *XReadGroupArgs) *XStreamSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 8+len(a.Streams))
	args = append(args, "xreadgroup", "group", a.Group, a.Consumer)

	keyPos := int8(4)
	if a.Count > 0 {
		args = append(args, "count", a.Count)
		keyPos += 2
	}
	if a.Block >= 0 {
		args = append(args, "block", int64(a.Block/time.Millisecond))
		keyPos += 2
	}
	if a.NoAck {
		args = append(args, "noack")
		keyPos++
	}
	args = append(args, "streams")
	keyPos++
	for _, s := range a.Streams {
		args = append(args, s)
	}

	cmd := NewXStreamSliceCmd(ctx, args...)
	if a.Block >= 0 {
		cmd.setReadTimeout(a.Block)
	}
	cmd.setFirstKeyPos(keyPos)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XAck(context interface{}, stream, group string, ids ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"xack", stream, group}
	for _, id := range ids {
		args = append(args, id)
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XPending(context interface{}, stream, group string) *XPendingCmd {
	ctx := unwrapCtx(context)
	cmd := NewXPendingCmd(ctx, "xpending", stream, group)
	_ = c(ctx, cmd)
	return cmd
}

type XPendingExtArgs struct {
	Stream   string
	Group    string
	Idle     time.Duration
	Start    string
	End      string
	Count    int64
	Consumer string
}

func (c cmdable) XPendingExt(context interface{}, a *XPendingExtArgs) *XPendingExtCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 9)
	args = append(args, "xpending", a.Stream, a.Group)
	if a.Idle != 0 {
		args = append(args, "idle", formatMs(ctx, a.Idle))
	}
	args = append(args, a.Start, a.End, a.Count)
	if a.Consumer != "" {
		args = append(args, a.Consumer)
	}
	cmd := NewXPendingExtCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type XAutoClaimArgs struct {
	Stream   string
	Group    string
	MinIdle  time.Duration
	Start    string
	Count    int64
	Consumer string
}

func (c cmdable) XAutoClaim(context interface{}, a *XAutoClaimArgs) *XAutoClaimCmd {
	ctx := unwrapCtx(context)
	args := xAutoClaimArgs(ctx, a)
	cmd := NewXAutoClaimCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XAutoClaimJustID(context interface{}, a *XAutoClaimArgs) *XAutoClaimJustIDCmd {
	ctx := unwrapCtx(context)
	args := xAutoClaimArgs(ctx, a)
	args = append(args, "justid")
	cmd := NewXAutoClaimJustIDCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func xAutoClaimArgs(ctx context.Context, a *XAutoClaimArgs) []interface{} {
	args := make([]interface{}, 0, 9)
	args = append(args, "xautoclaim", a.Stream, a.Group, a.Consumer, formatMs(ctx, a.MinIdle), a.Start)
	if a.Count > 0 {
		args = append(args, "count", a.Count)
	}
	return args
}

type XClaimArgs struct {
	Stream   string
	Group    string
	Consumer string
	MinIdle  time.Duration
	Messages []string
}

func (c cmdable) XClaim(context interface{}, a *XClaimArgs) *XMessageSliceCmd {
	ctx := unwrapCtx(context)
	args := xClaimArgs(a)
	cmd := NewXMessageSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XClaimJustID(context interface{}, a *XClaimArgs) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := xClaimArgs(a)
	args = append(args, "justid")
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func xClaimArgs(a *XClaimArgs) []interface{} {
	args := make([]interface{}, 0, 4+len(a.Messages))
	args = append(args,
		"xclaim",
		a.Stream,
		a.Group, a.Consumer,
		int64(a.MinIdle/time.Millisecond))
	for _, id := range a.Messages {
		args = append(args, id)
	}
	return args
}

// xTrim If approx is true, add the "~" parameter, otherwise it is the default "=" (redis default).
// example:
//		XTRIM key MAXLEN/MINID threshold LIMIT limit.
//		XTRIM key MAXLEN/MINID ~ threshold LIMIT limit.
// The redis-server version is lower than 6.2, please set limit to 0.
func (c cmdable) xTrim(
	ctx context.Context, key, strategy string,
	approx bool, threshold interface{}, limit int64,
) *IntCmd {
	args := make([]interface{}, 0, 7)
	args = append(args, "xtrim", key, strategy)
	if approx {
		args = append(args, "~")
	}
	args = append(args, threshold)
	if limit > 0 {
		args = append(args, "limit", limit)
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// Deprecated: use XTrimMaxLen, remove in v9.
func (c cmdable) XTrim(context interface{}, key string, maxLen int64) *IntCmd {
	ctx := unwrapCtx(context)
	return c.xTrim(ctx, key, "maxlen", false, maxLen, 0)
}

// Deprecated: use XTrimMaxLenApprox, remove in v9.
func (c cmdable) XTrimApprox(context interface{}, key string, maxLen int64) *IntCmd {
	ctx := unwrapCtx(context)
	return c.xTrim(ctx, key, "maxlen", true, maxLen, 0)
}

// XTrimMaxLen No `~` rules are used, `limit` cannot be used.
// cmd: XTRIM key MAXLEN maxLen
func (c cmdable) XTrimMaxLen(context interface{}, key string, maxLen int64) *IntCmd {
	ctx := unwrapCtx(context)
	return c.xTrim(ctx, key, "maxlen", false, maxLen, 0)
}

// XTrimMaxLenApprox LIMIT has a bug, please confirm it and use it.
// issue: https://github.com/redis/redis/issues/9046
// cmd: XTRIM key MAXLEN ~ maxLen LIMIT limit
func (c cmdable) XTrimMaxLenApprox(context interface{}, key string, maxLen, limit int64) *IntCmd {
	ctx := unwrapCtx(context)
	return c.xTrim(ctx, key, "maxlen", true, maxLen, limit)
}

// XTrimMinID No `~` rules are used, `limit` cannot be used.
// cmd: XTRIM key MINID minID
func (c cmdable) XTrimMinID(context interface{}, key string, minID string) *IntCmd {
	ctx := unwrapCtx(context)
	return c.xTrim(ctx, key, "minid", false, minID, 0)
}

// XTrimMinIDApprox LIMIT has a bug, please confirm it and use it.
// issue: https://github.com/redis/redis/issues/9046
// cmd: XTRIM key MINID ~ minID LIMIT limit
func (c cmdable) XTrimMinIDApprox(context interface{}, key string, minID string, limit int64) *IntCmd {
	ctx := unwrapCtx(context)
	return c.xTrim(ctx, key, "minid", true, minID, limit)
}

func (c cmdable) XInfoConsumers(context interface{}, key string, group string) *XInfoConsumersCmd {
	ctx := unwrapCtx(context)
	cmd := NewXInfoConsumersCmd(ctx, key, group)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XInfoGroups(context interface{}, key string) *XInfoGroupsCmd {
	ctx := unwrapCtx(context)
	cmd := NewXInfoGroupsCmd(ctx, key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) XInfoStream(context interface{}, key string) *XInfoStreamCmd {
	ctx := unwrapCtx(context)
	cmd := NewXInfoStreamCmd(ctx, key)
	_ = c(ctx, cmd)
	return cmd
}

// XInfoStreamFull XINFO STREAM FULL [COUNT count]
// redis-server >= 6.0.
func (c cmdable) XInfoStreamFull(context interface{}, key string, count int) *XInfoStreamFullCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 6)
	args = append(args, "xinfo", "stream", key, "full")
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewXInfoStreamFullCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

// Z represents sorted set member.
type Z struct {
	Score  float64
	Member interface{}
}

// ZWithKey represents sorted set member including the name of the key where it was popped.
type ZWithKey struct {
	Z
	Key string
}

// ZStore is used as an arg to ZInter/ZInterStore and ZUnion/ZUnionStore.
type ZStore struct {
	Keys    []string
	Weights []float64
	// Can be SUM, MIN or MAX.
	Aggregate string
}

func (z ZStore) len() (n int) {
	n = len(z.Keys)
	if len(z.Weights) > 0 {
		n += 1 + len(z.Weights)
	}
	if z.Aggregate != "" {
		n += 2
	}
	return n
}

func (z ZStore) appendArgs(args []interface{}) []interface{} {
	for _, key := range z.Keys {
		args = append(args, key)
	}
	if len(z.Weights) > 0 {
		args = append(args, "weights")
		for _, weights := range z.Weights {
			args = append(args, weights)
		}
	}
	if z.Aggregate != "" {
		args = append(args, "aggregate", z.Aggregate)
	}
	return args
}

// BZPopMax Redis `BZPOPMAX key [key ...] timeout` command.
func (c cmdable) BZPopMax(context interface{}, timeout time.Duration, keys ...string) *ZWithKeyCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys)+1)
	args[0] = "bzpopmax"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(args)-1] = formatSec(ctx, timeout)
	cmd := NewZWithKeyCmd(ctx, args...)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

// BZPopMin Redis `BZPOPMIN key [key ...] timeout` command.
func (c cmdable) BZPopMin(context interface{}, timeout time.Duration, keys ...string) *ZWithKeyCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys)+1)
	args[0] = "bzpopmin"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(args)-1] = formatSec(ctx, timeout)
	cmd := NewZWithKeyCmd(ctx, args...)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

// ZAddArgs WARN: The GT, LT and NX options are mutually exclusive.
type ZAddArgs struct {
	NX      bool
	XX      bool
	LT      bool
	GT      bool
	Ch      bool
	Members []Z
}

func (c cmdable) zAddArgs(key string, args ZAddArgs, incr bool) []interface{} {
	a := make([]interface{}, 0, 6+2*len(args.Members))
	a = append(a, "zadd", key)

	// The GT, LT and NX options are mutually exclusive.
	if args.NX {
		a = append(a, "nx")
	} else {
		if args.XX {
			a = append(a, "xx")
		}
		if args.GT {
			a = append(a, "gt")
		} else if args.LT {
			a = append(a, "lt")
		}
	}
	if args.Ch {
		a = append(a, "ch")
	}
	if incr {
		a = append(a, "incr")
	}
	for _, m := range args.Members {
		a = append(a, m.Score)
		a = append(a, m.Member)
	}
	return a
}

func (c cmdable) ZAddArgs(context interface{}, key string, args ZAddArgs) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, c.zAddArgs(key, args, false)...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZAddArgsIncr(context interface{}, key string, args ZAddArgs) *FloatCmd {
	ctx := unwrapCtx(context)
	cmd := NewFloatCmd(ctx, c.zAddArgs(key, args, true)...)
	_ = c(ctx, cmd)
	return cmd
}

// TODO: Compatible with v8 api, will be removed in v9.
func (c cmdable) zAdd(ctx context.Context, key string, args ZAddArgs, members ...*Z) *IntCmd {
	args.Members = make([]Z, len(members))
	for i, m := range members {
		args.Members[i] = *m
	}
	cmd := NewIntCmd(ctx, c.zAddArgs(key, args, false)...)
	_ = c(ctx, cmd)
	return cmd
}

// ZAdd Redis `ZADD key score member [score member ...]` command.
func (c cmdable) ZAdd(context interface{}, key string, members ...*Z) *IntCmd {
	ctx := unwrapCtx(context)
	return c.zAdd(ctx, key, ZAddArgs{}, members...)
}

// ZAddNX Redis `ZADD key NX score member [score member ...]` command.
func (c cmdable) ZAddNX(context interface{}, key string, members ...*Z) *IntCmd {
	ctx := unwrapCtx(context)
	return c.zAdd(ctx, key, ZAddArgs{
		NX: true,
	}, members...)
}

// ZAddXX Redis `ZADD key XX score member [score member ...]` command.
func (c cmdable) ZAddXX(context interface{}, key string, members ...*Z) *IntCmd {
	ctx := unwrapCtx(context)
	return c.zAdd(ctx, key, ZAddArgs{
		XX: true,
	}, members...)
}

// ZAddCh Redis `ZADD key CH score member [score member ...]` command.
// Deprecated: Use
//		client.ZAddArgs(ctx, ZAddArgs{
//			Ch: true,
//			Members: []Z,
//		})
//	remove in v9.
func (c cmdable) ZAddCh(context interface{}, key string, members ...*Z) *IntCmd {
	ctx := unwrapCtx(context)
	return c.zAdd(ctx, key, ZAddArgs{
		Ch: true,
	}, members...)
}

// ZAddNXCh Redis `ZADD key NX CH score member [score member ...]` command.
// Deprecated: Use
//		client.ZAddArgs(ctx, ZAddArgs{
//			NX: true,
//			Ch: true,
//			Members: []Z,
//		})
//	remove in v9.
func (c cmdable) ZAddNXCh(context interface{}, key string, members ...*Z) *IntCmd {
	ctx := unwrapCtx(context)
	return c.zAdd(ctx, key, ZAddArgs{
		NX: true,
		Ch: true,
	}, members...)
}

// ZAddXXCh Redis `ZADD key XX CH score member [score member ...]` command.
// Deprecated: Use
//		client.ZAddArgs(ctx, ZAddArgs{
//			XX: true,
//			Ch: true,
//			Members: []Z,
//		})
//	remove in v9.
func (c cmdable) ZAddXXCh(context interface{}, key string, members ...*Z) *IntCmd {
	ctx := unwrapCtx(context)
	return c.zAdd(ctx, key, ZAddArgs{
		XX: true,
		Ch: true,
	}, members...)
}

// ZIncr Redis `ZADD key INCR score member` command.
// Deprecated: Use
//		client.ZAddArgsIncr(ctx, ZAddArgs{
//			Members: []Z,
//		})
//	remove in v9.
func (c cmdable) ZIncr(context interface{}, key string, member *Z) *FloatCmd {
	ctx := unwrapCtx(context)
	return c.ZAddArgsIncr(ctx, key, ZAddArgs{
		Members: []Z{*member},
	})
}

// ZIncrNX Redis `ZADD key NX INCR score member` command.
// Deprecated: Use
//		client.ZAddArgsIncr(ctx, ZAddArgs{
//			NX: true,
//			Members: []Z,
//		})
//	remove in v9.
func (c cmdable) ZIncrNX(context interface{}, key string, member *Z) *FloatCmd {
	ctx := unwrapCtx(context)
	return c.ZAddArgsIncr(ctx, key, ZAddArgs{
		NX:      true,
		Members: []Z{*member},
	})
}

// ZIncrXX Redis `ZADD key XX INCR score member` command.
// Deprecated: Use
//		client.ZAddArgsIncr(ctx, ZAddArgs{
//			XX: true,
//			Members: []Z,
//		})
//	remove in v9.
func (c cmdable) ZIncrXX(context interface{}, key string, member *Z) *FloatCmd {
	ctx := unwrapCtx(context)
	return c.ZAddArgsIncr(ctx, key, ZAddArgs{
		XX:      true,
		Members: []Z{*member},
	})
}

func (c cmdable) ZCard(context interface{}, key string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "zcard", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZCount(context interface{}, key, min, max string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "zcount", key, min, max)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZLexCount(context interface{}, key, min, max string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "zlexcount", key, min, max)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZIncrBy(context interface{}, key string, increment float64, member string) *FloatCmd {
	ctx := unwrapCtx(context)
	cmd := NewFloatCmd(ctx, "zincrby", key, increment, member)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZInterStore(context interface{}, destination string, store *ZStore) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 3+store.len())
	args = append(args, "zinterstore", destination, len(store.Keys))
	args = store.appendArgs(args)
	cmd := NewIntCmd(ctx, args...)
	cmd.setFirstKeyPos(3)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZInter(context interface{}, store *ZStore) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 2+store.len())
	args = append(args, "zinter", len(store.Keys))
	args = store.appendArgs(args)
	cmd := NewStringSliceCmd(ctx, args...)
	cmd.setFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZInterWithScores(context interface{}, store *ZStore) *ZSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 3+store.len())
	args = append(args, "zinter", len(store.Keys))
	args = store.appendArgs(args)
	args = append(args, "withscores")
	cmd := NewZSliceCmd(ctx, args...)
	cmd.setFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZMScore(context interface{}, key string, members ...string) *FloatSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(members))
	args[0] = "zmscore"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewFloatSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZPopMax(context interface{}, key string, count ...int64) *ZSliceCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{
		"zpopmax",
		key,
	}

	switch len(count) {
	case 0:
		break
	case 1:
		args = append(args, count[0])
	default:
		panic("too many arguments")
	}

	cmd := NewZSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZPopMin(context interface{}, key string, count ...int64) *ZSliceCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{
		"zpopmin",
		key,
	}

	switch len(count) {
	case 0:
		break
	case 1:
		args = append(args, count[0])
	default:
		panic("too many arguments")
	}

	cmd := NewZSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// ZRangeArgs is all the options of the ZRange command.
// In version> 6.2.0, you can replace the(cmd):
//		ZREVRANGE,
//		ZRANGEBYSCORE,
//		ZREVRANGEBYSCORE,
//		ZRANGEBYLEX,
//		ZREVRANGEBYLEX.
// Please pay attention to your redis-server version.
//
// Rev, ByScore, ByLex and Offset+Count options require redis-server 6.2.0 and higher.
type ZRangeArgs struct {
	Key string

	// When the ByScore option is provided, the open interval(exclusive) can be set.
	// By default, the score intervals specified by <Start> and <Stop> are closed (inclusive).
	// It is similar to the deprecated(6.2.0+) ZRangeByScore command.
	// For example:
	//		ZRangeArgs{
	//			Key: 				"example-key",
	//	 		Start: 				"(3",
	//	 		Stop: 				8,
	//			ByScore:			true,
	//	 	}
	// 	 	cmd: "ZRange example-key (3 8 ByScore"  (3 < score <= 8).
	//
	// For the ByLex option, it is similar to the deprecated(6.2.0+) ZRangeByLex command.
	// You can set the <Start> and <Stop> options as follows:
	//		ZRangeArgs{
	//			Key: 				"example-key",
	//	 		Start: 				"[abc",
	//	 		Stop: 				"(def",
	//			ByLex:				true,
	//	 	}
	//		cmd: "ZRange example-key [abc (def ByLex"
	//
	// For normal cases (ByScore==false && ByLex==false), <Start> and <Stop> should be set to the index range (int).
	// You can read the documentation for more information: https://redis.io/commands/zrange
	Start interface{}
	Stop  interface{}

	// The ByScore and ByLex options are mutually exclusive.
	ByScore bool
	ByLex   bool

	Rev bool

	// limit offset count.
	Offset int64
	Count  int64
}

func (z ZRangeArgs) appendArgs(args []interface{}) []interface{} {
	// For Rev+ByScore/ByLex, we need to adjust the position of <Start> and <Stop>.
	if z.Rev && (z.ByScore || z.ByLex) {
		args = append(args, z.Key, z.Stop, z.Start)
	} else {
		args = append(args, z.Key, z.Start, z.Stop)
	}

	if z.ByScore {
		args = append(args, "byscore")
	} else if z.ByLex {
		args = append(args, "bylex")
	}
	if z.Rev {
		args = append(args, "rev")
	}
	if z.Offset != 0 || z.Count != 0 {
		args = append(args, "limit", z.Offset, z.Count)
	}
	return args
}

func (c cmdable) ZRangeArgs(context interface{}, z ZRangeArgs) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 9)
	args = append(args, "zrange")
	args = z.appendArgs(args)
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRangeArgsWithScores(context interface{}, z ZRangeArgs) *ZSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 10)
	args = append(args, "zrange")
	args = z.appendArgs(args)
	args = append(args, "withscores")
	cmd := NewZSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRange(context interface{}, key string, start, stop int64) *StringSliceCmd {
	ctx := unwrapCtx(context)
	return c.ZRangeArgs(ctx, ZRangeArgs{
		Key:   key,
		Start: start,
		Stop:  stop,
	})
}

func (c cmdable) ZRangeWithScores(context interface{}, key string, start, stop int64) *ZSliceCmd {
	ctx := unwrapCtx(context)
	return c.ZRangeArgsWithScores(ctx, ZRangeArgs{
		Key:   key,
		Start: start,
		Stop:  stop,
	})
}

type ZRangeBy struct {
	Min, Max      string
	Offset, Count int64
}

func (c cmdable) zRangeBy(context interface{}, zcmd, key string, opt *ZRangeBy, withScores bool) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{zcmd, key, opt.Min, opt.Max}
	if withScores {
		args = append(args, "withscores")
	}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"limit",
			opt.Offset,
			opt.Count,
		)
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRangeByScore(context interface{}, key string, opt *ZRangeBy) *StringSliceCmd {
	ctx := unwrapCtx(context)
	return c.zRangeBy(ctx, "zrangebyscore", key, opt, false)
}

func (c cmdable) ZRangeByLex(context interface{}, key string, opt *ZRangeBy) *StringSliceCmd {
	ctx := unwrapCtx(context)
	return c.zRangeBy(ctx, "zrangebylex", key, opt, false)
}

func (c cmdable) ZRangeByScoreWithScores(context interface{}, key string, opt *ZRangeBy) *ZSliceCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"zrangebyscore", key, opt.Min, opt.Max, "withscores"}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"limit",
			opt.Offset,
			opt.Count,
		)
	}
	cmd := NewZSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRangeStore(context interface{}, dst string, z ZRangeArgs) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 10)
	args = append(args, "zrangestore", dst)
	args = z.appendArgs(args)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRank(context interface{}, key, member string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "zrank", key, member)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRem(context interface{}, key string, members ...interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(members))
	args[0] = "zrem"
	args[1] = key
	args = appendArgs(args, members)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRemRangeByRank(context interface{}, key string, start, stop int64) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(
		ctx,
		"zremrangebyrank",
		key,
		start,
		stop,
	)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRemRangeByScore(context interface{}, key, min, max string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "zremrangebyscore", key, min, max)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRemRangeByLex(context interface{}, key, min, max string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "zremrangebylex", key, min, max)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRevRange(context interface{}, key string, start, stop int64) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "zrevrange", key, start, stop)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRevRangeWithScores(context interface{}, key string, start, stop int64) *ZSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewZSliceCmd(ctx, "zrevrange", key, start, stop, "withscores")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) zRevRangeBy(ctx context.Context, zcmd, key string, opt *ZRangeBy) *StringSliceCmd {
	args := []interface{}{zcmd, key, opt.Max, opt.Min}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"limit",
			opt.Offset,
			opt.Count,
		)
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRevRangeByScore(context interface{}, key string, opt *ZRangeBy) *StringSliceCmd {
	ctx := unwrapCtx(context)
	return c.zRevRangeBy(ctx, "zrevrangebyscore", key, opt)
}

func (c cmdable) ZRevRangeByLex(context interface{}, key string, opt *ZRangeBy) *StringSliceCmd {
	ctx := unwrapCtx(context)
	return c.zRevRangeBy(ctx, "zrevrangebylex", key, opt)
}

func (c cmdable) ZRevRangeByScoreWithScores(context interface{}, key string, opt *ZRangeBy) *ZSliceCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"zrevrangebyscore", key, opt.Max, opt.Min, "withscores"}
	if opt.Offset != 0 || opt.Count != 0 {
		args = append(
			args,
			"limit",
			opt.Offset,
			opt.Count,
		)
	}
	cmd := NewZSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZRevRank(context interface{}, key, member string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "zrevrank", key, member)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZScore(context interface{}, key, member string) *FloatCmd {
	ctx := unwrapCtx(context)
	cmd := NewFloatCmd(ctx, "zscore", key, member)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZUnion(context interface{}, store ZStore) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 2+store.len())
	args = append(args, "zunion", len(store.Keys))
	args = store.appendArgs(args)
	cmd := NewStringSliceCmd(ctx, args...)
	cmd.setFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZUnionWithScores(context interface{}, store ZStore) *ZSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 3+store.len())
	args = append(args, "zunion", len(store.Keys))
	args = store.appendArgs(args)
	args = append(args, "withscores")
	cmd := NewZSliceCmd(ctx, args...)
	cmd.setFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ZUnionStore(context interface{}, dest string, store *ZStore) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 3+store.len())
	args = append(args, "zunionstore", dest, len(store.Keys))
	args = store.appendArgs(args)
	cmd := NewIntCmd(ctx, args...)
	cmd.setFirstKeyPos(3)
	_ = c(ctx, cmd)
	return cmd
}

// ZRandMember redis-server version >= 6.2.0.
func (c cmdable) ZRandMember(context interface{}, key string, count int, withScores bool) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 4)

	// Although count=0 is meaningless, redis accepts count=0.
	args = append(args, "zrandmember", key, count)
	if withScores {
		args = append(args, "withscores")
	}

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// ZDiff redis-server version >= 6.2.0.
func (c cmdable) ZDiff(context interface{}, keys ...string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(keys))
	args[0] = "zdiff"
	args[1] = len(keys)
	for i, key := range keys {
		args[i+2] = key
	}

	cmd := NewStringSliceCmd(ctx, args...)
	cmd.setFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

// ZDiffWithScores redis-server version >= 6.2.0.
func (c cmdable) ZDiffWithScores(context interface{}, keys ...string) *ZSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 3+len(keys))
	args[0] = "zdiff"
	args[1] = len(keys)
	for i, key := range keys {
		args[i+2] = key
	}
	args[len(keys)+2] = "withscores"

	cmd := NewZSliceCmd(ctx, args...)
	cmd.setFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

// ZDiffStore redis-server version >=6.2.0.
func (c cmdable) ZDiffStore(context interface{}, destination string, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 3+len(keys))
	args = append(args, "zdiffstore", destination, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) PFAdd(context interface{}, key string, els ...interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2, 2+len(els))
	args[0] = "pfadd"
	args[1] = key
	args = appendArgs(args, els)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) PFCount(context interface{}, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 1+len(keys))
	args[0] = "pfcount"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) PFMerge(context interface{}, dest string, keys ...string) *StatusCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(keys))
	args[0] = "pfmerge"
	args[1] = dest
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) BgRewriteAOF(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "bgrewriteaof")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BgSave(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "bgsave")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClientKill(context interface{}, ipPort string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "client", "kill", ipPort)
	_ = c(ctx, cmd)
	return cmd
}

// ClientKillByFilter is new style syntax, while the ClientKill is old
//
//   CLIENT KILL <option> [value] ... <option> [value]
func (c cmdable) ClientKillByFilter(context interface{}, keys ...string) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(keys))
	args[0] = "client"
	args[1] = "kill"
	for i, key := range keys {
		args[2+i] = key
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClientList(context interface{}) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "client", "list")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClientPause(context interface{}, dur time.Duration) *BoolCmd {
	ctx := unwrapCtx(context)
	cmd := NewBoolCmd(ctx, "client", "pause", formatMs(ctx, dur))
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClientID(context interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "client", "id")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClientUnblock(context interface{}, id int64) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "client", "unblock", id)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClientUnblockWithError(context interface{}, id int64) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "client", "unblock", id, "error")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ConfigGet(context interface{}, parameter string) *SliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewSliceCmd(ctx, "config", "get", parameter)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ConfigResetStat(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "config", "resetstat")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ConfigSet(context interface{}, parameter, value string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "config", "set", parameter, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ConfigRewrite(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "config", "rewrite")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) DBSize(context interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "dbsize")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FlushAll(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "flushall")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FlushAllAsync(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "flushall", "async")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FlushDB(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "flushdb")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FlushDBAsync(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "flushdb", "async")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Info(context interface{}, section ...string) *StringCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"info"}
	if len(section) > 0 {
		args = append(args, section[0])
	}
	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LastSave(context interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "lastsave")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Save(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "save")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) shutdown(ctx context.Context, modifier string) *StatusCmd {
	var args []interface{}
	if modifier == "" {
		args = []interface{}{"shutdown"}
	} else {
		args = []interface{}{"shutdown", modifier}
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	if err := cmd.Err(); err != nil {
		if err == io.EOF {
			// Server quit as expected.
			cmd.err = nil
		}
	} else {
		// Server did not quit. String reply contains the reason.
		cmd.err = errors.New(cmd.val)
		cmd.val = ""
	}
	return cmd
}

func (c cmdable) Shutdown(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	return c.shutdown(ctx, "")
}

func (c cmdable) ShutdownSave(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	return c.shutdown(ctx, "save")
}

func (c cmdable) ShutdownNoSave(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	return c.shutdown(ctx, "nosave")
}

func (c cmdable) SlaveOf(context interface{}, host, port string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "slaveof", host, port)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) SlowLogGet(context interface{}, num int64) *SlowLogCmd {
	ctx := unwrapCtx(context)
	cmd := NewSlowLogCmd(ctx, "slowlog", "get", num)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Sync(_ context.Context) {
	panic("not implemented")
}

func (c cmdable) Time(context interface{}) *TimeCmd {
	ctx := unwrapCtx(context)
	cmd := NewTimeCmd(ctx, "time")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) DebugObject(context interface{}, key string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "debug", "object", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ReadOnly(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "readonly")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ReadWrite(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "readwrite")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) MemoryUsage(context interface{}, key string, samples ...int) *IntCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"memory", "usage", key}
	if len(samples) > 0 {
		if len(samples) != 1 {
			panic("MemoryUsage expects single sample count")
		}
		args = append(args, "SAMPLES", samples[0])
	}
	cmd := NewIntCmd(ctx, args...)
	cmd.setFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) Eval(context interface{}, script string, keys []string, args ...interface{}) *Cmd {
	ctx := unwrapCtx(context)
	cmdArgs := make([]interface{}, 3+len(keys), 3+len(keys)+len(args))
	cmdArgs[0] = "eval"
	cmdArgs[1] = script
	cmdArgs[2] = len(keys)
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	cmdArgs = appendArgs(cmdArgs, args)
	cmd := NewCmd(ctx, cmdArgs...)
	cmd.setFirstKeyPos(3)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) EvalSha(context interface{}, sha1 string, keys []string, args ...interface{}) *Cmd {
	ctx := unwrapCtx(context)
	cmdArgs := make([]interface{}, 3+len(keys), 3+len(keys)+len(args))
	cmdArgs[0] = "evalsha"
	cmdArgs[1] = sha1
	cmdArgs[2] = len(keys)
	for i, key := range keys {
		cmdArgs[3+i] = key
	}
	cmdArgs = appendArgs(cmdArgs, args)
	cmd := NewCmd(ctx, cmdArgs...)
	cmd.setFirstKeyPos(3)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ScriptExists(context interface{}, hashes ...string) *BoolSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(hashes))
	args[0] = "script"
	args[1] = "exists"
	for i, hash := range hashes {
		args[2+i] = hash
	}
	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ScriptFlush(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "script", "flush")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ScriptKill(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "script", "kill")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ScriptLoad(context interface{}, script string) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "script", "load", script)
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

// Publish posts the message to the channel.
func (c cmdable) Publish(context interface{}, channel string, message interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "publish", channel, message)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) PubSubChannels(context interface{}, pattern string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := []interface{}{"pubsub", "channels"}
	if pattern != "*" {
		args = append(args, pattern)
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) PubSubNumSub(context interface{}, channels ...string) *StringIntMapCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(channels))
	args[0] = "pubsub"
	args[1] = "numsub"
	for i, channel := range channels {
		args[2+i] = channel
	}
	cmd := NewStringIntMapCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) PubSubNumPat(context interface{}) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "pubsub", "numpat")
	_ = c(ctx, cmd)
	return cmd
}

//------------------------------------------------------------------------------

func (c cmdable) ClusterSlots(context interface{}) *ClusterSlotsCmd {
	ctx := unwrapCtx(context)
	cmd := NewClusterSlotsCmd(ctx, "cluster", "slots")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterNodes(context interface{}) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "cluster", "nodes")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterMeet(context interface{}, host, port string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "cluster", "meet", host, port)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterForget(context interface{}, nodeID string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "cluster", "forget", nodeID)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterReplicate(context interface{}, nodeID string) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "cluster", "replicate", nodeID)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterResetSoft(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "cluster", "reset", "soft")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterResetHard(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "cluster", "reset", "hard")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterInfo(context interface{}) *StringCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringCmd(ctx, "cluster", "info")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterKeySlot(context interface{}, key string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "cluster", "keyslot", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterGetKeysInSlot(context interface{}, slot int, count int) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "cluster", "getkeysinslot", slot, count)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterCountFailureReports(context interface{}, nodeID string) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "cluster", "count-failure-reports", nodeID)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterCountKeysInSlot(context interface{}, slot int) *IntCmd {
	ctx := unwrapCtx(context)
	cmd := NewIntCmd(ctx, "cluster", "countkeysinslot", slot)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterDelSlots(context interface{}, slots ...int) *StatusCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(slots))
	args[0] = "cluster"
	args[1] = "delslots"
	for i, slot := range slots {
		args[2+i] = slot
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterDelSlotsRange(context interface{}, min, max int) *StatusCmd {
	ctx := unwrapCtx(context)
	size := max - min + 1
	slots := make([]int, size)
	for i := 0; i < size; i++ {
		slots[i] = min + i
	}
	return c.ClusterDelSlots(ctx, slots...)
}

func (c cmdable) ClusterSaveConfig(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "cluster", "saveconfig")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterSlaves(context interface{}, nodeID string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	cmd := NewStringSliceCmd(ctx, "cluster", "slaves", nodeID)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterFailover(context interface{}) *StatusCmd {
	ctx := unwrapCtx(context)
	cmd := NewStatusCmd(ctx, "cluster", "failover")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterAddSlots(context interface{}, slots ...int) *StatusCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(slots))
	args[0] = "cluster"
	args[1] = "addslots"
	for i, num := range slots {
		args[2+i] = num
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ClusterAddSlotsRange(context interface{}, min, max int) *StatusCmd {
	ctx := unwrapCtx(context)
	size := max - min + 1
	slots := make([]int, size)
	for i := 0; i < size; i++ {
		slots[i] = min + i
	}
	return c.ClusterAddSlots(ctx, slots...)
}

//------------------------------------------------------------------------------

func (c cmdable) GeoAdd(context interface{}, key string, geoLocation ...*GeoLocation) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+3*len(geoLocation))
	args[0] = "geoadd"
	args[1] = key
	for i, eachLoc := range geoLocation {
		args[2+3*i] = eachLoc.Longitude
		args[2+3*i+1] = eachLoc.Latitude
		args[2+3*i+2] = eachLoc.Name
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// GeoRadius is a read-only GEORADIUS_RO command.
func (c cmdable) GeoRadius(
	context interface{}, key string, longitude, latitude float64, query *GeoRadiusQuery,
) *GeoLocationCmd {
	ctx := unwrapCtx(context)
	cmd := NewGeoLocationCmd(ctx, query, "georadius_ro", key, longitude, latitude)
	if query.Store != "" || query.StoreDist != "" {
		cmd.SetErr(errors.New("GeoRadius does not support Store or StoreDist"))
		return cmd
	}
	_ = c(ctx, cmd)
	return cmd
}

// GeoRadiusStore is a writing GEORADIUS command.
func (c cmdable) GeoRadiusStore(
	context interface{}, key string, longitude, latitude float64, query *GeoRadiusQuery,
) *IntCmd {
	ctx := unwrapCtx(context)
	args := geoLocationArgs(query, "georadius", key, longitude, latitude)
	cmd := NewIntCmd(ctx, args...)
	if query.Store == "" && query.StoreDist == "" {
		cmd.SetErr(errors.New("GeoRadiusStore requires Store or StoreDist"))
		return cmd
	}
	_ = c(ctx, cmd)
	return cmd
}

// GeoRadiusByMember is a read-only GEORADIUSBYMEMBER_RO command.
func (c cmdable) GeoRadiusByMember(
	context interface{}, key, member string, query *GeoRadiusQuery,
) *GeoLocationCmd {
	ctx := unwrapCtx(context)
	cmd := NewGeoLocationCmd(ctx, query, "georadiusbymember_ro", key, member)
	if query.Store != "" || query.StoreDist != "" {
		cmd.SetErr(errors.New("GeoRadiusByMember does not support Store or StoreDist"))
		return cmd
	}
	_ = c(ctx, cmd)
	return cmd
}

// GeoRadiusByMemberStore is a writing GEORADIUSBYMEMBER command.
func (c cmdable) GeoRadiusByMemberStore(
	context interface{}, key, member string, query *GeoRadiusQuery,
) *IntCmd {
	ctx := unwrapCtx(context)
	args := geoLocationArgs(query, "georadiusbymember", key, member)
	cmd := NewIntCmd(ctx, args...)
	if query.Store == "" && query.StoreDist == "" {
		cmd.SetErr(errors.New("GeoRadiusByMemberStore requires Store or StoreDist"))
		return cmd
	}
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) GeoSearch(context interface{}, key string, q *GeoSearchQuery) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 13)
	args = append(args, "geosearch", key)
	args = geoSearchArgs(q, args)
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) GeoSearchLocation(
	context interface{}, key string, q *GeoSearchLocationQuery,
) *GeoSearchLocationCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 16)
	args = append(args, "geosearch", key)
	args = geoSearchLocationArgs(q, args)
	cmd := NewGeoSearchLocationCmd(ctx, q, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) GeoSearchStore(context interface{}, key, store string, q *GeoSearchStoreQuery) *IntCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 0, 15)
	args = append(args, "geosearchstore", store, key)
	args = geoSearchArgs(&q.GeoSearchQuery, args)
	if q.StoreDist {
		args = append(args, "storedist")
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) GeoDist(
	context interface{}, key string, member1, member2, unit string,
) *FloatCmd {
	ctx := unwrapCtx(context)
	if unit == "" {
		unit = "km"
	}
	cmd := NewFloatCmd(ctx, "geodist", key, member1, member2, unit)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) GeoHash(context interface{}, key string, members ...string) *StringSliceCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(members))
	args[0] = "geohash"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) GeoPos(context interface{}, key string, members ...string) *GeoPosCmd {
	ctx := unwrapCtx(context)
	args := make([]interface{}, 2+len(members))
	args[0] = "geopos"
	args[1] = key
	for i, member := range members {
		args[2+i] = member
	}
	cmd := NewGeoPosCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
