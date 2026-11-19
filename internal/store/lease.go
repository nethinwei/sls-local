package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// 写入节点的租约：共用一个对象存储的写入节点靠 seq 高位的编号区分各自的分段，编号重复的两个节点会把对方的
// 分段当成自己的合并、删除。所以写入节点启动时在对象存储上登记 _writers/<编号>.json（令牌 + 续约时间），
// 之后每 leaseRenew 续一次：
//
//   - 启动时租约是别人的、还没过期：拒绝启动（编号重复，或旧进程还没退出）。
//   - 续约时发现租约变成别人的：停止写入、刷段、合并、删除（fenced），就绪检查失败。
//   - 令牌存在本地库里：同一块盘上重启（包括崩溃后）直接接着用，不用等租约过期。
//   - 正常退出时删掉租约。
//
// 对象存储没有比较后写入，两个节点同时启动时都可能写成功；后写的赢，先写的在下一次续约（≤ leaseRenew）时发现。

var (
	leaseTTL   = time.Minute
	leaseRenew = 10 * time.Second
	// abandonAfter：租约这么久没续，才认为写入节点已经下线（由别的节点接管它的清单、删它过期的分段）。比 leaseTTL
	// 长得多：还在运行、只是一时连不上对象存储的节点不能被接管。正常退出的节点删掉了租约，立即算下线。
	abandonAfter = 10 * time.Minute
)

// ErrFenced 是租约被另一个使用相同编号的写入节点拿走后的写入、刷段。
var ErrFenced = errors.New("another write node took over this -writer-id; this node stopped writing")

type leaseRecord struct {
	Token   string `json:"token"`
	Renewed int64  `json:"renewed"` // Unix 秒
	Host    string `json:"host,omitempty"`
}

func leaseKey(writerID int64) string { return "_writers/" + strconv.FormatInt(writerID, 10) + ".json" }

func (s *Store) readLease(ctx context.Context, writerID int64) (*leaseRecord, error) {
	data, err := s.remote.obj.ReadSmall(ctx, leaseKey(writerID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read lease of writer %d: %w", writerID, err)
	}
	var r leaseRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("read lease of writer %d: %w", writerID, err)
	}
	return &r, nil
}

func (r *leaseRecord) fresh(now time.Time) bool {
	return r != nil && now.Sub(time.Unix(r.Renewed, 0)) < leaseTTL
}

func (s *Store) writeLease(ctx context.Context) error {
	host, _ := os.Hostname()
	data, _ := json.Marshal(leaseRecord{Token: s.leaseToken, Renewed: time.Now().Unix(), Host: host})
	if err := s.remote.obj.WriteSmall(ctx, leaseKey(s.writerID), data); err != nil {
		return fmt.Errorf("write lease: %w", err)
	}
	return nil
}

// acquireLease 在启动时拿到本节点编号的租约（打开存储时、读对象存储上的分段之前调用）。
func (s *Store) acquireLease(ctx context.Context) error {
	var tok sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT value FROM sls_meta WHERE key = 'writer_token'`).Scan(&tok)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if s.leaseToken = tok.String; !tok.Valid {
		b := make([]byte, 16)
		rand.Read(b)
		s.leaseToken = hex.EncodeToString(b)
		if _, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO sls_meta VALUES ('writer_token', ?)`, s.leaseToken); err != nil {
			return err
		}
	}
	cur, err := s.readLease(ctx, s.writerID)
	if err != nil {
		return err
	}
	if cur != nil && cur.Token != s.leaseToken && cur.fresh(time.Now()) {
		return fmt.Errorf("-writer-id %d is in use by another write node (host %q, renewed %s ago): "+
			"give each write node its own id, or wait %s if the previous process has just stopped",
			s.writerID, cur.Host, time.Since(time.Unix(cur.Renewed, 0)).Round(time.Second), leaseTTL)
	}
	if err := s.writeLease(ctx); err != nil {
		return err
	}
	return s.checkLease(ctx) // 同时启动的另一个节点可能刚好覆盖了它
}

// checkLease 确认租约还是自己的；不是的话进入 fenced。
func (s *Store) checkLease(ctx context.Context) error {
	cur, err := s.readLease(ctx, s.writerID)
	if err != nil {
		return err
	}
	if cur != nil && cur.Token != s.leaseToken {
		s.fenced.Store(true)
		return fmt.Errorf("%w (host %q)", ErrFenced, cur.Host)
	}
	return nil
}

// renewLease 检查并续约。
func (s *Store) renewLease(ctx context.Context) error {
	if err := s.checkLease(ctx); err != nil {
		return err
	}
	return s.writeLease(ctx)
}

// leaseKeeper 定期续约，直到 ctx 结束。
func (s *Store) leaseKeeper(ctx context.Context, done chan struct{}) {
	defer close(done)
	tick := time.NewTicker(leaseRenew)
	defer tick.Stop()
	for !s.fenced.Load() {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if err := s.renewLease(ctx); err != nil && ctx.Err() == nil && s.onError != nil {
			s.onError(err)
		}
	}
}

// releaseLease 在正常退出时删掉自己的租约。
func (s *Store) releaseLease(ctx context.Context) error {
	if s.fenced.Load() || s.checkLease(ctx) != nil {
		return nil
	}
	return s.remote.obj.Delete(ctx, leaseKey(s.writerID))
}

// writerAlive 判断写入节点是否可能还在运行：有租约，且 abandonAfter 内续过。
func (s *Store) writerAlive(ctx context.Context, writerID int64) (bool, error) {
	r, err := s.readLease(ctx, writerID)
	if err != nil || r == nil {
		return false, err
	}
	return time.Since(time.Unix(r.Renewed, 0)) < abandonAfter, nil
}

// dropAbandonedBefore 处理已经下线的写入节点（租约已释放，或 10 分钟没续）在对象存储上留下的分段：分段只由写它的节点删、
// 清单只由它自己维护，没有这一步的话，缩容、换了编号的节点的分段会一直留着，崩溃前上传了却没写进清单的分段
// 也一直查不到。所以每个写入节点在保留期检查时：
//
//   - 删掉下线节点整段早于 cutoff 的分段，以及合并后没来得及删的旧分段（remoteGrace 之后删，查询节点可能正在下载）；
//   - 按列举到的、留下的分段重写它的清单（先于删除）。
//
// 还在运行的节点的不动，与它自己的删除、清单不会冲突。返回删掉的过期日志条数。
func (s *Store) dropAbandonedBefore(ctx context.Context, cutoff int64) (int64, error) {
	if s.remote == nil || s.readOnly || s.fenced.Load() {
		return 0, nil
	}
	keys, err := s.remote.obj.List(ctx, "")
	if err != nil {
		return 0, fmt.Errorf("list %s: %w", s.remote.obj, err)
	}
	byWriter := map[int64][]segment{}
	for _, k := range keys {
		g, ok := parseSegment(s.segDir, filepath.Join(s.segDir, filepath.FromSlash(k)))
		if ok && writerOf(g.seqMin) != s.writerID {
			byWriter[writerOf(g.seqMin)] = append(byWriter[writerOf(g.seqMin)], g)
		}
	}
	var n int64
	for w, segs := range byWriter {
		alive, err := s.writerAlive(ctx, w)
		if err != nil {
			return n, err
		}
		if alive {
			continue
		}
		kept, drop := dropContained(segs)
		live := kept[:0]
		for _, g := range kept {
			if g.tmax < cutoff {
				drop = append(drop, g)
				n += g.rows
			} else {
				live = append(live, g)
			}
		}
		if err := s.rewriteManifests(ctx, w, live); err != nil {
			return n, err
		}
		s.trashRemote(drop)
	}
	return n, nil
}
