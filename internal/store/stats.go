package store

import "sync/atomic"

// counters 是存储的累计计数（/metrics 用）。
type counters struct {
	flushes, flushedRows, compactedFiles atomic.Int64
	uploads, uploadFailures              atomic.Int64
	downloads, downloadFailures          atomic.Int64
	bloomChecked, bloomSkipped           atomic.Int64
}

// Stats 是存储的状态与累计计数。
type Stats struct {
	Segments, SegmentRows, PendingRows   int64
	Flushes, FlushedRows, CompactedFiles int64
	Uploads, UploadFailures              int64
	Downloads, DownloadFailures          int64
	BloomChecked, BloomSkipped           int64 // 按 bloom 检查过的分段数与跳过的分段数
}

// Stats 返回当前状态。
func (s *Store) Stats() Stats {
	s.segMu.Lock()
	var st Stats
	for _, g := range s.cur.segs {
		st.Segments++
		st.SegmentRows += g.rows
	}
	s.segMu.Unlock()
	s.mu.Lock()
	seq := s.seq
	s.mu.Unlock()
	if !s.readOnly {
		st.PendingRows = max(s.pendingRows(seq), 0)
	}
	c := &s.stats
	st.Flushes, st.FlushedRows, st.CompactedFiles = c.flushes.Load(), c.flushedRows.Load(), c.compactedFiles.Load()
	st.Uploads, st.UploadFailures = c.uploads.Load(), c.uploadFailures.Load()
	st.Downloads, st.DownloadFailures = c.downloads.Load(), c.downloadFailures.Load()
	st.BloomChecked, st.BloomSkipped = c.bloomChecked.Load(), c.bloomSkipped.Load()
	return st
}
