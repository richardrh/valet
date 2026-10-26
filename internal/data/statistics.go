package data

import (
	"fmt"
	"math"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// Coverage separates decoded outcomes from producer-declared metadata claims.
type Coverage struct {
	ValueRows               uint64  `json:"value_rows"`
	MetadataClaimedRows     uint64  `json:"metadata_claimed_rows"`
	AccountedRows           uint64  `json:"accounted_rows"`
	RowGroupsScanned        *uint64 `json:"row_groups_scanned"`
	RowGroupsStatisticsOnly *uint64 `json:"row_groups_statistics_only"`
	RowGroupsTotal          *uint64 `json:"row_groups_total"`
}
type RuleCoverage struct {
	ValueRows           uint64 `json:"value_rows"`
	MetadataClaimedRows uint64 `json:"metadata_claimed_rows"`
}
type RowMaterialization struct {
	Policy  string `json:"policy"`
	Emitted uint64 `json:"emitted"`
	Omitted uint64 `json:"omitted"`
}

// CheckedAdd rejects counter/provenance overflow instead of wrapping.
func CheckedAdd(a, b uint64) (uint64, error) {
	if b > math.MaxUint64-a {
		return 0, fmt.Errorf("row counter overflow")
	}
	return a + b, nil
}

// ColumnStatistics borrows modern bounds from the guarded dataset footer.
// Optional fields preserve absence; no generated getter defaults are evidence.
type ColumnStatistics struct {
	Name               string
	Type               arrow.DataType
	PhysicalType       string
	PhysicalLength     int
	NullCount          *int64
	NaNCount           *int64
	MinValue, MaxValue []byte
	MinExact, MaxExact *bool
	TypeDefinedOrder   bool
	LegacyOnly         bool
}
type ParquetGroup struct {
	SourceOrdinal  uint64
	RowGroup       uint64
	DeclaredRows   uint64
	Source         string
	FooterSHA256   string
	FooterOffset   int64
	FooterLength   int64
	IdentityMethod string
	Identity       map[string]string
	Columns        []ColumnStatistics
}
type CheckEvidence struct {
	Code                   string   `json:"code"`
	CheckIndex             int      `json:"check_index"`
	Columns                []string `json:"columns"`
	Claim                  string   `json:"claim"`
	Fields                 []string `json:"fields"`
	Reason                 string   `json:"reason"`
	ImplicitConstraintKind string   `json:"implicit_constraint_kind,omitempty"`
}
type GroupEvidence struct {
	SourceOrdinal         uint64            `json:"source_ordinal"`
	RowGroup              uint64            `json:"row_group"`
	DeclaredRows          uint64            `json:"declared_rows"`
	Source                string            `json:"source"`
	FooterSHA256          string            `json:"footer_sha256"`
	FooterOffset          int64             `json:"footer_offset"`
	FooterLength          int64             `json:"footer_length"`
	IdentityMethod        string            `json:"identity_method"`
	Identity              map[string]string `json:"identity"`
	Action                string            `json:"action"`
	Reason                string            `json:"reason"`
	Checks                []CheckEvidence   `json:"checks"`
	IngestionVerification string            `json:"ingestion_verification"`
	PayloadVerification   string            `json:"payload_verification"`
}

// StatisticsPlan owns its immutable decisions and their managed reservation.
// Retain gives a consumer a distinct release handle sharing the same storage.
// Dataset.ApplyStatisticsPlan retains its own handle; callers still Release.
type StatisticsPlan struct {
	Groups []GroupEvidence
	owner  *statisticsPlanOwner
	once   sync.Once
}
type statisticsPlanOwner struct {
	mu      sync.Mutex
	refs    int
	cleanup func()
}

func NewStatisticsPlan(groups []GroupEvidence, cleanup func()) *StatisticsPlan {
	return &StatisticsPlan{Groups: groups, owner: &statisticsPlanOwner{refs: 1, cleanup: cleanup}}
}
func (p *StatisticsPlan) Retain() *StatisticsPlan {
	if p == nil {
		return nil
	}
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	if p.owner.refs == 0 {
		return nil
	}
	p.owner.refs++
	return &StatisticsPlan{Groups: p.Groups, owner: p.owner}
}
func (p *StatisticsPlan) Release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.owner.mu.Lock()
		p.owner.refs--
		last := p.owner.refs == 0
		cleanup := p.owner.cleanup
		if last {
			p.owner.cleanup = nil
		}
		p.owner.mu.Unlock()
		p.Groups = nil
		if last && cleanup != nil {
			cleanup()
		}
	})
}
