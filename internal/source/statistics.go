package source

//lint:file-ignore ST1005 Error strings here begin with the proper nouns Parquet or Arrow.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/richardrh/valet/internal/data"
)

// validateParquetStructure runs before a trusted decision can suppress payload
// guards. Optional statistics are deliberately not structural requirements.
func validateParquetStructure(meta *metadata.FileMetaData, schema *arrow.Schema, payloadEnd int64) error {
	if meta.NumRows < 0 || payloadEnd < 4 {
		return errors.New("invalid Parquet structural dimensions")
	}
	var rows int64
	for group, raw := range meta.RowGroups {
		if raw == nil || raw.NumRows < 0 || raw.NumRows > math.MaxInt64-rows {
			return errors.New("invalid or overflowing Parquet row-group row count")
		}
		rows += raw.NumRows
		if len(raw.Columns) != schema.NumFields() || len(raw.Columns) != meta.Schema.NumColumns() {
			return errors.New("row-group column count differs from schema")
		}
		if raw.TotalByteSize < 0 || (raw.TotalCompressedSize != nil && *raw.TotalCompressedSize < 0) {
			return errors.New("invalid Parquet row-group size")
		}
		rg := meta.RowGroup(group)
		for col, rawColumn := range raw.Columns {
			if rawColumn == nil || rawColumn.MetaData == nil {
				return errors.New("missing Parquet column chunk metadata")
			}
			chunk, err := rg.ColumnChunk(col)
			if err != nil {
				return err
			}
			descriptor := meta.Schema.Column(col)
			if chunk.Type() != descriptor.PhysicalType() {
				return errors.New("Parquet chunk physical type differs from schema")
			}
			if descriptor.MaxRepetitionLevel() != 0 || descriptor.MaxDefinitionLevel() > 1 {
				return errors.New("nested Parquet levels are unsupported")
			}
			if chunk.FilePath() != "" || chunk.CryptoMetadata() != nil {
				return errors.New("external or encrypted Parquet column chunks are unsupported")
			}
			if chunk.NumValues() != raw.NumRows {
				return errors.New("flat Parquet chunk count differs from row-group count")
			}
			if chunk.TotalUncompressedSize() < 0 {
				return errors.New("invalid Parquet column uncompressed size")
			}
			codec := chunk.Compression()
			if codec != compress.Codecs.Uncompressed && codec != compress.Codecs.Snappy && codec != compress.Codecs.Zstd {
				return fmt.Errorf("unsupported Parquet codec %s", codec)
			}
			start := chunk.DataPageOffset()
			dictionary := chunk.DictionaryPageOffset()
			if dictionary > 0 {
				if start == 0 && chunk.NumValues() == 0 {
					start = dictionary
				} else {
					start = min(start, dictionary)
				}
			}
			size := chunk.TotalCompressedSize()
			if size == 0 && chunk.NumValues() == 0 {
				continue
			}
			if start < 4 || size < 0 || start > payloadEnd || size > payloadEnd-start {
				return errors.New("Parquet column chunk lies outside the payload")
			}
			if rawColumn.FileOffset < 0 || (rawColumn.OffsetIndexOffset != nil && (*rawColumn.OffsetIndexOffset < 0 || rawColumn.OffsetIndexLength == nil || *rawColumn.OffsetIndexLength < 0 || *rawColumn.OffsetIndexOffset > payloadEnd || int64(*rawColumn.OffsetIndexLength) > payloadEnd-*rawColumn.OffsetIndexOffset)) || (rawColumn.ColumnIndexOffset != nil && (*rawColumn.ColumnIndexOffset < 0 || rawColumn.ColumnIndexLength == nil || *rawColumn.ColumnIndexLength < 0 || *rawColumn.ColumnIndexOffset > payloadEnd || int64(*rawColumn.ColumnIndexLength) > payloadEnd-*rawColumn.ColumnIndexOffset)) {
				return errors.New("invalid Parquet column structural offset")
			}
		}
	}
	if rows != meta.NumRows {
		return errors.New("Parquet file row count differs from row-group counts")
	}
	return nil
}

// ParquetGroups borrows guarded facts whose lifetime is the dataset. It never
// reads payload and does not confer trust; only a complete checks plan can skip.
func (d *Dataset) ParquetGroups() ([]data.ParquetGroup, error) {
	if d.closed {
		return nil, errors.New("dataset is closed")
	}
	if d.kind != inputParquet {
		return nil, errors.New("statistics require Parquet input")
	}
	if d.groups != nil {
		return d.groups, nil
	}
	var count int64
	var charge int64
	for _, obj := range d.objects {
		prep := d.parquet[obj.URI]
		if prep == nil {
			return nil, errors.New("Parquet metadata is not prepared")
		}
		n := int64(len(prep.metadata.RowGroups))
		columns := int64(prep.schema.NumFields())
		if columns > (math.MaxInt64-512)/256 {
			return nil, errors.New("Parquet statistics column accounting overflow")
		}
		perGroup := int64(512) + columns*256
		overhead := int64(len(obj.URI)) + int64(len(obj.ETag)) + int64(len(obj.VersionID)) + 1024
		if n > math.MaxInt64-count || overhead > math.MaxInt64-charge || (n != 0 && perGroup > (math.MaxInt64-charge-overhead)/n) {
			return nil, errors.New("Parquet statistics metadata accounting overflow")
		}
		count += n
		charge += n*perGroup + overhead
	}
	if count > int64(math.MaxInt) {
		return nil, errors.New("too many Parquet row groups")
	}
	if err := d.reserve(charge, "Parquet statistics facts"); err != nil {
		return nil, err
	}
	d.groups = make([]data.ParquetGroup, 0, int(count))
	var total uint64
	for ordinal, obj := range d.objects {
		prep := d.parquet[obj.URI]
		identity := map[string]string{"size": strconv.FormatInt(obj.Size, 10), "mtime": obj.ModTime.Format(time.RFC3339Nano)}
		if obj.ETag != "" {
			identity["etag"] = obj.ETag
		}
		if obj.VersionID != "" {
			identity["version_id"] = obj.VersionID
		}
		for group, raw := range prep.metadata.RowGroups {
			rows := uint64(raw.NumRows)
			var err error
			total, err = data.CheckedAdd(total, rows)
			if err != nil {
				return nil, err
			}
			facts := data.ParquetGroup{SourceOrdinal: uint64(ordinal + 1), RowGroup: uint64(group), DeclaredRows: rows, Source: obj.URI, FooterSHA256: prep.footerSHA256, FooterOffset: prep.footerOffset, FooterLength: prep.footerLength, IdentityMethod: obj.IdentityMethod, Identity: identity, Columns: make([]data.ColumnStatistics, len(raw.Columns))}
			for col, chunk := range raw.Columns {
				f := prep.schema.Field(col)
				stats := data.ColumnStatistics{Name: d.rename(f.Name), Type: f.Type, PhysicalType: chunk.MetaData.Type.String()}
				stats.PhysicalLength = prep.metadata.Schema.Column(col).TypeLength()
				if len(prep.metadata.ColumnOrders) > col && prep.metadata.ColumnOrders[col] != nil {
					stats.TypeDefinedOrder = prep.metadata.ColumnOrders[col].IsSetTYPE_ORDER()
				}
				if rawStats := chunk.MetaData.Statistics; rawStats != nil {
					stats.NullCount, stats.NaNCount = rawStats.NullCount, rawStats.NanCount
					stats.MinValue, stats.MaxValue = rawStats.MinValue, rawStats.MaxValue
					stats.MinExact, stats.MaxExact = rawStats.IsMinValueExact, rawStats.IsMaxValueExact
					stats.LegacyOnly = (rawStats.MinValue == nil || rawStats.MaxValue == nil) && (rawStats.Min != nil || rawStats.Max != nil)
				}
				facts.Columns[col] = stats
			}
			d.groups = append(d.groups, facts)
		}
	}
	return d.groups, nil
}

// ApplyStatisticsPlan retains an immutable, complete, source-ordered plan. A
// prepass has already required ordinary values and cannot later be short-cut.
// Baselines for fully scanned inputs are established first; the plan is only
// committed once they succeed, so a failed call leaves no plan applied.
func (d *Dataset) ApplyStatisticsPlan(ctx context.Context, plan *data.StatisticsPlan) error {
	if d.closed {
		return errors.New("dataset is closed")
	}
	if d.statistics != nil {
		return errors.New("statistics plan already applied")
	}
	if plan == nil {
		return errors.New("nil statistics plan")
	}
	groups, err := d.ParquetGroups()
	if err != nil {
		return err
	}
	if len(plan.Groups) != len(groups) {
		return errors.New("statistics plan does not cover every row group")
	}
	for i, decision := range plan.Groups {
		fact := groups[i]
		if decision.SourceOrdinal != fact.SourceOrdinal || decision.RowGroup != fact.RowGroup || decision.DeclaredRows != fact.DeclaredRows || decision.Source != fact.Source || decision.FooterSHA256 != fact.FooterSHA256 || decision.FooterOffset != fact.FooterOffset || decision.FooterLength != fact.FooterLength {
			return errors.New("statistics plan does not match pinned footer")
		}
		if decision.Action != "skip" && decision.Action != "scan" {
			return errors.New("invalid statistics group action")
		}
		if decision.Action == "skip" && (decision.DeclaredRows == 0 || d.cfg.Input.Parquet.Statistics != "trusted" || (d.cfg.Output.Mode != "failures" && d.cfg.Output.Mode != "report") || d.openedBeforeStatistics || d.plan) {
			return errors.New("statistics plan skips a group requiring ordinary values")
		}
	}
	// Group decisions per object; establish baselines before changing state.
	perObject := make([][]data.GroupEvidence, len(d.objects))
	offset := 0
	for i := range d.objects {
		obj := &d.objects[i]
		prep := d.parquet[obj.URI]
		n := len(prep.metadata.RowGroups)
		perObject[i] = plan.Groups[offset : offset+n]
		offset += n
		if !anySkipped(perObject[i]) && !prep.baseline && !d.plan {
			if err = d.store.EstablishBaseline(ctx, *obj); err != nil {
				return err
			}
			prep.baseline = true
			if obj.IdentityMethod == "local_stat" {
				d.passes = 1
			}
		}
	}
	retained := plan.Retain()
	if retained == nil {
		return errors.New("statistics plan has been released")
	}
	d.statistics = retained
	for i := range retained.Groups {
		d.logStatisticsDecision(ctx, &retained.Groups[i])
	}
	for i := range d.objects {
		obj := &d.objects[i]
		prep := d.parquet[obj.URI]
		decisions := perObject[i]
		prep.decisions = decisions
		prep.selective = anySkipped(decisions)
		if prep.selective {
			obj.IngestionVerification = "partial"
			obj.PayloadVerification = "partial"
			if !anyScanned(decisions) {
				obj.PayloadVerification = "not_verified"
			}
		}
	}
	d.statisticsPending = false
	return nil
}

func anySkipped(decisions []data.GroupEvidence) bool {
	for _, decision := range decisions {
		if decision.Action == "skip" {
			return true
		}
	}
	return false
}

func anyScanned(decisions []data.GroupEvidence) bool {
	for _, decision := range decisions {
		if decision.Action != "skip" {
			return true
		}
	}
	return false
}

func (d *Dataset) establishPendingBaselines(ctx context.Context) error {
	if !d.statisticsPending {
		return nil
	}
	d.openedBeforeStatistics = true
	for _, obj := range d.objects {
		prep := d.parquet[obj.URI]
		if !prep.baseline && !d.plan {
			if err := d.store.EstablishBaseline(ctx, obj); err != nil {
				return err
			}
			prep.baseline = true
		}
	}
	d.statisticsPending = false
	if d.hasLocalObjects() {
		d.passes++
	}
	return nil
}

// VerifyStatisticsInputs is mandatory before committing selective results. It
// does not imply unread payload content, encodings, or row sizes were verified.
func (d *Dataset) VerifyStatisticsInputs(ctx context.Context) error {
	if d.closed {
		return errors.New("dataset is closed")
	}
	for _, obj := range d.objects {
		if prep := d.parquet[obj.URI]; prep != nil && prep.selective {
			if err := d.store.VerifyIdentity(ctx, obj); err != nil {
				return err
			}
		}
	}
	return nil
}
