package source

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/storage"
)

// csvOptions selects CSV dialect details independently of the dataset, so
// mapping files can be read with fixed defaults.
type csvOptions struct {
	comma rune
	// columns replaces the header line when non-nil.
	columns []string
	// rejectMalformed returns rows whose field count differs from the header
	// as malformed records instead of failing. Only dataset inputs set it;
	// mapping files are configuration and stay strict.
	rejectMalformed bool
}

func (d *Dataset) csvOptions() csvOptions {
	o := csvOptions{comma: ',', rejectMalformed: d.cfg.Input.OnMalformed == "reject"}
	if d.cfg.Input.Delimiter != "" {
		o.comma, _ = utf8.DecodeRuneInString(d.cfg.Input.Delimiter)
	}
	if d.cfg.Input.Header != nil && !*d.cfg.Input.Header {
		o.columns = d.cfg.Input.Columns
	}
	return o
}

// csvInput reads one object with a single long-lived encoding/csv.Reader.
type csvInput struct {
	d               *Dataset
	src             *textStream
	reader          *csv.Reader
	header          []string
	values          []cell
	bom             int64 // bytes skipped before the csv reader's offset 0
	recordEnd       int64 // csv offset just after the previous record
	nextLine        int   // physical line the next record must start on
	headerFix       int64 // charge for the retained header
	rejectMalformed bool
	workspace       highWater
}

func openCSV(ctx context.Context, d *Dataset, obj storage.Object, o csvOptions) (*csvInput, error) {
	src, err := openTextStream(ctx, d, obj, textReadBufferBytes, false)
	if err != nil {
		return nil, err
	}
	return newCSVInput(d, src, o)
}

// newCSVInput takes ownership of src and reads the header unless o.columns
// supplies the names.
func newCSVInput(d *Dataset, src *textStream, o csvOptions) (out *csvInput, err error) {
	c := &csvInput{d: d, src: src, nextLine: 1, rejectMalformed: o.rejectMalformed, workspace: highWater{budget: d.budget, phase: "CSV decoder buffers"}}
	// Reserve before each read, not for the configured record ceiling. The
	// limiter includes decoder read-ahead in n, so even buffered records and
	// comma-only records are covered before encoding/csv can expand them.
	src.limiter.reserve = func(n int64) error {
		needed, sizeErr := decoderWorkspace(n, csvRecordCopies, n+1, csvDecoderFieldBytes)
		if sizeErr != nil {
			return sizeErr
		}
		return c.workspace.growTo(needed)
	}
	defer func() {
		if err != nil {
			_ = c.close()
		}
	}()
	br := bufio.NewReaderSize(src.limiter, textReadBufferBytes)
	c.bom = skipBOM(br)
	src.limiter.startRecord(c.bom)
	c.reader = csv.NewReader(br)
	c.reader.Comma = o.comma
	c.reader.FieldsPerRecord = -1
	c.reader.LazyQuotes = false
	c.reader.ReuseRecord = true
	header := o.columns
	if header == nil {
		fields, e := c.read()
		if errors.Is(e, io.EOF) {
			return nil, c.malformed(diagnostics.ReasonMissingHeader, 1, 0)
		}
		if e != nil {
			return nil, e
		}
		header = fields
	}
	if err = validateNames(header); err != nil {
		return nil, err
	}
	c.headerFix = int64(len(header)) * csvRetainedFieldBytes
	for _, s := range header {
		c.headerFix += int64(len(s))
	}
	if err = d.budget.Reserve(c.headerFix, "CSV header"); err != nil {
		c.headerFix = 0
		return nil, err
	}
	c.header = slices.Clone(header)
	c.values = make([]cell, len(c.header))
	return c, nil
}

// read returns the next record's fields (reused by the following call),
// enforcing the byte limit, UTF-8, and that no blank line was skipped.
func (c *csvInput) read() ([]string, error) {
	fields, err := c.reader.Read()
	if c.src.limiter.exceeded {
		return nil, c.tooLarge()
	}
	if errors.Is(err, io.EOF) {
		if c.reader.InputOffset() != c.recordEnd {
			return nil, c.malformed(diagnostics.ReasonBlankLine, c.nextLine, 0)
		}
		return nil, io.EOF
	}
	if err != nil {
		var parse *csv.ParseError
		if errors.As(err, &parse) {
			reason := diagnostics.ReasonCSVSyntax
			switch {
			case errors.Is(parse.Err, csv.ErrQuote):
				reason = diagnostics.ReasonQuote
			case errors.Is(parse.Err, csv.ErrBareQuote):
				reason = diagnostics.ReasonBareQuote
			case errors.Is(parse.Err, csv.ErrFieldCount):
				reason = diagnostics.ReasonFieldCount
			}
			// Column counts from parse.Line, so it only locates the error
			// when the record starts on that line.
			column := 0
			if parse.Line == parse.StartLine {
				column = parse.Column
			}
			return nil, c.malformed(reason, parse.StartLine, column)
		}
		return nil, err
	}
	end := c.reader.InputOffset()
	size := end - c.recordEnd
	if size > c.src.limiter.limit {
		return nil, c.tooLarge()
	}
	// encoding/csv silently skips empty lines; the spec forbids silent skips.
	line, _ := c.reader.FieldPos(0)
	if line != c.nextLine {
		return nil, c.malformed(diagnostics.ReasonBlankLine, c.nextLine, 0)
	}
	last := len(fields) - 1
	lastLine, _ := c.reader.FieldPos(last)
	c.nextLine = lastLine + strings.Count(fields[last], "\n") + 1
	c.recordEnd = end
	c.src.limiter.startRecord(end + c.bom)
	for _, f := range fields {
		if !utf8.ValidString(f) {
			return nil, c.malformed(diagnostics.ReasonInvalidUTF8, line, 0)
		}
	}
	return fields, nil
}

// malformed describes a framing failure by position only; record contents
// never enter the error.
func (c *csvInput) malformed(reason string, line, column int) *diagnostics.InputError {
	return &diagnostics.InputError{Format: "csv", Reason: reason, Source: c.src.uri, Line: int64(line), Column: int64(column), Delimiter: c.reader.Comma}
}

func (c *csvInput) tooLarge() error {
	err := c.src.limiter.inputError()
	err.Format, err.Source, err.Line, err.Delimiter = "csv", c.src.uri, int64(c.nextLine), c.reader.Comma
	return err
}

func (c *csvInput) names() []string { return c.header }

// next returns one record. Decode time and rows are recorded per batch by the
// caller, not per record.
func (c *csvInput) next(ctx context.Context) (out lexicalRecord, err error) {
	if err := ctx.Err(); err != nil {
		return lexicalRecord{}, err
	}
	start := c.nextLine
	fields, err := c.read()
	if err != nil {
		return lexicalRecord{}, err
	}
	if len(fields) != len(c.header) {
		if c.rejectMalformed {
			// The whole record was read, so its boundaries are certain.
			return lexicalRecord{names: c.header, malformed: slices.Clone(fields), line: uint64(start)}, nil
		}
		err := c.malformed(diagnostics.ReasonFieldCount, start, 0)
		err.Expected, err.Found = len(c.header), len(fields)
		return lexicalRecord{}, err
	}
	for i, s := range fields {
		c.values[i] = cell{text: s, kind: kindString, present: true}
	}
	return lexicalRecord{names: c.header, values: c.values, line: uint64(start)}, nil
}

func (c *csvInput) close() error {
	c.workspace.release()
	c.d.budget.Release(c.headerFix)
	c.headerFix = 0
	if c.src == nil {
		return nil
	}
	err := c.src.close()
	c.src = nil
	return err
}
