package source

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/richardrh/valet/internal/diagnostics"
	"github.com/richardrh/valet/internal/storage"
)

// projection is a trie of JSON Pointer reference tokens built once from the
// declared columns. A node exists exactly for declared paths and their
// ancestors, so walking it alongside the decoder classifies every member in
// O(1).
type projection struct {
	children map[string]*projection
	columns  []int // declared columns selecting exactly this node
}

// projectionNodeBytes is charged per trie node: the node, its map slot and
// the token string header (token bytes are charged separately).
const projectionNodeBytes = 128

// insert adds ptr for column and returns the bytes newly retained.
func (p *projection) insert(ptr jsontext.Pointer, column int) int64 {
	var added int64
	node := p
	for token := range ptr.Tokens() {
		child := node.children[token]
		if child == nil {
			if node.children == nil {
				node.children = make(map[string]*projection)
			}
			child = &projection{}
			node.children[token] = child
			added += projectionNodeBytes + int64(len(token))
		}
		node = child
	}
	node.columns = append(node.columns, column)
	return added + 8
}

type jsonPolicy struct {
	ignoreExtras bool
	reserve      func(int64) error
}

// jsonWalker projects one record from a positioned jsontext.Decoder. It
// tokenises every byte once: selected scalars become cells, undeclared
// members become extras, and ignored subtrees are still read token by token
// so depth, syntax, duplicate names and UTF-8 are enforced by the decoder.
type jsonWalker struct {
	dec       *jsontext.Decoder
	root      *projection // nil: schema-less flat objects
	columns   []string    // source names of declared columns, by column
	maxDepth  int
	depthBase int // decoder stack depth outside the record (1 inside a JSON array)
	policy    jsonPolicy
	path      []string
	rec       lexicalRecord
	flatNames []string // reused name slice for schema-less records
}

var errFlatJSON = errors.New("JSON without schema supports flat scalar objects only")

// record walks the object whose '{' was just read and returns its projection.
// The returned slices are reused by the next call.
func (w *jsonWalker) record() (lexicalRecord, error) {
	values, sourceNames := w.rec.values, w.rec.sourceNames[:0]
	w.rec = lexicalRecord{sourceNames: sourceNames}
	if err := w.checkDepth(); err != nil {
		return w.rec, err
	}
	if w.root == nil {
		w.rec.names, w.rec.values = w.flatNames[:0], values[:0]
		err := w.flat()
		w.flatNames = w.rec.names
		return w.rec, err
	}
	if cap(values) < len(w.columns) {
		values = make([]cell, len(w.columns))
	}
	w.rec.names, w.rec.values = w.columns, values[:len(w.columns)]
	clear(w.rec.values)
	w.path = w.path[:0]
	w.selectNode(w.root, cell{present: true, kind: kindObject})
	return w.rec, w.members(w.root, !isLeaf(w.root))
}

func (w *jsonWalker) checkDepth() error {
	if w.dec.StackDepth()-w.depthBase > w.maxDepth {
		return errors.New("JSON nesting exceeds max_depth")
	}
	return nil
}

// flat reads a schema-less record: top-level scalar members only.
func (w *jsonWalker) flat() error {
	for {
		tok, err := w.dec.ReadToken()
		if err != nil {
			return err
		}
		if tok.Kind() == '}' {
			return nil
		}
		name := tok.String()
		value, err := w.dec.ReadToken()
		if err != nil {
			return err
		}
		v, scalar := scalarCell(value)
		if !scalar {
			return errFlatJSON
		}
		w.rec.names = append(w.rec.names, name)
		w.rec.sourceNames = append(w.rec.sourceNames, name)
		w.rec.values = append(w.rec.values, v)
	}
}

// isLeaf reports a declared node with nothing declared beneath it: its
// contents are the selected value, not further schema, so they are neither
// projected nor reported as extras.
func isLeaf(node *projection) bool { return len(node.columns) > 0 && len(node.children) == 0 }

func (w *jsonWalker) selectNode(node *projection, v cell) {
	for _, column := range node.columns {
		w.rec.values[column] = v
	}
}

// members walks the members of an object whose '{' was just read.
func (w *jsonWalker) members(node *projection, active bool) error {
	top := w.dec.StackDepth()-w.depthBase == 1
	for {
		tok, err := w.dec.ReadToken()
		if err != nil {
			return err
		}
		if tok.Kind() == '}' {
			return nil
		}
		var child *projection
		if active || top {
			name := tok.String()
			if top {
				w.rec.sourceNames = append(w.rec.sourceNames, name)
			}
			if active {
				child = node.children[name]
				if child == nil {
					if err = w.extra(name); err != nil {
						return err
					}
				}
				w.path = append(w.path, name)
			}
		}
		if err = w.value(child); err != nil {
			return err
		}
		if active {
			w.path = w.path[:len(w.path)-1]
		}
	}
}

// elements walks the elements of an array whose '[' was just read. Array
// elements are addressed by index and never reported as extras.
func (w *jsonWalker) elements(node *projection, active bool) error {
	for index := 0; ; index++ {
		if w.dec.PeekKind() == ']' {
			_, err := w.dec.ReadToken()
			return err
		}
		var child *projection
		if active {
			token := strconv.Itoa(index)
			child = node.children[token]
			w.path = append(w.path, token)
		}
		if err := w.value(child); err != nil {
			return err
		}
		if active {
			w.path = w.path[:len(w.path)-1]
		}
	}
}

// value reads the next value; node is nil when it is not projected.
func (w *jsonWalker) value(node *projection) error {
	tok, err := w.dec.ReadToken()
	if err != nil {
		return err
	}
	if v, scalar := scalarCell(tok); scalar {
		if node != nil {
			w.selectNode(node, v)
		}
		return nil
	}
	if err = w.checkDepth(); err != nil {
		return err
	}
	kind := kindObject
	if tok.Kind() == '[' {
		kind = kindArray
	}
	active := node != nil
	if active {
		w.selectNode(node, cell{present: true, kind: kind})
		active = !isLeaf(node)
	}
	if kind == kindArray {
		return w.elements(node, active)
	}
	return w.members(node, active)
}

// scalarCell converts a scalar token, keeping exact number lexemes.
func scalarCell(tok jsontext.Token) (cell, bool) {
	switch tok.Kind() {
	case 'n':
		return cell{present: true, kind: kindNull}, true
	case 't', 'f':
		return cell{text: tok.String(), present: true, kind: kindBool}, true
	case '0':
		return cell{text: tok.String(), present: true, kind: kindNumber}, true
	case '"':
		return cell{text: tok.String(), present: true, kind: kindString}, true
	}
	return cell{}, false
}

// extra records the undeclared member name under the current path as a JSON
// Pointer, reserving its retained size first.
func (w *jsonWalker) extra(name string) error {
	if w.policy.ignoreExtras {
		return nil
	}
	size := int64(0)
	for _, segment := range append(w.path, name) {
		size += 1 + int64(len(segment)) + int64(countEscapes(segment))
	}
	if w.policy.reserve != nil {
		if err := w.policy.reserve(size*2 + 64); err != nil {
			return err
		}
	}
	var ptr jsontext.Pointer
	for _, segment := range w.path {
		ptr = ptr.AppendToken(segment)
	}
	w.rec.extras = append(w.rec.extras, string(ptr.AppendToken(name)))
	return nil
}

func countEscapes(s string) int {
	n := 0
	for i := range len(s) {
		if s[i] == '~' || s[i] == '/' {
			n++
		}
	}
	return n
}

// jsonInput reads a JSON array or NDJSON object with one jsontext.Decoder.
type jsonInput struct {
	d         *Dataset
	src       *textStream
	dec       *jsontext.Decoder
	walker    jsonWalker
	array     bool
	started   bool
	done      bool
	records   uint64
	charge    int64 // per-record extra-path reservation
	workspace highWater
}

func openJSON(ctx context.Context, d *Dataset, obj storage.Object) (*jsonInput, error) {
	src, err := openTextStream(ctx, d, obj, jsonDecoderFixedBytes, true)
	if err != nil {
		return nil, err
	}
	return newJSONInput(d, src)
}

func newJSONInput(d *Dataset, src *textStream) (*jsonInput, error) {
	j := &jsonInput{d: d, src: src, array: d.kind == inputJSON}
	j.workspace = highWater{budget: d.budget, phase: "JSON decoder buffers"}
	needed, err := decoderWorkspace(d.recordLimit, jsonRecordCopies, 0, 0)
	if err != nil {
		_ = src.close()
		return nil, err
	}
	if err = j.workspace.growTo(needed); err != nil {
		_ = src.close()
		return nil, err
	}
	j.dec = jsontext.NewDecoder(src.limiter)
	j.walker = jsonWalker{dec: j.dec, root: d.projection, columns: d.selectionNames, maxDepth: d.maxDepth}
	if j.array {
		j.walker.depthBase = 1
	}
	j.walker.policy = jsonPolicy{ignoreExtras: d.ignoreExtras(), reserve: j.reserveRecord}
	return j, nil
}

func (j *jsonInput) reserveRecord(n int64) error {
	if err := j.d.budget.Reserve(n, "JSON projected paths"); err != nil {
		return err
	}
	j.charge += n
	return nil
}

func (j *jsonInput) names() []string {
	if j.walker.root == nil {
		return nil
	}
	return j.walker.columns
}

func (j *jsonInput) next(ctx context.Context) (out lexicalRecord, err error) {
	span := diagnostics.Start(ctx, diagnostics.Decode)
	defer func() {
		if errors.Is(err, io.EOF) {
			span.End(nil)
		} else {
			span.End(err)
		}
		if err == nil {
			diagnostics.AddRows(ctx, diagnostics.Decode, 1)
		}
	}()
	j.d.budget.Release(j.charge)
	j.charge = 0
	if err := ctx.Err(); err != nil {
		return lexicalRecord{}, err
	}
	rec, err := j.read()
	if err != nil {
		return lexicalRecord{}, j.classify(err)
	}
	return rec, nil
}

func (j *jsonInput) read() (lexicalRecord, error) {
	lim := j.src.limiter
	lim.startRecord(j.dec.InputOffset())
	if j.done {
		return lexicalRecord{}, io.EOF
	}
	if j.array && !j.started {
		tok, err := j.dec.ReadToken()
		if err != nil || tok.Kind() != '[' {
			return lexicalRecord{}, errors.New("JSON input requires a top-level array")
		}
		j.started = true
		lim.startRecord(j.dec.InputOffset())
	}
	tok, err := j.dec.ReadToken()
	if err != nil {
		if errors.Is(err, io.EOF) && !j.array {
			j.done = true
		}
		return lexicalRecord{}, err
	}
	switch kind := tok.Kind(); {
	case kind == '{':
	case kind == ']' && j.array:
		j.done = true
		if _, err = j.dec.ReadToken(); !errors.Is(err, io.EOF) {
			if lim.exceeded {
				return lexicalRecord{}, lim.err()
			}
			return lexicalRecord{}, errors.New("trailing data after JSON array")
		}
		return lexicalRecord{}, io.EOF
	case j.array:
		return lexicalRecord{}, errors.New("JSON array records must be objects")
	default:
		return lexicalRecord{}, errors.New("JSON records must be objects")
	}
	start := j.dec.InputOffset() - 1
	if !j.array && j.records > 0 && bytes.IndexByte(lim.span(lim.tapeBase, start), '\n') < 0 {
		return lexicalRecord{}, errors.New("NDJSON line contains more than one record")
	}
	lim.startRecord(start)
	line := lim.lineAt(start)
	rec, err := j.walker.record()
	if err != nil {
		return lexicalRecord{}, err
	}
	end := j.dec.InputOffset()
	if end-start > lim.limit {
		return lexicalRecord{}, lim.err()
	}
	rec.raw = lim.span(start, end)
	if !j.array && bytes.IndexByte(rec.raw, '\n') >= 0 {
		return lexicalRecord{}, errors.New("NDJSON record spans multiple lines")
	}
	if len(j.d.transforms) > 0 {
		renamed := make([]string, len(rec.sourceNames))
		for i, name := range rec.sourceNames {
			renamed[i] = j.d.rename(name)
		}
		if err = validateNames(renamed); err != nil {
			return lexicalRecord{}, err
		}
	}
	rec.line = line
	j.records++
	return rec, nil
}

// classify maps decoder failures to stable, value-free messages.
func (j *jsonInput) classify(err error) error {
	var syntax *jsontext.SyntacticError
	switch {
	case errors.Is(err, io.EOF):
		return io.EOF
	case j.src.limiter.exceeded:
		return j.src.limiter.err()
	case errors.Is(err, jsontext.ErrDuplicateName):
		return errors.New("duplicate JSON object key")
	case errors.As(err, &syntax):
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return errors.New("unterminated JSON input")
		}
		return fmt.Errorf("malformed JSON at byte offset %d", syntax.ByteOffset)
	}
	return err
}

func (j *jsonInput) close() error {
	j.d.budget.Release(j.charge)
	j.charge = 0
	j.workspace.release()
	if j.src == nil {
		return nil
	}
	err := j.src.close()
	j.src = nil
	return err
}
