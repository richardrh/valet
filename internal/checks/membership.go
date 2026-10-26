package checks

// literalSet accelerates homogeneous primitive lists only when scalar identity
// equals the existing comparator. Cross-domain coercions keep the linear path.
type literalSet struct {
	kind   valueKind
	values map[scalar]struct{}
}

func (p *Program) membershipSet(values []scalar, numeric bool) (*literalSet, error) {
	if len(values) < 8 {
		return nil, nil
	}
	kind := values[0].kind
	switch kind {
	case valString:
		if numeric {
			return nil, nil
		}
	case valInt, valUint, valFloat, valBool:
	default:
		return nil, nil
	}
	for _, value := range values {
		if value.kind != kind {
			return nil, nil
		}
	}
	if err := p.reserve(int64(len(values))*bytesPerOperand, "membership lookup index"); err != nil {
		return nil, err
	}
	set := &literalSet{kind: kind, values: make(map[scalar]struct{}, len(values))}
	for _, value := range values {
		if kind == valFloat && !finite(value.f) {
			continue
		}
		set.values[value] = struct{}{}
	}
	return set, nil
}
func (s *literalSet) lookup(v scalar) (found, handled bool) {
	if s == nil || v.kind != s.kind {
		return false, false
	}
	if v.kind == valFloat && !finite(v.f) {
		return false, true
	}
	_, found = s.values[v]
	return found, true
}
