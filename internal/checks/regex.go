package checks

import (
	"regexp"
	"regexp/syntax"
)

func (c *compiler) compileRegex(pattern string) (*regexp.Regexp, error) {
	// The program's base reservation covers both parse trees. Repetition
	// expands only after its conservative instruction cost is reserved.
	tree, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil, err
	}
	if err := c.p.reserve(regexInstructions(tree)*bytesPerRegexInstruction, "compiled regular expression"); err != nil {
		return nil, err
	}
	return regexp.Compile(pattern)
}

// regexInstructions estimates compiled instructions, saturating at
// maxRegexInstructions. Go's parser already caps repeat counts at 1000, so
// saturation only guards nested repeats.
func regexInstructions(re *syntax.Regexp) int64 {
	n := int64(4 + len(re.Rune))
	for _, child := range re.Sub {
		n = min(n+regexInstructions(child), maxRegexInstructions)
	}
	if re.Op == syntax.OpRepeat {
		copies := re.Max
		if copies < 0 {
			copies = re.Min + 1
		}
		copies = max(copies, 1)
		if n > maxRegexInstructions/int64(copies) {
			return maxRegexInstructions
		}
		n *= int64(copies)
	}
	return n
}
