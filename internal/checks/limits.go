package checks

// Managed-memory charges are conservative upper estimates of Go heap use for
// run-scoped compiled state. They are not measured sizes; they only need to
// grow with the configuration so a hostile configuration fails the budget
// instead of the process.
const (
	// bytesPerCompiledItem covers one rule, dataset rule, stat or schema field:
	// its compiled struct, map entries and summary counters.
	bytesPerCompiledItem = 4096
	// bytesPerCheckDefinition covers one configured check before operands.
	bytesPerCheckDefinition = 4096
	// bytesPerPatternByte covers regexp parse trees per pattern source byte.
	bytesPerPatternByte = 512
	// bytesPerLiteral covers one bound scalar including its text header.
	bytesPerLiteral = 128
	// bytesPerOperand covers one bound column operand or its bound literals.
	bytesPerOperand = 256
	// bytesPerDateFormat and bytesPerDateFormatByte cover source and translated
	// layouts retained by checks and derived column contexts.
	bytesPerDateFormat     = 128
	bytesPerDateFormatByte = 8
	// bytesPerSchemaOperand covers one schema_matches column comparison.
	bytesPerSchemaOperand = 512
	// bytesPerConditionNode covers one compiled `when` node.
	bytesPerConditionNode = 1024
	// bytesPerRegexInstruction covers one compiled regexp instruction.
	bytesPerRegexInstruction = 256
	// bytesPerResultRow covers the one-byte bucket; fixed result ownership
	// bookkeeping is charged separately.
	bytesPerResultRow = 1
	// bytesPerResultIssue covers one data.Issue value and its string headers.
	bytesPerResultIssue = 256
	// indexBytesPerKeyByte covers key string storage plus growth overlap.
	indexBytesPerKeyByte = 2
	// indexBytesPerEntry covers map bucket load factor and overflow buckets.
	indexBytesPerEntry = 256
	// minKeyBufferBytes is the smallest key scratch allocation.
	minKeyBufferBytes = 256
)

// Evaluation and arithmetic envelopes.
const (
	// maxConditionDepth and maxConditionNodes bound one `when` expression.
	maxConditionDepth = 16
	maxConditionNodes = 1024
	// rowsPerCancellationCheck is how often Evaluate polls its context.
	rowsPerCancellationCheck = 1024
	// maxRatBits bounds numerator/denominator width of exact thresholds and
	// accumulators.
	maxRatBits = 256
	// maxDecimalTextBytes and maxDecimalExponent bound exact decimal parsing.
	maxDecimalTextBytes = 1024
	maxDecimalExponent  = 1000
	// maxRegexInstructions saturates the regexp cost estimate.
	maxRegexInstructions = 1 << 40
)
