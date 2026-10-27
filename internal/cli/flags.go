package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"

	"github.com/richardrh/valet/internal/config"
)

var (
	logFormats      = []string{"text", "json"}
	logLevelNames   = []string{"debug", "info", "warn", "error"}
	planFormats     = []string{"json", "pipeline"}
	schemaFormats   = []string{"json", "text"}
	longFlagAliases = map[string]string{"p": "pipeline", "i": "input", "o": "output", "v": "verbose"}
)

// commands lists each command's flags; a command rejects every flag it does
// not register.
var commands = map[string]func(*flag.FlagSet, *options){
	"validate": func(f *flag.FlagSet, o *options) {
		o.pipelineFlags(f)
		o.inputFlags(f)
		o.outputFlags(f)
		o.executionFlags(f)
		o.optionFlags(f)
		f.BoolVar(&o.plain, "plain", false, "")
		f.BoolVar(&o.stats, "stats", false, "")
		f.Func("json-output", "", nonEmpty(&o.jsonOutput, "--json-output requires a nonempty path or '-'"))
		o.logFlags(f)
	},
	"plan": func(f *flag.FlagSet, o *options) {
		o.pipelineFlags(f)
		o.inputFlags(f)
		o.outputFlags(f)
		o.executionFlags(f)
		o.optionFlags(f)
		f.Var(oneOf(&o.format, planFormats), "format", "")
		o.logFlags(f)
	},
	"schema": func(f *flag.FlagSet, o *options) {
		o.pipelineFlags(f)
		o.inputFlags(f)
		f.Func("memory-budget", "", nonEmpty(&o.memory, "--memory-budget requires a size"))
		f.Var(oneOf(&o.format, schemaFormats), "format", "")
		o.logFlags(f)
	},
}

func (o *options) pipelineFlags(f *flag.FlagSet) {
	for _, name := range []string{"p", "pipeline"} {
		f.StringVar(&o.pipeline, name, "", "")
	}
	// Recognised only to explain that it was removed.
	f.BoolVar(&o.legacyStdout, "stdout", false, "")
}

func (o *options) inputFlags(f *flag.FlagSet) {
	set := func(s string) error {
		switch {
		case strings.TrimSpace(s) == "":
			return errors.New("--input requires a nonempty URI")
		case s == "-":
			return errors.New("--input - (stdin) is unsupported: inputs must be replayable files or s3:// objects")
		}
		o.input = s
		return nil
	}
	f.Func("i", "", set)
	f.Func("input", "", set)
}

func (o *options) outputFlags(f *flag.FlagSet) {
	set := func(s string) error {
		switch {
		case strings.TrimSpace(s) == "":
			return errors.New("--output requires a nonempty URI")
		case s == "-":
			return errors.New("--output - is unsupported: bucket outputs need a directory or s3:// prefix; use --json-output - for summary JSON")
		}
		o.output = s
		return nil
	}
	f.Func("o", "", set)
	f.Func("output", "", set)
}

func (o *options) executionFlags(f *flag.FlagSet) {
	f.Func("memory-budget", "", nonEmpty(&o.memory, "--memory-budget requires a size"))
	f.Func("batch-target", "", nonEmpty(&o.batch, "--batch-target requires a size"))
	f.IntVar(&o.workers, "workers", 0, "")
}

func (o *options) optionFlags(f *flag.FlagSet) {
	f.Var(oneOf(&o.outputMode, config.OutputModes), "output-mode", "")
	f.Var(oneOf(&o.parquetStats, config.ParquetStatisticsPolicies), "parquet-stats", "")
}

func (o *options) logFlags(f *flag.FlagSet) {
	f.Var(oneOf(&o.logLevel, logLevelNames), "log-level", "")
	f.Var(oneOf(&o.logFormat, logFormats), "log-format", "")
	for _, name := range []string{"v", "verbose"} {
		f.BoolVar(&o.verbose, name, false, "")
	}
}

// choice is a flag restricted to fixed values.
type choice struct {
	value   *string
	allowed []string
}

func oneOf(value *string, allowed []string) choice { return choice{value, allowed} }

func (c choice) String() string {
	if c.value == nil {
		return ""
	}
	return *c.value
}

func (c choice) Set(s string) error {
	if !slices.Contains(c.allowed, s) {
		return fmt.Errorf("must be %s", config.OrList(c.allowed))
	}
	*c.value = s
	return nil
}

func nonEmpty(value *string, message string) func(string) error {
	return func(s string) error {
		if strings.TrimSpace(s) == "" {
			return errors.New(message)
		}
		*value = s
		return nil
	}
}

var (
	undefinedFlag = regexp.MustCompile(`^flag provided but not defined: -(.+)$`)
	missingValue  = regexp.MustCompile(`^flag needs an argument: -(.+)$`)
	invalidValue  = regexp.MustCompile(`^invalid (?:value|boolean value) "(.*)" for flag -([^:]+): (.*)$`)
)

// flagError rewrites the flag package's messages into the CLI's style and,
// for a flag another command accepts, says which flags this command takes.
func flagError(command string, f *flag.FlagSet, err error) error {
	msg := err.Error()
	switch {
	case undefinedFlag.MatchString(msg):
		name := undefinedFlag.FindStringSubmatch(msg)[1]
		if !anyCommandAccepts(name) {
			return fmt.Errorf("unknown flag %s; see valet --help", dashed(name))
		}
		return fmt.Errorf("%s is not a %s option; %s accepts %s", dashed(name), command, command, acceptedFlags(f))
	case missingValue.MatchString(msg):
		return fmt.Errorf("%s needs a value", dashed(missingValue.FindStringSubmatch(msg)[1]))
	case invalidValue.MatchString(msg):
		m := invalidValue.FindStringSubmatch(msg)
		value, name, reason := m[1], m[2], m[3]
		switch {
		case strings.HasPrefix(reason, "must be "):
			return fmt.Errorf("--%s %s, not %q", longName(name), reason, value)
		case strings.HasPrefix(reason, "--"):
			return errors.New(reason)
		case strings.Contains(reason, "parse error"):
			return fmt.Errorf("--%s must be a number, not %q", longName(name), value)
		}
	}
	return err
}

func anyCommandAccepts(name string) bool {
	for _, register := range commands {
		f := flag.NewFlagSet("", flag.ContinueOnError)
		register(f, &options{})
		if f.Lookup(name) != nil {
			return true
		}
	}
	return false
}

// acceptedFlags lists a command's flags by long name, in registration order.
func acceptedFlags(f *flag.FlagSet) string {
	var names []string
	f.VisitAll(func(fl *flag.Flag) {
		if _, short := longFlagAliases[fl.Name]; short || fl.Name == "stdout" {
			return
		}
		names = append(names, "--"+fl.Name)
	})
	order := map[string]int{"--pipeline": 0, "--input": 1, "--output": 2}
	slices.SortStableFunc(names, func(a, b string) int {
		ra, aok := order[a]
		rb, bok := order[b]
		switch {
		case aok && bok:
			return ra - rb
		case aok:
			return -1
		case bok:
			return 1
		}
		return 0
	})
	return strings.Join(names, ", ")
}

func longName(name string) string {
	if long, ok := longFlagAliases[name]; ok {
		return long
	}
	return name
}

func dashed(name string) string {
	if len(name) == 1 {
		return "-" + name
	}
	return "--" + name
}

// newFlagSet builds the flag set for command with errors and usage silenced;
// parse reports errors itself.
func newFlagSet(command string, o *options) (*flag.FlagSet, bool) {
	register, ok := commands[command]
	if !ok {
		return nil, false
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	register(f, o)
	return f, true
}
