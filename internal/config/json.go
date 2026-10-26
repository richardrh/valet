package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// decodeJSONDocument converts strict JSON directly into the same yaml.Node
// representation used by YAML configuration. This accepts the full JSON string
// escape grammar while preserving object order and exact number spellings.
func decodeJSONDocument(input []byte) (yaml.Node, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	root, err := decodeJSONValue(decoder)
	if err != nil {
		return yaml.Node{}, err
	}
	if token, trailingErr := decoder.Token(); trailingErr != io.EOF {
		if trailingErr != nil {
			return yaml.Node{}, trailingErr
		}
		return yaml.Node{}, fmt.Errorf("configuration must contain exactly one document; found trailing token %v", token)
	}
	return yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{root}}, nil
}

func decodeJSONValue(decoder *json.Decoder) (*yaml.Node, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			return decodeJSONObject(decoder)
		case '[':
			return decodeJSONArray(decoder)
		default:
			return nil, fmt.Errorf("unexpected JSON delimiter %q", value)
		}
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}, nil
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(value)}, nil
	case string:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}, nil
	case json.Number:
		tag := "!!int"
		if strings.ContainsAny(value.String(), ".eE") {
			tag = "!!float"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: value.String()}, nil
	default:
		return nil, fmt.Errorf("unsupported JSON token %T", token)
	}
}

func decodeJSONObject(decoder *json.Decoder) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("JSON object key must be a string")
		}
		value, err := decodeJSONValue(decoder)
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name},
			value,
		)
	}
	if token, err := decoder.Token(); err != nil {
		return nil, err
	} else if token != json.Delim('}') {
		return nil, fmt.Errorf("unexpected JSON object terminator %v", token)
	}
	return node, nil
}

func decodeJSONArray(decoder *json.Decoder) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	for decoder.More() {
		value, err := decodeJSONValue(decoder)
		if err != nil {
			return nil, err
		}
		node.Content = append(node.Content, value)
	}
	if token, err := decoder.Token(); err != nil {
		return nil, err
	} else if token != json.Delim(']') {
		return nil, fmt.Errorf("unexpected JSON array terminator %v", token)
	}
	return node, nil
}
