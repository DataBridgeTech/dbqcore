// Copyright 2025 The DBQ Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbqcore

import (
	"os"

	"gopkg.in/yaml.v3"
)

type OnFailAction string

const (
	OnFailActionWarn  OnFailAction = "warn"
	OnFailActionError OnFailAction = "error"
)

type ChecksFileConfig struct {
	Version string           `yaml:"version"`
	Rules   []ValidationRule `yaml:"rules"`
}

type ValidationRule struct {
	Dataset string             `yaml:"dataset"`
	Where   string             `yaml:"where,omitempty"`
	Checks  []DataQualityCheck `yaml:"checks"`
}

type DataQualityCheck struct {
	Expression  string       `yaml:"-"`
	Description string       `yaml:"desc,omitempty"`
	OnFail      OnFailAction `yaml:"on_fail,omitempty"`
	Query       string       `yaml:"query,omitempty"`

	// Schema check fields
	SchemaCheck *SchemaCheckConfig `yaml:"schema_check,omitempty"`
	ParsedCheck *CheckExpression   `yaml:"-"`
}

type SchemaCheckConfig struct {
	ExpectColumnsOrdered *ExpectColumnsOrderedConfig `yaml:"expect_columns_ordered,omitempty"`
}

type ExpectColumnsOrderedConfig struct {
	ColumnsOrder []string `yaml:"columns_order"`
}

func (c *DataQualityCheck) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.MappingNode && len(node.Content) >= 2 {
		key := node.Content[0].Value
		value := node.Content[1]

		if key == "schema_check" {
			// Handle schema_check format
			type tempCheck struct {
				SchemaCheck *SchemaCheckConfig `yaml:"schema_check"`
				Desc        string             `yaml:"desc,omitempty"`
				OnFail      OnFailAction       `yaml:"on_fail,omitempty"`
			}

			// Decode the entire node to get all fields
			var temp tempCheck
			if err := node.Decode(&temp); err != nil {
				return err
			}

			c.SchemaCheck = temp.SchemaCheck
			c.Description = temp.Desc
			c.OnFail = temp.OnFail

			// For schema checks, we set the Expression but don't create ParsedCheck
			if c.SchemaCheck != nil && c.SchemaCheck.ExpectColumnsOrdered != nil {
				c.Expression = "expect_columns_ordered"
				// ParsedCheck remains nil - the SchemaCheck field contains all needed info
			}
		} else if key == "raw_query" {
			c.Expression = key
			var rawQueryCheck struct {
				Desc   string       `yaml:"desc,omitempty"`
				Query  string       `yaml:"query"`
				OnFail OnFailAction `yaml:"on_fail,omitempty"`
			}
			if err := value.Decode(&rawQueryCheck); err != nil {
				return err
			}
			c.Description = rawQueryCheck.Desc
			c.Query = rawQueryCheck.Query
			c.OnFail = rawQueryCheck.OnFail

			parsedCheck, err := ParseCheckExpression("raw_query")
			if err != nil {
				return err
			}
			c.ParsedCheck = parsedCheck
		} else {
			c.Expression = key

			if value.Kind == yaml.MappingNode {
				var checkDetails struct {
					Desc   string       `yaml:"desc,omitempty"`
					OnFail OnFailAction `yaml:"on_fail,omitempty"`
				}
				if err := value.Decode(&checkDetails); err != nil {
					return err
				}
				c.Description = checkDetails.Desc
				c.OnFail = checkDetails.OnFail
			}

			parsedCheck, err := ParseCheckExpression(key)
			if err != nil {
				return err
			}
			c.ParsedCheck = parsedCheck
		}
	} else if node.Kind == yaml.ScalarNode {
		c.Expression = node.Value
		parsedCheck, err := ParseCheckExpression(node.Value)
		if err != nil {
			return err
		}
		c.ParsedCheck = parsedCheck
	}

	return nil
}

func LoadChecksFileConfig(fileName string) (*ChecksFileConfig, error) {
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		return nil, err
	}

	var cfg ChecksFileConfig
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
