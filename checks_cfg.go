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
	"gopkg.in/yaml.v3"
	"os"
)

type OnFailAction string

const (
	OnFailActionError   OnFailAction = "error"
	OnFailActionWarning OnFailAction = "warn"
)

type ChecksFileConfig struct {
	Version     string           `yaml:"version"`
	Validations []ValidationRule `yaml:"validations"`
}

type ValidationRule struct {
	Dataset string             `yaml:"dataset"`
	Where   string             `yaml:"where,omitempty"` // optional, applies for all checks
	Checks  []DataQualityCheck `yaml:"checks"`
}

type DataQualityCheck struct {
	ID          string       `yaml:"id"`
	Description string       `yaml:"description,omitempty"` // optional
	OnFail      OnFailAction `yaml:"on_fail,omitempty"`     // optional (error, warn)
	Query       string       `yaml:"query,omitempty"`       // optional raw query
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
