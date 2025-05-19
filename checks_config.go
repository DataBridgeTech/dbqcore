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

type OnFailAction string

const (
	OnFailActionError   = "error"
	OnFailActionWarning = "warn"
)

type ChecksConfig struct {
	Version     string       `yaml:"version"`
	Validations []Validation `yaml:"validations"`
}

type Validation struct {
	Dataset string  `yaml:"dataset"`
	Where   string  `yaml:"where,omitempty"` // Optional where clause
	Checks  []Check `yaml:"checks"`
}

type Check struct {
	ID          string       `yaml:"id"`
	Description string       `yaml:"description,omitempty"` // optional
	OnFail      OnFailAction `yaml:"on_fail,omitempty"`     // optional (error, warn)
	Query       string       `yaml:"query,omitempty"`       // optional raw query
}
