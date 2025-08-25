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
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type CheckScope string

const (
	ScopeSchema CheckScope = "schema"
	ScopeTable  CheckScope = "table"
	ScopeColumn CheckScope = "column"
)

type BetweenRange struct {
	Min interface{}
	Max interface{}
}

type CheckExpression struct {
	FunctionName       string
	FunctionParameters []string
	Scope              CheckScope
	Operator           string
	ThresholdValue     interface{}
}

var (
	tableScopeFunctions = map[string]bool{
		"row_count": true,
		"raw_query": true,
	}

	columnScopeFunctions = map[string]bool{
		"not_null":   true,
		"uniqueness": true,
		"freshness":  true,
		"min":        true,
		"max":        true,
		"sum":        true,
		"stddev":     true,
	}

	schemaScopeFunctions = map[string]bool{
		"expect_columns":         true,
		"expect_columns_ordered": true,
		"columns_not_present":    true,
	}
)

func ParseCheckExpression(expression string) (*CheckExpression, error) {
	expression = strings.TrimSpace(expression)

	if expression == "" {
		return nil, fmt.Errorf("empty expression")
	}

	check := &CheckExpression{
		FunctionParameters: []string{},
	}

	betweenRegex := regexp.MustCompile(`^(\w+)(?:\((.*?)\))?\s+between\s+(.+)\s+and\s+(.+)$`)
	operatorRegex := regexp.MustCompile(`^(\w+)(?:\((.*?)\))?\s*([<>=!]+)\s*(.+)$`)
	functionOnlyRegex := regexp.MustCompile(`^(\w+)(?:\((.*?)\))?$`)

	if matches := betweenRegex.FindStringSubmatch(expression); matches != nil {
		check.FunctionName = matches[1]
		check.Operator = "between"

		if matches[2] != "" {
			check.FunctionParameters = parseParameters(matches[2])
		}

		minVal, err := parseValue(strings.TrimSpace(matches[3]))
		if err != nil {
			return nil, fmt.Errorf("failed to parse min value: %v", err)
		}

		maxVal, err := parseValue(strings.TrimSpace(matches[4]))
		if err != nil {
			return nil, fmt.Errorf("failed to parse max value: %v", err)
		}

		check.ThresholdValue = BetweenRange{Min: minVal, Max: maxVal}

	} else if matches := operatorRegex.FindStringSubmatch(expression); matches != nil {
		check.FunctionName = matches[1]
		check.Operator = matches[3]

		if matches[2] != "" {
			check.FunctionParameters = parseParameters(matches[2])
		}

		val, err := parseValue(strings.TrimSpace(matches[4]))
		if err != nil {
			return nil, fmt.Errorf("failed to parse threshold value: %v", err)
		}
		check.ThresholdValue = val

	} else if matches := functionOnlyRegex.FindStringSubmatch(expression); matches != nil {
		check.FunctionName = matches[1]
		check.Operator = ""

		if matches[2] != "" {
			check.FunctionParameters = parseParameters(matches[2])
		}

	} else {
		return nil, fmt.Errorf("invalid expression format: %s", expression)
	}

	check.Scope = inferScope(check.FunctionName)

	return check, nil
}

func parseParameters(paramStr string) []string {
	if paramStr == "" {
		return []string{}
	}

	params := strings.Split(paramStr, ",")
	for i, param := range params {
		params[i] = strings.TrimSpace(param)
	}

	return params
}

func parseValue(valueStr string) (interface{}, error) {
	valueStr = strings.TrimSpace(valueStr)

	if valueStr == "" {
		return nil, fmt.Errorf("empty value")
	}

	if strings.HasSuffix(valueStr, "d") {
		return valueStr, nil
	}

	if strings.Contains(valueStr, ".") {
		if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
			return floatVal, nil
		}
	}

	if intVal, err := strconv.Atoi(valueStr); err == nil {
		return intVal, nil
	}

	return valueStr, nil
}

func inferScope(functionName string) CheckScope {
	if tableScopeFunctions[functionName] {
		return ScopeTable
	}

	if columnScopeFunctions[functionName] {
		return ScopeColumn
	}

	if schemaScopeFunctions[functionName] {
		return ScopeSchema
	}

	return ScopeColumn
}
