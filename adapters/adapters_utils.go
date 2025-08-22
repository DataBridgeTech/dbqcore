package adapters

import (
	"fmt"
	"strings"

	"github.com/DataBridgeTech/dbqcore"
)

func requireParameter(functionName string, params []string) error {
	if len(params) == 0 {
		return fmt.Errorf("%s check requires a column parameter", functionName)
	}
	return nil
}

func createMockParsedCheck(functionName string, parameters []string, operator string, thresholdValue interface{}) *dbqcore.CheckExpression {
	return &dbqcore.CheckExpression{
		FunctionName:       functionName,
		FunctionParameters: parameters,
		Scope:              dbqcore.ScopeColumn,
		Operator:           operator,
		ThresholdValue:     thresholdValue,
	}
}

func extractDatabaseAndTableFromDataset(dataset string) (string, string, error) {
	parts := strings.Split(dataset, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("dataset must be in format database.table")
	}
	return parts[0], parts[1], nil
}
