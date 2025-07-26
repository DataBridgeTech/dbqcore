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

import "context"

// DbqConnector is the interface that wraps the basic connector methods.
type DbqConnector interface {
	// Ping checks if the connection to the data source is alive.
	Ping(ctx context.Context) (string, error)

	// ImportDatasets imports datasets from the data source, with an optional filter.
	ImportDatasets(ctx context.Context, filter string) ([]string, error)
}
