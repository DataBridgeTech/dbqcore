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

package profilers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/DataBridgeTech/dbqcore"
)

type BaseProfiler struct {
	profilerImpl dbqcore.DbqDataProfiler
	logger       *slog.Logger
}

func NewBaseProfiler(profiler dbqcore.DbqDataProfiler, logger *slog.Logger) *BaseProfiler {
	return &BaseProfiler{
		profilerImpl: profiler,
		logger:       logger,
	}
}

func (p *BaseProfiler) ProfileDataset(ctx context.Context, dataset string, sample bool, maxConcurrent int, collectErrors bool) (*dbqcore.TableMetrics, error) {
	startTime := time.Now()
	taskPool := dbqcore.NewTaskPool(maxConcurrent, p.logger)

	var databaseName, tableName string
	parts := strings.SplitN(dataset, ".", 2)
	if len(parts) == 2 {
		databaseName = strings.TrimSpace(parts[0])
		tableName = strings.TrimSpace(parts[1])
	}

	var metricsLock sync.Mutex
	metrics := &dbqcore.TableMetrics{
		ProfiledAt:     time.Now().Unix(),
		TableName:      tableName,
		DatabaseName:   databaseName,
		ColumnsMetrics: make(map[string]*dbqcore.ColumnMetrics),
	}

	totalRows, err := p.profilerImpl.GetTotalRows(ctx, dataset)
	if err != nil {
		return nil, fmt.Errorf("failed to get total row count for %s: %w", dataset, err)
	}
	metrics.TotalRows = totalRows

	columnsToProcess, err := p.profilerImpl.GetColumns(ctx, databaseName, tableName)
	if err != nil {
		return metrics, err
	}

	if len(columnsToProcess) == 0 {
		p.logger.Warn("no columns found for table, returning basic info", "dataset", dataset)
		metrics.ProfilingDurationMs = time.Since(startTime).Milliseconds()
		return metrics, nil
	}

	p.logger.Debug(fmt.Sprintf("found %d columns to process", len(columnsToProcess)))

	if sample {
		taskPool.Enqueue("task:sampling", func() error {
			sampleData, err := p.profilerImpl.GetSampleData(ctx, dataset)
			if err != nil {
				p.logger.Warn("failed to sample data", slog.String("dataset", tableName), slog.Any("error", err))
				return nil
			}
			metricsLock.Lock()
			metrics.RowsSample = sampleData
			metricsLock.Unlock()
			return nil
		})
	}

	for _, col := range columnsToProcess {
		column := col
		var colWg sync.WaitGroup
		var colMetricsLock sync.Mutex
		colStartTime := time.Now()

		p.logger.Debug("start column processing",
			"col_name", column.Name,
			"col_type", column.Type)

		colMetrics := &dbqcore.ColumnMetrics{
			ColumnName:     column.Name,
			DataType:       column.Type,
			ColumnComment:  column.Comment,
			ColumnPosition: column.Position,
		}

		taskIdPrefix := fmt.Sprintf("task:%s:", column.Name)

		enqueueTask(taskPool, &colWg, taskIdPrefix+"null_count", func() error {
			nullCount, err := p.profilerImpl.GetNullCount(ctx, dataset, column)
			if err != nil {
				p.logger.Warn("failed to get NULL count", "error", err.Error(), "col_name", column.Name)
				return err
			}
			colMetricsLock.Lock()
			colMetrics.NullCount = nullCount
			colMetricsLock.Unlock()
			return nil
		})

		if p.profilerImpl.IsStringType(column.Type) {
			enqueueTask(taskPool, &colWg, taskIdPrefix+"blank_count", func() error {
				blankCount, err := p.profilerImpl.GetBlankCount(ctx, dataset, column)
				if err != nil {
					p.logger.Warn("failed to get blank count", "error", err.Error(), "col_name", column.Name)
					return err
				}
				colMetricsLock.Lock()
				colMetrics.BlankCount = &blankCount
				colMetricsLock.Unlock()
				return nil
			})
		}

		if p.profilerImpl.IsNumericType(column.Type) {
			enqueueTask(taskPool, &colWg, taskIdPrefix+"num_stats", func() error {
				numericStats, err := p.profilerImpl.GetNumericStats(ctx, dataset, column)
				if err != nil {
					p.logger.Warn("failed to get numeric aggregates", "error", err.Error(), "col_name", column.Name)
					return err
				}
				colMetricsLock.Lock()
				colMetrics.MinValue = numericStats.MinValue
				colMetrics.MaxValue = numericStats.MaxValue
				colMetrics.AvgValue = numericStats.AvgValue
				colMetrics.StddevValue = numericStats.StddevValue
				colMetricsLock.Unlock()
				return nil
			})
		}

		enqueueTask(taskPool, &colWg, taskIdPrefix+"mfv", func() error {
			mfv, err := p.profilerImpl.GetMostFrequentValue(ctx, dataset, column)
			if err != nil {
				p.logger.Warn("failed to get most frequent value", "error", err.Error(), "col_name", column.Name)
				return err
			}
			colMetricsLock.Lock()
			colMetrics.MostFrequentValue = mfv
			colMetricsLock.Unlock()
			return nil
		})

		go func() {
			colWg.Wait()
			colMetrics.ProfilingDurationMs = time.Since(colStartTime).Milliseconds()

			metricsLock.Lock()
			metrics.ColumnsMetrics[column.Name] = colMetrics
			metricsLock.Unlock()

			p.logger.Debug("finished processing column",
				"col_name", column.Name,
				"proc_duration_ms", colMetrics.ProfilingDurationMs)
		}()
	}

	taskPool.Join()

	metrics.ProfilingDurationMs = time.Since(startTime).Milliseconds()

	if collectErrors {
		metrics.DbqErrors = taskPool.Errors()
	}

	p.logger.Debug("finished data profiling for table",
		"dataset", dataset,
		"profile_duration_ms", metrics.ProfilingDurationMs)

	return metrics, nil
}

func enqueueTask(taskPool *dbqcore.TaskPool, subsetWg *sync.WaitGroup, taskId string, task func() error) {
	subsetWg.Add(1)
	taskPool.Enqueue(taskId, func() error {
		defer subsetWg.Done()
		return task()
	})
}
