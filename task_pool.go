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
	"io"
	"log/slog"
	"sync"
	"time"
)

type TaskPool struct {
	semaphore chan struct{}
	logger    *slog.Logger
	wg        sync.WaitGroup
	mu        sync.Mutex
	errors    []error
}

func NewTaskPool(poolSize int, logger *slog.Logger) *TaskPool {
	if logger == nil {
		// noop logger by default
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &TaskPool{
		semaphore: make(chan struct{}, poolSize),
		logger:    logger,
	}
}

func (tp *TaskPool) Enqueue(id string, task func() error) {
	tp.wg.Add(1)
	go func() {
		tp.semaphore <- struct{}{}
		defer func() {
			<-tp.semaphore
			tp.wg.Done()
		}()

		tp.logger.Debug("executing task", "task_id", id)
		exeStartTime := time.Now()
		if err := task(); err != nil {
			tp.logger.Error("task failed", "task_id", id, "error", err.Error())
			tp.mu.Lock()
			tp.errors = append(tp.errors, err)
			tp.mu.Unlock()
		}
		elapsed := time.Since(exeStartTime).Milliseconds()
		tp.logger.Debug("completed task", "task_id", id, "elapsed_ms", elapsed)
	}()
}

func (tp *TaskPool) Join() {
	tp.wg.Wait()
}

func (tp *TaskPool) Errors() []error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	errsCopy := make([]error, len(tp.errors))
	copy(errsCopy, tp.errors)
	return errsCopy
}
