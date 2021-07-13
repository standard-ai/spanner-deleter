//
// Copyright 2020 Google LLC
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
//

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	spannerProto "google.golang.org/genproto/googleapis/spanner/v1"
)

// Status is a delete status.
type status int

const (
	statusAnalyzing       status = iota // Status for calculating the total rows in the table.
	statusWaiting                       // Status for waiting for dependent tables being deleted.
	statusDeleting                      // Status for deleting rows.
	statusCascadeDeleting               // Status for deleting rows by parent in cascaded way.
	statusCompleted                     // Status for delete completed.
)

// deleter deletes all rows from the table.
type deleter struct {
	tableName    string
	client       *spanner.Client
	status       status
	column       string
	columnValues []string
	lower        string
	upper        string
	priority     int32

	// Total rows in the table.
	// Once set, we don't update this number even if new rows are added to the table.
	totalRows uint64

	// Remained rows in the table.
	remainedRows uint64
}

// deleteRows deletes rows from the table using PDML.
func (d *deleter) deleteRows(ctx context.Context) error {
	d.status = statusDeleting
	stmt := spanner.NewStatement(d.getDeleteStatementString())
	// low priority https://pkg.go.dev/google.golang.org/genproto/googleapis/spanner/v1#RequestOptions_Priority
	options := spanner.QueryOptions{Priority: spannerProto.RequestOptions_Priority(d.priority)}
	_, err := d.client.PartitionedUpdateWithOptions(ctx, stmt, options)
	return err
}

func (d *deleter) getDeleteStatementString() string {
	stmtStr := fmt.Sprintf("DELETE FROM `%s`", d.tableName)
	stmtStr += d.getStatementStringSuffix()
	return stmtStr
}

func (d *deleter) getStatementStringSuffix() string {
	suffix := ""
	if len(d.columnValues) > 0 {
		suffix = fmt.Sprintf(" WHERE %s IN ('%s')", d.column, strings.Join(d.columnValues, "','"))
		return suffix
	}
	hasLower := d.column != "" && d.lower != ""
	hasUpper := d.column != "" && d.upper != ""
	if hasLower && hasUpper {
		suffix = fmt.Sprintf(" WHERE %s > '%s' AND %s < '%s'", d.column, d.lower, d.column, d.upper)
	} else if hasLower {
		suffix = fmt.Sprintf(" WHERE %s > '%s'", d.column, d.lower)
	} else if hasUpper {
		suffix = fmt.Sprintf(" WHERE %s < '%s'", d.column, d.upper)
	}
	return suffix
}

func (d *deleter) parentDeletionStarted() {
	d.status = statusCascadeDeleting
}

// startRowCountUpdater starts periodical row count in another goroutine.
func (d *deleter) startRowCountUpdater(ctx context.Context) {
	go func() {
		for {
			if d.status == statusCompleted {
				return
			}

			begin := time.Now()

			// Ignore error as it could be a temporal error.
			d.updateRowCount(ctx)

			// Sleep for a while to minimize the impact on CPU usage caused by SELECT COUNT(*) queries.
			time.Sleep(time.Since(begin) * 10)
		}
	}()
}

func (d *deleter) updateRowCount(ctx context.Context) error {
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT COUNT(*) as count FROM `%s` %s", d.tableName, d.getStatementStringSuffix()))
	var count int64

	// Use stale read to minimize the impact on the leader replica.
	txn := d.client.Single().WithTimestampBound(spanner.ExactStaleness(time.Second))
	// low priority https://pkg.go.dev/google.golang.org/genproto/googleapis/spanner/v1#RequestOptions_Priority
	options := spanner.QueryOptions{Priority: spannerProto.RequestOptions_Priority(d.priority)}
	if err := txn.QueryWithOptions(ctx, stmt, options).Do(func(r *spanner.Row) error {
		return r.ColumnByName("count", &count)
	}); err != nil {
		return err
	}

	if d.totalRows == 0 {
		d.totalRows = uint64(count)
	}
	d.remainedRows = uint64(count)

	if count == 0 {
		d.status = statusCompleted
	} else if d.status == statusAnalyzing {
		d.status = statusWaiting
	}

	return nil
}
