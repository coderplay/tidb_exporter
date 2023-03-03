// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
)

func TestScrapeGlobalVariables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening a stub database connection: %s", err)
	}
	defer db.Close()

	columns := []string{"Variable_name", "Value"}
	rows := sqlmock.NewRows(columns).
		AddRow("tidb_gc_life_time", "168h").
		AddRow("tls_version", "TLSv1,TLSv1.1,TLSv1.2"). // literal, skip
		AddRow("tidb_enable_async_commit", "on").
		AddRow("tidb_query_log_max_len", "4096").
		AddRow("tidb_rc_write_check_ts", "off").
		AddRow("tidb_server_memory_limit_sess_min_size", "134217728").
		AddRow("tidb_max_tiflash_threads", "-1").
		AddRow("lower_case_table_names", "2").          // noop for tidb, skip
		AddRow("innodb_default_row_format", "dynamic"). // literal, skip
		AddRow("tidb_init_chunk_size", "32").
		AddRow("tidb_replica_read", "leader").        // literal, skip
		AddRow("rpl_semi_sync_slave_enabled", "OFF"). // noop for tidb, skip
		AddRow("innodb_open_files", "2000").          // noop for tidb, skip
		AddRow("tidb_persist_analyze_options", "ON").
		AddRow("version", "5.7.25-TiDB-v6.5.0").
		AddRow("version_comment", "TiDB Server (Apache License 2.0) Enterprise Edition, MySQL 5.7 compatible")
	mock.ExpectQuery(globalVariablesQuery).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		if err = (ScrapeGlobalVariables{}).Scrape(context.Background(), db, ch, log.NewNopLogger()); err != nil {
			t.Errorf("error calling function on test: %s", err)
		}
		close(ch)
	}()

	counterExpected := []MetricResult{
		{labels: labelMap{}, value: 604800, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: 1, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: 4096, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: 0, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: 134217728, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: -1, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: 32, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{}, value: 1, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"version": "5.7.25-TiDB-v6.5.0", "version_comment": "TiDB Server (Apache License 2.0) Enterprise Edition, MySQL 5.7 compatible"}, value: 1, metricType: dto.MetricType_GAUGE},
	}
	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range counterExpected {
			got := readMetric(<-ch)
			convey.So(got, convey.ShouldResemble, expect)
		}
	})

	// Ensure all SQL queries were executed
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
