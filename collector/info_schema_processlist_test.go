// Copyright 2021 The Prometheus Authors
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
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
	"gopkg.in/alecthomas/kingpin.v2"
)

func TestScrapeProcesslist(t *testing.T) {
	_, err := kingpin.CommandLine.Parse([]string{
		"--collect.info_schema.processlist.processes_by_user",
		"--collect.info_schema.processlist.processes_by_client",
	})
	if err != nil {
		t.Fatal(err)
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening a stub database connection: %s", err)
	}
	defer db.Close()

	query := fmt.Sprintf(infoSchemaProcesslistQuery, 0)
	columns := []string{"instance", "user", "host", "db", "time", "state", "mem", "disk"}
	rows := sqlmock.NewRows(columns).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "observer", "10.120.252.10", "employee", 1425, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "observer", "10.120.252.10", "employee", 1279, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 163, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 417, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 1668, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1395, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "admin", "10.120.252.16", "mysql", 549, "in transaction", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 423, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1485, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 439, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1320, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 840, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 518, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 608, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 35, "autocommit", 8192, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1227, "in transaction", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 11, "autocommit", 8192, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1741, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.37", "employee", 1262, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 625, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 662, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1464, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 241, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 255, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 0, "autocommit", 8192, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 122, "in transaction", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 1037, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.37", "employee", 1, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1678, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 327, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1734, "in transaction", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 920, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 303, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 849, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.89", "mysql", 666, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1257, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 103, "in transaction", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 257, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 547, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "observer", "10.120.252.10", "employee", 1551, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "admin", "10.120.252.16", "mysql", 487, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 67, "autocommit", 8192, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 1643, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 219, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 557, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "observer", "10.120.252.79", "employee", 95, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1196, "in transaction", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 1032, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 160, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 809, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1092, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 566, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "admin", "10.120.252.16", "mysql", 585, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1236, "in transaction", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "observer", "10.120.252.79", "employee", 217, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 929, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.89", "mysql", 803, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 264, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 634, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1082, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 0, "autocommit", 2079772, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 250, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 348, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 52, "autocommit", 8192, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 103, "in transaction", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1244, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 312, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 332, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "observer", "10.120.252.79", "employee", 198, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 26, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 976, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 0, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "observer", "10.120.252.10", "employee", 1563, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 994, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 1235, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 147, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 148, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 240, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1589, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 1206, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 1, "in transaction", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 918, "autocommit", 0, 0).
		AddRow("tidb-0.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 747, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 657, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1114, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1700, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 676, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.37", "employee", 366, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1597, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1651, "in transaction", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 8, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 484, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1653, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 505, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "observer", "10.120.252.79", "employee", 189, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 565, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 280, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 764, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 662, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 843, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1703, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 117, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 775, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 2, "in transaction", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 814, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1130, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 0, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 886, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 6, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 556, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1424, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 17, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 0, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 856, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.37", "employee", 1322, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1313, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 965, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 792, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 946, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 715, "in transaction", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "admin", "10.120.252.32", "mysql", 989, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 750, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 191, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1624, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "observer", "10.120.252.79", "employee", 105, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 365, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1050, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 0, "autocommit", 2054, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1118, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 192, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1591, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 140, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 1211, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1609, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 194, "in transaction", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 452, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1607, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 443, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 974, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 1153, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.89", "mysql", 852, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 65, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 747, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 695, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "in transaction", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 637, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "observer", "10.120.252.79", "employee", 195, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 111, "in transaction", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 0, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 702, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 904, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 331, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 111, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 99, "in transaction", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1497, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 743, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 1228, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 677, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 5, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1675, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 8192, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "admin", "10.120.252.16", "mysql", 465, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 294, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 1216, "in transaction", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 884, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1629, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "observer", "10.120.252.10", "employee", 1375, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 289, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 1132, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1430, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 3, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 32, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 96, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 443, "in transaction", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.89", "mysql", 1142, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 956, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 1703, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 1013, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 203, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 174, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 409, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 857, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-1.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 858, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.89", "mysql", 957, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 392, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 179, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 271, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "runner", "10.120.252.37", "employee", 1389, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1228, "in transaction", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 46, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 677, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1650, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "admin", "10.120.252.32", "mysql", 945, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 13, "autocommit", 8192, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 360, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 0, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 1186, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 575, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 662, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 576, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 552, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 0, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1576, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 243, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 621, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 602, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 703, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1619, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 258, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "runner", "10.120.252.28", "employee", 910, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 1048, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1218, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1597, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 825, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 236, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1679, "in transaction", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "house", "10.120.252.38", "employee", 1515, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 194, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 706, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "runner", "10.120.252.37", "employee", 1450, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 824, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 981, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1456, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 0, "autocommit", 8192, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1471, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 655, "in transaction", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 375, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1283, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "house", "10.120.252.28", "employee", 58, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "orch", "10.120.252.64", "employee", 1250, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "orch", "10.120.252.36", "employee", 885, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 0, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 696, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 457, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 949, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 450, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "runner", "10.120.252.37", "employee", 1033, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 908, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 188, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "ops", "10.120.252.14", "employee", 1471, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "runner", "10.120.252.67", "employee", 24, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 750, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "admin", "10.120.252.16", "mysql", 461, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-reader000", "10.120.252.70", "mysql", 467, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.60", "mysql", 619, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.87", "employee", 62, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "apiuser", "10.120.252.79", "employee", 1545, "autocommit", 0, 0).
		AddRow("tidb-3.tidb-peer.tidb.svc:10080", "r-applier000", "10.120.252.32", "mysql", 558, "autocommit", 0, 0)
	mock.ExpectQuery(sanitizeQuery(query)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		if err = (ScrapeProcesslist{}).Scrape(context.Background(), db, ch, log.NewNopLogger()); err != nil {
			t.Errorf("error calling function on test: %s", err)
		}
		close(ch)
	}()

	expected := []MetricResult{
		{labels: labelMap{"state": "autocommit"}, value: 241, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"state": "autocommit"}, value: 163667, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"state": "autocommit"}, value: 2147362, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"state": "autocommit"}, value: 0, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"state": "in transaction"}, value: 21, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"state": "in transaction"}, value: 14264, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"state": "in transaction"}, value: 0, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"state": "in transaction"}, value: 0, metricType: dto.MetricType_GAUGE},

		{labels: labelMap{"server": "tidb-0.tidb-peer.tidb.svc:10080"}, value: 88, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"server": "tidb-1.tidb-peer.tidb.svc:10080"}, value: 107, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"server": "tidb-3.tidb-peer.tidb.svc:10080"}, value: 67, metricType: dto.MetricType_GAUGE},

		{labels: labelMap{"client": "10.120.252.10"}, value: 5, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.14"}, value: 10, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.16"}, value: 5, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.28"}, value: 31, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.32"}, value: 30, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.36"}, value: 10, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.37"}, value: 7, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.38"}, value: 20, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.60"}, value: 27, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.64"}, value: 12, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.67"}, value: 13, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.70"}, value: 50, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.79"}, value: 23, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.87"}, value: 14, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"client": "10.120.252.89"}, value: 5, metricType: dto.MetricType_GAUGE},

		{labels: labelMap{"mysql_user": "admin"}, value: 7, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"mysql_user": "apiuser"}, value: 31, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"mysql_user": "house"}, value: 40, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"mysql_user": "observer"}, value: 11, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"mysql_user": "ops"}, value: 10, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"mysql_user": "orch"}, value: 22, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"mysql_user": "r-applier000"}, value: 60, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"mysql_user": "r-reader000"}, value: 50, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"mysql_user": "runner"}, value: 31, metricType: dto.MetricType_GAUGE},

		{labels: labelMap{"db": "employee"}, value: 117, metricType: dto.MetricType_GAUGE},
		{labels: labelMap{"db": "mysql"}, value: 117, metricType: dto.MetricType_GAUGE},
	}
	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			got := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, got)
		}
	})

	// Ensure all SQL queries were executed
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}
