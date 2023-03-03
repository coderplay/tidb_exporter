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

// Scrape `information_schema.processlist`.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
	"reflect"
	"sort"
)

const infoSchemaProcesslistQuery = `
		  SELECT
		    instance,
		    user,
		    SUBSTRING_INDEX(host, ':', 1) AS host,
		    COALESCE(db,'') AS db,
		    time,
		    state,
		    mem,
		    disk
		  FROM information_schema.cluster_processlist
		  WHERE ID != connection_id()
		    AND TIME >= %d
		`

// Tunable flags.
var (
	processlistMinTime = kingpin.Flag(
		"collect.info_schema.processlist.min_time",
		"Minimum time a thread must be in each state to be counted",
	).Default("0").Int()
	processesByUserFlag = kingpin.Flag(
		"collect.info_schema.processlist.processes_by_user",
		"Enable collecting the number of processes by user",
	).Default("true").Bool()
	processesByClientFlag = kingpin.Flag(
		"collect.info_schema.processlist.processes_by_client",
		"Enable collecting the number of processes by client host",
	).Default("true").Bool()
	processesByServerFlag = kingpin.Flag(
		"collect.info_schema.processlist.processes_by_server",
		"Enable collecting the number of processes by tidb server host",
	).Default("true").Bool()
	processesByDBFlag = kingpin.Flag(
		"collect.info_schema.processlist.processes_by_db",
		"Enable collecting the number of processes by database",
	).Default("true").Bool()
)

// Metric descriptors.
var (
	processlistCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "threads"),
		"The number of threads (connections) split by current state.",
		[]string{"state"}, nil)
	processlistTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "threads_seconds"),
		"The number of seconds threads (connections) have used split by current state.",
		[]string{"state"}, nil)
	processlistMemDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "memory_bytes"),
		"The number of bytes memory have allocated split by current state.",
		[]string{"state"}, nil)
	processlistDiskDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "disk_bytes"),
		"The number of disk bytes have used split by current state.",
		[]string{"state"}, nil)
	processesByUserDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "processes_by_user"),
		"The number of processes by user.",
		[]string{"mysql_user"}, nil)
	processesByDBDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "processes_by_db"),
		"The number of processes by database.",
		[]string{"db"}, nil)
	processesByClientDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "processes_by_client"),
		"The number of processes by client host.",
		[]string{"client"}, nil)
	processesByServerDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "processes_by_server"),
		"The number of processes by tidb server node.",
		[]string{"server"}, nil)
)

// ScrapeProcesslist collects from `information_schema.processlist`.
type ScrapeProcesslist struct{}

// Name of the Scraper. Should be unique.
func (ScrapeProcesslist) Name() string {
	return informationSchema + ".processlist"
}

// Help describes the role of the Scraper.
func (ScrapeProcesslist) Help() string {
	return "Collect current thread state counts from the information_schema.processlist"
}

// Version of MySQL from which scraper is available.
func (ScrapeProcesslist) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeProcesslist) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	processQuery := fmt.Sprintf(
		infoSchemaProcesslistQuery,
		*processlistMinTime,
	)
	processlistRows, err := db.QueryContext(ctx, processQuery)
	if err != nil {
		return err
	}
	defer processlistRows.Close()

	var (
		instance string
		user     string
		host     string
		databse  string
		time     uint32
		state    string
		mem      uint64
		disk     uint64
	)
	stateCounts := make(map[string]uint32)
	stateTime := make(map[string]uint32)
	stateMem := make(map[string]uint64)
	stateDisk := make(map[string]uint64)
	clientCount := make(map[string]uint32)
	userCount := make(map[string]uint32)
	dbCount := make(map[string]uint32)
	serverCount := make(map[string]uint32)

	for processlistRows.Next() {
		err = processlistRows.Scan(&instance, &user, &host, &databse, &time, &state, &mem, &disk)
		if err != nil {
			return err
		}
		if host == "" {
			host = "unknown"
		}

		// Init maps
		stateCounts[state] += 1
		stateTime[state] += time
		stateMem[state] += mem
		stateDisk[state] += disk

		serverCount[instance] += 1
		clientCount[host] += 1
		userCount[user] += 1
		dbCount[databse] += 1

	}

	for _, state := range sortedMapKeys(stateTime) {
		ch <- prometheus.MustNewConstMetric(processlistCountDesc, prometheus.GaugeValue, float64(stateCounts[state]), state)
		ch <- prometheus.MustNewConstMetric(processlistTimeDesc, prometheus.GaugeValue, float64(stateTime[state]), state)
		ch <- prometheus.MustNewConstMetric(processlistMemDesc, prometheus.GaugeValue, float64(stateMem[state]), state)
		ch <- prometheus.MustNewConstMetric(processlistDiskDesc, prometheus.GaugeValue, float64(stateDisk[state]), state)
	}

	if *processesByServerFlag {
		for _, server := range sortedMapKeys(serverCount) {
			ch <- prometheus.MustNewConstMetric(processesByServerDesc, prometheus.GaugeValue, float64(serverCount[server]), server)
		}
	}

	if *processesByClientFlag {
		for _, client := range sortedMapKeys(clientCount) {
			ch <- prometheus.MustNewConstMetric(processesByClientDesc, prometheus.GaugeValue, float64(clientCount[client]), client)
		}
	}

	if *processesByUserFlag {
		for _, user := range sortedMapKeys(userCount) {
			ch <- prometheus.MustNewConstMetric(processesByUserDesc, prometheus.GaugeValue, float64(userCount[user]), user)
		}
	}

	if *processesByDBFlag {
		for _, database := range sortedMapKeys(dbCount) {
			ch <- prometheus.MustNewConstMetric(processesByDBDesc, prometheus.GaugeValue, float64(dbCount[databse]), database)
		}
	}

	return nil
}

func sortedMapKeys(m interface{}) []string {
	v := reflect.ValueOf(m)
	keys := make([]string, 0, len(v.MapKeys()))
	for _, key := range v.MapKeys() {
		keys = append(keys, key.String())
	}
	sort.Strings(keys)
	return keys
}

// check interface
var _ Scraper = ScrapeProcesslist{}
