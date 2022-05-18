// Copyright 2017 The Prometheus Authors
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

// The main package for the Prometheus server executable.
package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"

	"github.com/tkeel-io/prometheus-remote-storage/clickhouse"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/prometheus/prometheus/prompb"
)

type config struct {
	clickhouseURL      string
	clickhouseDatabase string
	clickhouseTable    string
	listenAddr         string
	telemetryPath      string
	promlogConfig      promlog.Config
}

func init() {
}

func main() {
	cfg := parseFlags()
	http.Handle(cfg.telemetryPath, promhttp.Handler())

	logger := promlog.New(&cfg.promlogConfig)

	writers, readers := buildClients(logger, cfg)
	if err := serve(logger, cfg.listenAddr, writers, readers); err != nil {
		_ = level.Error(logger).Log("msg", "Failed to listen", "addr", cfg.listenAddr, "err", err)
		os.Exit(1)
	}
}

func parseFlags() *config {
	a := kingpin.New(filepath.Base(os.Args[0]), "Remote storage adapter")
	a.HelpFlag.Short('h')

	cfg := &config{
		promlogConfig: promlog.Config{},
	}

	a.Flag("clickhouse.url", "The URL of the remote Clickhouse server to send samples to. None, if empty.").
		Default("").StringVar(&cfg.clickhouseURL)
	a.Flag("clickhouse.database", "The name of the database to use for storing samples in Clickhouse.").
		Default("prometheus").StringVar(&cfg.clickhouseDatabase)
	a.Flag("clickhouse.table", "The name of the table to use for storing samples in Clickhouse.").
		Default("metrics").StringVar(&cfg.clickhouseTable)
	a.Flag("web.listen-address", "Address to listen on for web endpoints.").
		Default(":9201").StringVar(&cfg.listenAddr)
	a.Flag("web.telemetry-path", "Address to listen on for web endpoints.").
		Default("/metrics").StringVar(&cfg.telemetryPath)

	flag.AddFlags(a, &cfg.promlogConfig)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	return cfg
}

type writer interface {
	Write(samples model.Samples) error
	Name() string
}

type reader interface {
	Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error)
	Name() string
}

func buildClients(logger log.Logger, cfg *config) ([]writer, []reader) {
	var writers []writer
	var readers []reader
	if cfg.clickhouseURL != "" {
		dsn := "clickhouse://default:C1ickh0use@clickhouse-tkeel-core:9000"
		dsn = cfg.clickhouseURL
		c := clickhouse.NewClient(
			log.With(logger, "storage", "Clickhouse"),
			dsn,
			cfg.clickhouseDatabase,
			cfg.clickhouseTable,
		)
		prometheus.MustRegister(c)
		writers = append(writers, c)
		readers = append(readers, c)
	}
	_ = level.Info(logger).Log("msg", "Starting up...")
	return writers, readers
}

func serve(logger log.Logger, addr string, writers []writer, readers []reader) error {
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			_ = level.Error(logger).Log("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			_ = level.Error(logger).Log("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			_ = level.Error(logger).Log("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// TODO: Support reading from more than one reader and merging the results.
		if len(readers) != 1 {
			http.Error(w, fmt.Sprintf("expected exactly one reader, found %d readers", len(readers)), http.StatusInternalServerError)
			return
		}
		reader := readers[0]

		var resp *prompb.ReadResponse
		resp, err = reader.Read(&req)
		if err != nil {
			_ = level.Warn(logger).Log("msg", "Error executing query", "query", req, "storage", reader.Name(), "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			_ = level.Warn(logger).Log("msg", "Error writing response", "storage", reader.Name(), "err", err)
		}
	})

	return http.ListenAndServe(addr, nil)
}
