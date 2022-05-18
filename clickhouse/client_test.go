// Copyright 2015 The Prometheus Authors
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

package clickhouse

import (
	"math"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
)

func TestClient(t *testing.T) {
	samples := model.Samples{
		{
			Metric: model.Metric{
				model.MetricNameLabel: "testmetric",
				"test_label":          "test_label_value1",
			},
			Timestamp: model.Time(123456789123),
			Value:     1.23,
		},
		{
			Metric: model.Metric{
				model.MetricNameLabel: "testmetric",
				"test_label":          "test_label_value2",
			},
			Timestamp: model.Time(123456789123),
			Value:     5.1234,
		},
		{
			Metric: model.Metric{
				model.MetricNameLabel: "nan_value",
			},
			Timestamp: model.Time(123456789123),
			Value:     model.SampleValue(math.NaN()),
		},
		{
			Metric: model.Metric{
				model.MetricNameLabel: "pos_inf_value",
			},
			Timestamp: model.Time(123456789123),
			Value:     model.SampleValue(math.Inf(1)),
		},
		{
			Metric: model.Metric{
				model.MetricNameLabel: "neg_inf_value",
			},
			Timestamp: model.Time(123456789123),
			Value:     model.SampleValue(math.Inf(-1)),
		},
	}

	c := NewClient(
		nil,
		"clickhouse://default:C1ickh0use@clickhouse-tkeel-core:9000?dial_timeout=1s&compress=true",
		"core2",
		"test4",
	)

	if err := c.Write(samples); err != nil {
		t.Fatalf("Error sending samples: %s", err)
	}
}

func TestClient_Read(t *testing.T) {
	c := NewClient(
		nil,
		"clickhouse://default:C1ickh0use@clickhouse-tkeel-core:9000?dial_timeout=1s&compress=true",
		"core",
		"timeseries",
	)
	matcher1, _ := labels.NewMatcher(labels.MatchEqual, "__name__", "abc")
	matcher2, _ := labels.NewMatcher(labels.MatchEqual, "id", "iotd-891b86ad-5a30-4a84-8491-94a56a0ad69b")
	query, _ := remote.ToQuery(time.Now().UnixMilli()-9200*1e3, time.Now().UnixMilli(), []*labels.Matcher{matcher1, matcher2}, &storage.SelectHints{Step: 0, Func: "avg"})
	req := prompb.ReadRequest{
		Queries: []*prompb.Query{query},
	}
	if resp, err := c.Read(&req); err != nil {
		t.Fatalf("Error sending samples: %s", err)
	} else {
		t.Log(resp)
	}
}
