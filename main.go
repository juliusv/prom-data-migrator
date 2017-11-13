package main

import (
	"context"
	"flag"
	"os"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"gopkg.in/cheggaaa/pb.v1"
)

func main() {
	v1Dir := flag.String("v1-dir", "./data-v1", "Path to the v1 storage directory.")
	v2Dir := flag.String("v2-dir", "./data-v2", "Path to the v2 storage directory.")
	lookback := flag.Duration("lookback", 15*24*time.Hour, "How far back to start when exporting old data.")
	step := flag.Duration("step", 15*time.Minute, "How much data to load at once.")
	v1HeapSize := flag.Uint64("v1-target-heap-size", 2000000000, "How much memory to use for v1 storage in bytes")
	flag.Parse()

	logger := log.NewSyncLogger(log.NewLogfmtLogger(os.Stderr))

	v1Storage := local.NewMemorySeriesStorage(&local.MemorySeriesStorageOptions{
		TargetHeapSize:             *v1HeapSize,
		PersistenceRetentionPeriod: 999999 * time.Hour,
		PersistenceStoragePath:     *v1Dir,
		HeadChunkTimeout:           999999 * time.Hour,
		CheckpointInterval:         999999 * time.Hour,
		CheckpointDirtySeriesLimit: 1e9,
		MinShrinkRatio:             0.1,
		SyncStrategy:               local.Never,
	})
	if err := v1Storage.Start(); err != nil {
		level.Error(logger).Log("msg", "Error starting v1 storage", "err", err)
		os.Exit(1)
	}
	defer v1Storage.Stop()

	v2Storage, err := tsdb.Open(*v2Dir, logger, nil, &tsdb.Options{
		WALFlushInterval:  5 * time.Second,
		RetentionDuration: 999999 * 24 * 60 * 60 * 1000,
		BlockRanges:       tsdb.ExponentialBlockRanges(int64(2*60*60*1000), 10, 3),
	})
	if err != nil {
		level.Error(logger).Log("msg", "Error starting v2 storage", "err", err)
		os.Exit(1)
	}
	defer v2Storage.Close()

	// TODO: This queries *all* series, which will not work on huge source data directories.
	//       Shard this (e.g. on the "instance" label) and experiment with parallelism.
	matcher, err := metric.NewLabelMatcher(metric.RegexMatch, model.MetricNameLabel, ".+")
	if err != nil {
		panic(err)
	}

	now := model.Now()
	totalSteps := (*lookback / *step).Nanoseconds()
	bar := pb.StartNew(int(totalSteps))
	level.Info(logger).Log("msg", "Total steps", "steps", totalSteps)
	for t := now.Add(-*lookback); !t.After(now); t = t.Add(*step) {
		level.Debug(logger).Log("msg", "Migrating time step", "start", t, "end", t.Add(*step))
		bar.Increment()
		its, err := v1Storage.QueryRange(context.Background(), t, t.Add(*step), matcher)
		if err != nil {
			level.Error(logger).Log("msg", "Error querying v1 storage", "err", err)
			os.Exit(1)
		}

		app := v2Storage.Appender()

		for _, it := range its {
			samples := it.RangeValues(metric.Interval{
				OldestInclusive: t,
				NewestInclusive: t.Add(*step),
			})

			ls := make(labels.Labels, 0, len(it.Metric().Metric))
			for k, v := range it.Metric().Metric {
				ls = append(ls, labels.Label{Name: string(k), Value: string(v)})
			}
			sort.Sort(ls)

			for _, s := range samples {
				_, err := app.Add(ls, int64(s.Timestamp), float64(s.Value))

				if err != nil {
					level.Error(logger).Log("msg", "Error appending samples to v2 storage", "err", err)
					os.Exit(1)
				}
			}
		}

		if err := app.Commit(); err != nil {
			level.Error(logger).Log("msg", "Error committing samples to v2 storage", "err", err)
			os.Exit(1)
		}
	}
	bar.FinishPrint("Migration Complete")
}
