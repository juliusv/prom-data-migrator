package main

import (
	"context"
	"flag"
	"os"
	"sort"
	"sync"
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
	endTimestamp := flag.Int64("end-timestamp", 0, "Unix timestamp in seconds of the end of the time range to migrate. If 0, the current time is chosen.")
	step := flag.Duration("step", 15*time.Minute, "How much data to load at once.")
	v1HeapSize := flag.Uint64("v1-target-heap-size", 2000000000, "How much memory to use for v1 storage in bytes")
	maxParallelism := flag.Int("max-parallelism", 1, "How many instances to migrate at the same time.")
	flag.Parse()

	logger := log.NewSyncLogger(log.NewLogfmtLogger(os.Stderr))

	v1Storage := local.NewMemorySeriesStorage(&local.MemorySeriesStorageOptions{
		TargetHeapSize:             *v1HeapSize,
		PersistenceRetentionPeriod: 999999 * time.Hour,
		PersistenceStoragePath:     *v1Dir,
		HeadChunkTimeout:           0,
		CheckpointInterval:         999999 * time.Hour,
		CheckpointDirtySeriesLimit: 1e9,
		MinShrinkRatio:             0.1,
		SyncStrategy:               local.Never,
	})
	if err := v1Storage.Start(); err != nil {
		level.Error(logger).Log("msg", "error starting v1 storage", "err", err)
		os.Exit(1)
	}
	defer v1Storage.Stop()

	v2Storage, err := tsdb.Open(*v2Dir, logger, nil, &tsdb.Options{
		WALFlushInterval:  5 * time.Second,
		RetentionDuration: 999999 * 24 * 60 * 60 * 1000,
		BlockRanges:       tsdb.ExponentialBlockRanges(int64(2*60*60*1000), 10, 3),
	})
	if err != nil {
		level.Error(logger).Log("msg", "error starting v2 storage", "err", err)
		os.Exit(1)
	}
	defer v2Storage.Close()

	instances, err := v1Storage.LabelValuesForLabelName(context.Background(), model.InstanceLabel)
	if err != nil {
		level.Error(logger).Log("msg", "error querying instance labels from v1 storage", "err", err)
		os.Exit(1)
	}

	endTime := model.Now()
	if *endTimestamp != 0 {
		endTime = model.TimeFromUnix(*endTimestamp)
	}

	totalSteps := (*lookback / *step).Nanoseconds()
	bar := pb.StartNew(int(totalSteps))
	level.Info(logger).Log("msg", "Total steps", "steps", totalSteps)
	for t := endTime.Add(-*lookback); !t.After(endTime); t = t.Add(*step) {
		bar.Increment()

		var wg sync.WaitGroup
		sema := make(chan struct{}, *maxParallelism)
		for _, instance := range instances {
			matcher, err := metric.NewLabelMatcher(metric.Equal, model.InstanceLabel, instance)
			if err != nil {
				panic(err)
			}

			wg.Add(1)
			go func() {
				sema <- struct{}{}
				if err := migrate(v1Storage, v2Storage, t, t.Add(*step), matcher); err != nil {
					level.Error(logger).Log("msg", "error migrating", "err", err)
					os.Exit(1)
				}
				<-sema
				wg.Done()
			}()
		}
		wg.Wait()
	}
	bar.FinishPrint("Migration Complete")
}

func migrate(v1Storage *local.MemorySeriesStorage, v2Storage *tsdb.DB, from, through model.Time, matcher *metric.LabelMatcher) error {
	its, err := v1Storage.QueryRange(context.Background(), from, through, matcher)
	if err != nil {
		return err
	}

	app := v2Storage.Appender()

	for _, it := range its {
		samples := it.RangeValues(metric.Interval{
			OldestInclusive: from,
			NewestInclusive: through,
		})

		ls := make(labels.Labels, 0, len(it.Metric().Metric))
		for k, v := range it.Metric().Metric {
			ls = append(ls, labels.Label{Name: string(k), Value: string(v)})
		}
		sort.Sort(ls)

		for _, s := range samples {
			_, err := app.Add(ls, int64(s.Timestamp), float64(s.Value))

			if err != nil {
				return err
			}
		}
	}

	return app.Commit()
}
