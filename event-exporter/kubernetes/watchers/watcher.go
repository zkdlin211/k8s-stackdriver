/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package watchers

import (
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"k8s.io/client-go/tools/cache"
)

// WatcherConfig represents the configuration of the Kubernetes API watcher.
type WatcherConfig struct {
	ListerWatcher     cache.ListerWatcher
	ExpectedType      interface{}
	StoreConfig       *WatcherStoreConfig
	ResyncPeriod      time.Duration
	WatchListPageSize int64
}

// Watcher is an interface of the generic proactive API watcher.
type Watcher interface {
	Run(stopCh <-chan struct{})
	DebugMemoryStats()
}

// watcher satisfies the Watcher interface.
type watcher struct {
	reflector *cache.Reflector
	store     *watcherStore
}

func (w *watcher) Run(stopCh <-chan struct{}) {
	w.reflector.Run(stopCh)
}

func (w *watcher) DebugMemoryStats() {
	items := w.store.List()
	count := len(items)

	if count == 0 {
		return
	}

	// 1. Calculate Average Size using a Sample (First 50 items)
	// This prevents allocating memory for all 10k+ items
	sampleSize := 50
	if count < sampleSize {
		sampleSize = count
	}

	var totalSampleSize int64
	for i := 0; i < sampleSize; i++ {
		// Marshal to JSON to get the "wire" size
		bytes, _ := json.Marshal(items[i])
		totalSampleSize += int64(len(bytes))
	}

	avgSize := float64(totalSampleSize) / float64(sampleSize)

	// 2. Estimate Total RAM Usage
	// Multiply by 2.0 to account for Go struct overhead (pointers, maps, interfaces)
	// This is a heuristic: RAM is usually 1.5x - 2.5x larger than JSON.
	estimatedTotalBytes := avgSize * float64(count) * 2.0

	fmt.Printf("--- Event Watcher Memory Stats ---\n")
	fmt.Printf("Cached Objects: %d\n", count)
	fmt.Printf("Avg Event Size (JSON): %.2f KB\n", avgSize/1024)
	fmt.Printf("Est. Total Resident Memory: %.2f MB\n", estimatedTotalBytes/1024/1024)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Actual Sys Total Alloc: %.2f MB\n", float64(m.Alloc)/1024/1024)
	fmt.Printf("----------------------------------\n")
}

// NewWatcher creates a new Kubernetes API watcher using provided configuration.
func NewWatcher(config *WatcherConfig) Watcher {
	store := newWatcherStore(config.StoreConfig)
	r := cache.NewReflector(
		config.ListerWatcher,
		config.ExpectedType,
		store,
		config.ResyncPeriod,
	)
	r.WatchListPageSize = config.WatchListPageSize
	return &watcher{
		reflector: r,
		store:     store,
	}
}
