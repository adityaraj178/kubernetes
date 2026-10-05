/*
Copyright 2024 The Kubernetes Authors.

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

package cache

import (
	"fmt"
	"runtime"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// makeBenchPod returns a Pod-sized *unstructured.Unstructured for benchmarks.
// Each object is unique so results represent real informer workloads rather
// than compressing identical copies.
func makeBenchPod(index int) *unstructured.Unstructured {
	labels := map[string]interface{}{}
	annotations := map[string]interface{}{}
	for i := 0; i < 10; i++ {
		labels[fmt.Sprintf("label-key-%d", i)] = fmt.Sprintf("label-value-%d-%d", index, i)
		annotations[fmt.Sprintf("annotation-key-%d", i)] = fmt.Sprintf("annotation-value-%d-%d-with-some-longer-text", index, i)
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":            fmt.Sprintf("pod-%d", index),
			"namespace":       "default",
			"resourceVersion": fmt.Sprintf("%d", 100000+index),
			"uid":             fmt.Sprintf("uid-%d-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", index),
			"labels":          labels,
			"annotations":     annotations,
		},
		"spec": map[string]interface{}{
			"containers": []interface{}{map[string]interface{}{
				"name":  "main",
				"image": "registry.example.com/some/reasonably-long-image-name:v1.2.3",
				"ports": []interface{}{
					map[string]interface{}{"containerPort": int64(8080), "protocol": "TCP"},
					map[string]interface{}{"containerPort": int64(8443), "protocol": "TCP"},
				},
				"env": []interface{}{
					map[string]interface{}{"name": "ENV_ONE", "value": "some-configuration-value-one"},
					map[string]interface{}{"name": "ENV_TWO", "value": "some-configuration-value-two"},
				},
			}},
		},
		"status": map[string]interface{}{
			"phase": "Running", "podIP": "10.244.1.37", "hostIP": "192.168.1.10",
		},
	}}
}

// TestCBORHeapReduction measures the live heap for a store holding 3000
// Pod-sized *unstructured.Unstructured objects.  The source objects are not
// retained by the caller (models a real informer where the store holds the
// only reference and the reflector has dropped its copy).
//
// Run with:
//
//	go test ./staging/src/k8s.io/client-go/tools/cache/ -run TestCBORHeapReduction -v
func TestCBORHeapReduction(t *testing.T) {
	const n = 3000

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	store := NewThreadSafeStore(Indexers{}, Indices{})
	for i := 0; i < n; i++ {
		store.Add(fmt.Sprintf("default/pod-%d", i), makeBenchPod(i))
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(store)

	if after.HeapAlloc > before.HeapAlloc {
		t.Logf("live heap for %d unstructured Pods: %d KB", n, (after.HeapAlloc-before.HeapAlloc)/1024)
	}
}

// BenchmarkCBORWrite measures Add → CBOR encode for *unstructured.Unstructured.
func BenchmarkCBORWrite(b *testing.B) {
	pod := makeBenchPod(0)
	store := NewThreadSafeStore(Indexers{}, Indices{})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Add("default/pod", pod)
	}
}

// BenchmarkCBORRead measures Get → CBOR decode for *unstructured.Unstructured.
func BenchmarkCBORRead(b *testing.B) {
	store := NewThreadSafeStore(Indexers{}, Indices{})
	store.Add("default/pod", makeBenchPod(0))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := store.Get("default/pod"); !ok {
			b.Fatal("missing key")
		}
	}
}

// BenchmarkCBORList measures List() over 1000 objects — the worst-case read
// path (full resync), where every object is decoded on each call.
func BenchmarkCBORList(b *testing.B) {
	const n = 1000
	store := NewThreadSafeStore(Indexers{}, Indices{})
	for i := 0; i < n; i++ {
		store.Add(fmt.Sprintf("default/pod-%d", i), makeBenchPod(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.List()
	}
}
