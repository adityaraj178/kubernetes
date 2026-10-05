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
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/fxamacker/cbor/v2"
	"github.com/klauspost/compress/s2"
	"github.com/vmihailenco/msgpack/v5"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
	sigsjson "sigs.k8s.io/json"
)

// SerializationFormat selects how a *unstructured.Unstructured object's
// decoded map is packed into bytes before it is stored at rest.
//
// Proto is deliberately not offered here: protobuf marshaling requires a
// compiled message schema, and *unstructured.Unstructured exists precisely
// for types (CRDs) that have none, so there is nothing for a proto codec to
// target.
type SerializationFormat string

const (
	// SerializationCBOR packs the object with CBOR (github.com/fxamacker/cbor).
	// This is the default: compact, and already a client-go dependency.
	SerializationCBOR SerializationFormat = "cbor"
	// SerializationJSON packs the object with encoding/json, decoded back
	// through sigs.k8s.io/json so integers survive as int64 rather than
	// becoming float64 (the same helper apimachinery's own JSON codec uses).
	SerializationJSON SerializationFormat = "json"
	// SerializationMsgpack packs the object with MessagePack
	// (github.com/vmihailenco/msgpack).
	SerializationMsgpack SerializationFormat = "msgpack"
)

// CompressionAlgorithm selects how the serialized bytes are compressed before
// being stored at rest.
type CompressionAlgorithm string

const (
	// CompressionNone stores the serialized bytes as-is.
	CompressionNone CompressionAlgorithm = "none"
	// CompressionGzip uses compress/gzip at gzip.DefaultCompression.
	CompressionGzip CompressionAlgorithm = "gzip"
	// CompressionGzipBestSpeed uses compress/gzip at gzip.BestSpeed.
	CompressionGzipBestSpeed CompressionAlgorithm = "gzip-bestspeed"
	// CompressionDeflate uses raw DEFLATE (compress/flate), no gzip header.
	CompressionDeflate CompressionAlgorithm = "deflate"
	// CompressionS2 uses klauspost/compress/s2's block format, a faster
	// Snappy-derived codec.
	CompressionS2 CompressionAlgorithm = "s2"
)

// encodedObject is the at-rest wrapper for an object that has passed through
// a storeCodec. A named type (not raw []byte) avoids ambiguity: any []byte a
// caller stores directly is passed through unchanged; only values produced by
// storeCodec.encode carry this wrapper.
type encodedObject struct{ data []byte }

// cborDecMode decodes all CBOR maps (including nested ones) as
// map[string]interface{}, matching the shape that *unstructured.Unstructured
// field accessors expect, and decodes all integers as int64 (rather than a
// uint64/int64 mix) to match what unstructured accessors like NestedInt64
// expect.
var cborDecMode cbor.DecMode

func init() {
	dm, err := cbor.DecOptions{
		DefaultMapType: reflect.TypeOf(map[string]interface{}{}),
		IntDec:         cbor.IntDecConvertSignedOrFail,
	}.DecMode()
	if err != nil {
		panic(fmt.Sprintf("cache: failed to create CBOR DecMode: %v", err))
	}
	cborDecMode = dm
}

// storeCodec packs and unpacks *unstructured.Unstructured objects at rest in
// a threadSafeMap, using a fixed serialization format and compression
// algorithm for the lifetime of the store.
type storeCodec struct {
	serialization SerializationFormat
	compression   CompressionAlgorithm
}

// newStoreCodec builds a storeCodec, filling in defaults for zero values, and
// logs the effective configuration once.
func newStoreCodec(serialization SerializationFormat, compression CompressionAlgorithm) *storeCodec {
	if serialization == "" {
		serialization = SerializationCBOR
	}
	if compression == "" {
		compression = CompressionNone
	}
	klog.Background().Info("📦 ThreadSafeStore cache codec configured — 🔤 serialization + 🗜️ compression",
		"serialization", serialization, "compression", compression)
	return &storeCodec{serialization: serialization, compression: compression}
}

// WithThreadSafeStoreCodec configures how *unstructured.Unstructured objects
// are packed at rest in the store. Objects of any other type always pass
// through unchanged.
func WithThreadSafeStoreCodec(serialization SerializationFormat, compression CompressionAlgorithm) ThreadSafeStoreOption {
	return func(c *threadSafeMap) {
		c.codec = newStoreCodec(serialization, compression)
	}
}

// encode packs obj to an encodedObject when obj is a
// *unstructured.Unstructured. Any other type is returned unchanged so the
// store remains compatible with typed objects.
func (c *storeCodec) encode(key string, obj interface{}) interface{} {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return obj
	}
	raw, err := c.marshal(u.Object)
	if err != nil {
		klog.Background().Error(err, "ThreadSafeStore: serialization failed, storing decoded",
			"key", key, "serialization", c.serialization)
		return obj
	}
	packed, err := c.compressBytes(raw)
	if err != nil {
		klog.Background().Error(err, "ThreadSafeStore: compression failed, storing uncompressed",
			"key", key, "compression", c.compression)
		packed = raw
	}
	return encodedObject{data: packed}
}

// decode unpacks an encodedObject back to *unstructured.Unstructured. Any
// other value is returned unchanged.
func (c *storeCodec) decode(key string, stored interface{}) interface{} {
	enc, ok := stored.(encodedObject)
	if !ok {
		return stored
	}
	raw, err := c.decompressBytes(enc.data)
	if err != nil {
		klog.Background().Error(err, "ThreadSafeStore: decompression failed, returning stored",
			"key", key, "compression", c.compression)
		return stored
	}
	m, err := c.unmarshal(raw)
	if err != nil {
		klog.Background().Error(err, "ThreadSafeStore: deserialization failed, returning stored",
			"key", key, "serialization", c.serialization)
		return stored
	}
	return &unstructured.Unstructured{Object: m}
}

func (c *storeCodec) marshal(m map[string]interface{}) ([]byte, error) {
	switch c.serialization {
	case SerializationJSON:
		return json.Marshal(m)
	case SerializationMsgpack:
		return msgpack.Marshal(m)
	default: // SerializationCBOR
		return cbor.Marshal(m)
	}
}

func (c *storeCodec) unmarshal(data []byte) (map[string]interface{}, error) {
	var m map[string]interface{}
	var err error
	switch c.serialization {
	case SerializationJSON:
		// UnmarshalCaseSensitivePreserveInts keeps whole numbers as int64
		// instead of the float64 plain encoding/json would produce, matching
		// what unstructured field accessors expect.
		err = sigsjson.UnmarshalCaseSensitivePreserveInts(data, &m)
	case SerializationMsgpack:
		err = msgpack.Unmarshal(data, &m)
	default: // SerializationCBOR
		err = cborDecMode.Unmarshal(data, &m)
	}
	return m, err
}

func (c *storeCodec) compressBytes(data []byte) ([]byte, error) {
	switch c.compression {
	case CompressionGzip:
		return gzipCompress(data, gzip.DefaultCompression)
	case CompressionGzipBestSpeed:
		return gzipCompress(data, gzip.BestSpeed)
	case CompressionDeflate:
		return deflateCompress(data)
	case CompressionS2:
		return s2.Encode(nil, data), nil
	default: // CompressionNone
		return data, nil
	}
}

func (c *storeCodec) decompressBytes(data []byte) ([]byte, error) {
	switch c.compression {
	case CompressionGzip, CompressionGzipBestSpeed:
		return gzipDecompress(data)
	case CompressionDeflate:
		return deflateDecompress(data)
	case CompressionS2:
		return s2.Decode(nil, data)
	default: // CompressionNone
		return data, nil
	}
}

func gzipCompress(data []byte, level int) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func deflateCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func deflateDecompress(data []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	return io.ReadAll(r)
}
