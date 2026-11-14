package replaysuite

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultHistoriesDir       = ".histories"
	decodedPayloadDataJSONKey = "__replaysuite_decodedData"
)

func encodeHistory(hist *historypb.History) ([]byte, error) {

	// convert history to any so that it's easier to annotate history, e.g.
	// add custom fields for decoded data or worker identity redaction
	marshal := protojson.MarshalOptions{Indent: "  "}
	raw, err := marshal.Marshal(hist)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal history: %w", err)
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("failed to unmarshal history: %w", err)
	}

	// payloads are base64 encoded which makes it less convenient to
	// inspect the actual values in the history. This also records a
	// new field in the history with the decoded value. The replayer
	// discards unknown fields so it should be ok.
	//https://github.com/temporalio/sdk-go/blob/35367242edb9e92be90825cd7653ab0046a660d1/internal/internal_worker.go#L2054
	annotatePayloadData(value)

	return json.MarshalIndent(value, "", "  ")
}

func writeHistory(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("failed to create location %s: %w", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("failed to write at path %s: %w", path, err)
	}
	return nil
}

// redactWorkerIdentityInHistory makes the worker identity in the history
// anonymous. It finds all events that reference the worker either as an
// identity or a sticky task queue.
//
// Note that because multiple protos in the history have an identity field,
// this helper makes the change on the raw value rather than a history proto
// object.
func redactWorkerIdentityInHistory(value any) {
	switch v := value.(type) {
	case map[string]any:
		if _, ok := v["identity"].(string); ok {
			v["identity"] = "placeholder"
		}
		if kind, ok := v["kind"].(string); ok && kind == "TASK_QUEUE_KIND_STICKY" {
			if name, ok := v["name"].(string); ok {
				if idx := strings.LastIndex(name, ":"); idx >= 0 {
					v["name"] = "placeholder" + name[idx:]
				} else {
					v["name"] = "placeholder"
				}
			}
		}
		for _, child := range v {
			redactWorkerIdentityInHistory(child)
		}
	case []any:
		for _, child := range v {
			redactWorkerIdentityInHistory(child)
		}
	}
}

// ai slop incoming below, haven't checked

func annotatePayloadData(value any) {
	switch v := value.(type) {
	case map[string]any:
		if decoded, ok := decodePayloadData(v); ok {
			v[decodedPayloadDataJSONKey] = decoded
		}
		for _, child := range v {
			annotatePayloadData(child)
		}
	case []any:
		for _, child := range v {
			annotatePayloadData(child)
		}
	}
}

func decodePayloadData(payload map[string]any) (any, bool) {
	metadata, ok := payload["metadata"].(map[string]any)
	if !ok {
		return nil, false
	}
	encodedEncoding, ok := metadata["encoding"].(string)
	if !ok {
		return nil, false
	}
	encodingBytes, err := base64.StdEncoding.DecodeString(encodedEncoding)
	if err != nil {
		return nil, false
	}

	switch string(encodingBytes) {
	case "binary/null":
		return nil, true
	case "json/plain", "json/protobuf":
		encodedData, ok := payload["data"].(string)
		if !ok {
			return nil, false
		}
		data, err := base64.StdEncoding.DecodeString(encodedData)
		if err != nil {
			return nil, false
		}
		var decoded any
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(&decoded); err != nil {
			return nil, false
		}
		return decoded, true
	default:
		return nil, false
	}
}
