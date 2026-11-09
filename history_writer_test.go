package replaysuite

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"reflect"
	"testing"
)

//go:embed testdata/annotate_payload_data.json
var annotatePayloadDataEvents []byte

func TestAnnotatePayloadData(t *testing.T) {
	t.Parallel()

	dec := json.NewDecoder(bytes.NewReader(annotatePayloadDataEvents))
	dec.UseNumber()

	var events []any
	if err := dec.Decode(&events); err != nil {
		t.Fatalf("decode events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}

	annotatePayloadData(events[0])
	if !reflect.DeepEqual(events[0], events[1]) {
		t.Fatalf("annotated event = %v, want %v", events[0], events[1])
	}
}
