package replaysuite

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
)

func plainActivityForNameTest() error {
	return nil
}

type activityFunctionNameTestActivities struct{}

func (activityFunctionNameTestActivities) ValueReceiverActivity() error {
	return nil
}

func (*activityFunctionNameTestActivities) PointerReceiverActivity() error {
	return nil
}

func TestActivityFunctionName(t *testing.T) {
	t.Run("string activity name", func(t *testing.T) {
		// Given
		const activityName = "CustomActivityName"

		// When
		name, isMethod := activityFunctionName(activityName)

		// Then
		if name != activityName {
			t.Fatalf("name = %q, want %q", name, activityName)
		}
		if isMethod {
			t.Fatal("isMethod = true, want false")
		}
	})

	t.Run("plain function", func(t *testing.T) {
		// Given
		activityFn := plainActivityForNameTest

		// When
		name, isMethod := activityFunctionName(activityFn)

		// Then
		if name != "plainActivityForNameTest" {
			t.Fatalf("name = %q, want %q", name, "plainActivityForNameTest")
		}
		if isMethod {
			t.Fatal("isMethod = true, want false")
		}
	})

	t.Run("value receiver method value", func(t *testing.T) {
		// Given
		activities := activityFunctionNameTestActivities{}

		// When
		name, isMethod := activityFunctionName(activities.ValueReceiverActivity)

		// Then
		if name != "ValueReceiverActivity" {
			t.Fatalf("name = %q, want %q", name, "ValueReceiverActivity")
		}
		if isMethod {
			t.Fatal("isMethod = true, want false")
		}
	})

	t.Run("pointer receiver method value", func(t *testing.T) {
		// Given
		activities := &activityFunctionNameTestActivities{}

		// When
		name, isMethod := activityFunctionName(activities.PointerReceiverActivity)

		// Then
		if name != "PointerReceiverActivity" {
			t.Fatalf("name = %q, want %q", name, "PointerReceiverActivity")
		}
		if !isMethod {
			t.Fatal("isMethod = false, want true")
		}
	})

	t.Run("nil pointer receiver method value", func(t *testing.T) {
		// Given
		var activities *activityFunctionNameTestActivities

		// When
		name, isMethod := activityFunctionName(activities.PointerReceiverActivity)

		// Then
		if name != "PointerReceiverActivity" {
			t.Fatalf("name = %q, want %q", name, "PointerReceiverActivity")
		}
		if !isMethod {
			t.Fatal("isMethod = false, want true")
		}
	})
}

func TestStubCallMatchesSupportsTestifyMatchers(t *testing.T) {
	// Given
	call := stubCall{
		expectedArgs: []interface{}{
			mock.Anything,
			mock.MatchedBy(func(input *activityFunctionNameTestActivities) bool {
				return input != nil
			}),
		},
	}
	decoded := []interface{}{
		"ignored",
		&activityFunctionNameTestActivities{},
	}

	// When
	matches := stubCallMatches(call, decoded)

	// Then
	if !matches {
		t.Fatal("stubCallMatches = false, want true")
	}
}

func TestAnnotatePayloadData(t *testing.T) {
	raw := []byte(`{
		"events": [
			{
				"payloads": [
					{
						"metadata": {
							"encoding": "anNvbi9wbGFpbg=="
						},
						"data": "InZhbHVlOjci"
					},
					{
						"metadata": {
							"encoding": "anNvbi9wbGFpbg=="
						},
						"data": "Nw=="
					},
					{
						"metadata": {
							"encoding": "YmluYXJ5L251bGw="
						}
					}
				]
			}
		]
	}`)

	annotated, err := annotatePayloadData(raw)
	if err != nil {
		t.Fatalf("annotatePayloadData returned error: %v", err)
	}

	var decoded struct {
		Events []struct {
			Payloads []struct {
				Data        string          `json:"data"`
				DecodedData json.RawMessage `json:"__replaysuite_decodedData"`
			} `json:"payloads"`
		} `json:"events"`
	}
	if err := json.Unmarshal(annotated, &decoded); err != nil {
		t.Fatalf("annotated JSON did not unmarshal: %v", err)
	}

	if got := string(decoded.Events[0].Payloads[0].DecodedData); got != `"value:7"` {
		t.Fatalf("first %s = %s, want %s", decodedPayloadDataJSONKey, got, `"value:7"`)
	}
	if got := string(decoded.Events[0].Payloads[1].DecodedData); got != `7` {
		t.Fatalf("second %s = %s, want 7", decodedPayloadDataJSONKey, got)
	}
	if got := string(decoded.Events[0].Payloads[2].DecodedData); got != `null` {
		t.Fatalf("third %s = %s, want null", decodedPayloadDataJSONKey, got)
	}
	if decoded.Events[0].Payloads[0].Data != "InZhbHVlOjci" {
		t.Fatal("original payload data was not preserved")
	}
}
