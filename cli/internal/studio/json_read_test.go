package studio

import "testing"

func TestNativeJSONEncodingAdmission(t *testing.T) {
	for _, document := range [][]byte{[]byte(`null`), []byte(`{"text":"café"}`), []byte(`9007199254740993`)} {
		if !validNativeJSON(document) {
			t.Fatal("valid UTF-8 document refused")
		}
	}
	for _, document := range [][]byte{[]byte{'"', 0xe9, '"'}, []byte{'"', 0xff, '"'}, []byte(`{"invalid":}`)} {
		if validNativeJSON(document) {
			t.Fatal("invalid encoding or document admitted")
		}
	}
}
