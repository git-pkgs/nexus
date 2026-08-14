package nexus

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDecodeModifiedUTF8(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{"empty", nil, ""},
		{"ASCII", []byte("Homebrew"), "Homebrew"},
		{"encoded NUL", []byte{'a', 0xC0, 0x80, 'b'}, "a\x00b"},
		{"two byte", []byte{0xC2, 0xA9}, "©"},
		{"three byte", []byte{0xE2, 0x82, 0xAC}, "€"},
		{"surrogate pair", []byte{0xED, 0xA0, 0xBD, 0xED, 0xBA, 0x80}, "🚀"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeModifiedUTF8(test.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Errorf("decodeModifiedUTF8() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestDecodeModifiedUTF8Errors(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{"raw NUL", []byte{0}, "NUL must use"},
		{"short two byte", []byte{0xC2}, "incomplete two-byte"},
		{"short three byte", []byte{0xE2, 0x82}, "incomplete three-byte"},
		{"bad second byte", []byte{0xC2, 'a'}, "invalid continuation"},
		{"bad third byte", []byte{0xE2, 0x82, 'a'}, "invalid continuation"},
		{"overlong two byte", []byte{0xC1, 0x81}, "overlong two-byte"},
		{"overlong three byte", []byte{0xE0, 0x80, 0x80}, "overlong three-byte"},
		{"four byte UTF-8", []byte{0xF0, 0x9F, 0x9A, 0x80}, "invalid leading byte"},
		{"continuation first", []byte{0x80}, "invalid leading byte"},
		{"high surrogate at end", []byte{0xED, 0xA0, 0xBD}, "has no low surrogate"},
		{"high surrogate followed by text", []byte{0xED, 0xA0, 0xBD, 'a'}, "followed by"},
		{"low surrogate", []byte{0xED, 0xBA, 0x80}, "unpaired low surrogate"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeModifiedUTF8(test.input)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestModifiedUTF8Validator(t *testing.T) {
	valid := [][]byte{
		nil,
		[]byte("Homebrew"),
		{'a', 0xC0, 0x80, 'b'},
		{0xC2, 0xA9},
		{0xE2, 0x82, 0xAC},
		{0xED, 0xA0, 0xBD, 0xED, 0xBA, 0x80},
	}
	for _, input := range valid {
		for split := 0; split <= len(input); split++ {
			validator := modifiedUTF8Validator{}
			if err := validator.Write(input[:split]); err != nil {
				t.Fatalf("Write(%x[:%d]) error = %v", input, split, err)
			}
			if err := validator.Write(input[split:]); err != nil {
				t.Fatalf("Write(%x[%d:]) error = %v", input, split, err)
			}
			if err := validator.Finish(); err != nil {
				t.Fatalf("Finish(%x split at %d) error = %v", input, split, err)
			}
		}
	}

	invalid := []struct {
		input []byte
		want  string
	}{
		{[]byte{0}, "NUL must use"},
		{[]byte{0xC2}, "incomplete two-byte"},
		{[]byte{0xE2, 0x82}, "incomplete three-byte"},
		{[]byte{0xC2, 'a'}, "invalid continuation"},
		{[]byte{0xE2, 0x82, 'a'}, "invalid continuation"},
		{[]byte{0xC1, 0x81}, "overlong two-byte"},
		{[]byte{0xE0, 0x80, 0x80}, "overlong three-byte"},
		{[]byte{0xF0, 0x9F, 0x9A, 0x80}, "invalid leading byte"},
		{[]byte{0x80}, "invalid leading byte"},
		{[]byte{0xED, 0xA0, 0xBD}, "has no low surrogate"},
		{[]byte{0xED, 0xA0, 0xBD, 'a'}, "followed by"},
		{[]byte{0xED, 0xBA, 0x80}, "unpaired low surrogate"},
	}
	for _, test := range invalid {
		validator := modifiedUTF8Validator{}
		err := validator.Write(test.input)
		if err == nil {
			err = validator.Finish()
		}
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("validate(%x) error = %v, want containing %q", test.input, err, test.want)
		}
	}
}

func FuzzDecodeModifiedUTF8(f *testing.F) {
	f.Add([]byte("artifact"))
	f.Add([]byte{0xC0, 0x80})
	f.Add([]byte{0xED, 0xA0, 0xBD, 0xED, 0xBA, 0x80})
	f.Add([]byte{0xFF, 0x00, 0x80})

	f.Fuzz(func(t *testing.T, input []byte) {
		decoded, err := decodeModifiedUTF8(input)
		if err == nil && !utf8.ValidString(decoded) {
			t.Fatalf("successful decode returned invalid UTF-8: %x", decoded)
		}

		validator := modifiedUTF8Validator{}
		split := len(input) / 2
		validationErr := validator.Write(input[:split])
		if validationErr == nil {
			validationErr = validator.Write(input[split:])
		}
		if validationErr == nil {
			validationErr = validator.Finish()
		}
		if (err == nil) != (validationErr == nil) {
			t.Fatalf("decode error = %v, validation error = %v", err, validationErr)
		}
	})
}
