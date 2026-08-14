package nexus

import (
	"fmt"
	"strings"
	"unicode/utf16"
)

const (
	continuationMask        = 0xC0
	continuationValue       = 0x80
	continuationPayloadMask = 0x3F
	continuationBits        = 6
	threeByteLeadingShift   = 12
	twoByteLeadingMask      = 0x1F
	threeByteLeadingMask    = 0x0F
	twoByteSequenceSize     = 2
	threeByteSequenceSize   = 3
	minimumTwoByteValue     = 0x80
	minimumThreeByteValue   = 0x800
)

func decodeModifiedUTF8(data []byte) (string, error) {
	ascii := true
	for _, value := range data {
		if value == 0 || value >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return string(data), nil
	}

	var decoded strings.Builder
	decoded.Grow(len(data))
	for offset := 0; offset < len(data); {
		unit, size, err := decodeModifiedUTF8Unit(data, offset)
		if err != nil {
			return "", err
		}
		if unit >= 0xD800 && unit <= 0xDBFF {
			lowOffset := offset + size
			low, lowSize, lowErr := decodeModifiedUTF8Unit(data, lowOffset)
			if lowErr != nil {
				return "", fmt.Errorf("modified UTF-8 byte %d: high surrogate U+%04X has no low surrogate: %w", offset, unit, lowErr)
			}
			if low < 0xDC00 || low > 0xDFFF {
				return "", fmt.Errorf("modified UTF-8 byte %d: high surrogate U+%04X followed by U+%04X", offset, unit, low)
			}
			decoded.WriteRune(utf16.DecodeRune(rune(unit), rune(low)))
			offset += size + lowSize
			continue
		}
		if unit >= 0xDC00 && unit <= 0xDFFF {
			return "", fmt.Errorf("modified UTF-8 byte %d: unpaired low surrogate U+%04X", offset, unit)
		}
		decoded.WriteRune(rune(unit))
		offset += size
	}
	return decoded.String(), nil
}

type modifiedUTF8Validator struct {
	offset              int
	sequence            [threeByteSequenceSize]byte
	sequenceStart       int
	sequenceLength      int
	sequenceSize        int
	highSurrogate       uint16
	highSurrogateOffset int
}

func (validator *modifiedUTF8Validator) Write(data []byte) error {
	for len(data) > 0 {
		if validator.sequenceLength == 0 && validator.highSurrogate == 0 {
			asciiLength := leadingModifiedUTF8ASCII(data)
			validator.offset += asciiLength
			data = data[asciiLength:]
			if len(data) == 0 {
				return nil
			}
		}

		value := data[0]
		data = data[1:]
		offset := validator.offset
		validator.offset++
		if validator.sequenceLength == 0 {
			switch {
			case value >= 0x01 && value <= 0x7F:
				if err := validator.consumeUnit(uint16(value), offset); err != nil {
					return err
				}
			case value == 0:
				return fmt.Errorf("modified UTF-8 byte %d: NUL must use the two-byte encoding", offset)
			case value >= 0xC0 && value <= 0xDF:
				validator.startSequence(value, offset, twoByteSequenceSize)
			case value >= 0xE0 && value <= 0xEF:
				validator.startSequence(value, offset, threeByteSequenceSize)
			default:
				return fmt.Errorf("modified UTF-8 byte %d: invalid leading byte 0x%02X", offset, value)
			}
			continue
		}

		if value&continuationMask != continuationValue {
			return fmt.Errorf("modified UTF-8 byte %d: invalid continuation byte 0x%02X", offset, value)
		}
		validator.sequence[validator.sequenceLength] = value
		validator.sequenceLength++
		if validator.sequenceLength != validator.sequenceSize {
			continue
		}

		unit, err := validator.finishSequence()
		if err != nil {
			return err
		}
		if err := validator.consumeUnit(unit, validator.sequenceStart); err != nil {
			return err
		}
	}
	return nil
}

func leadingModifiedUTF8ASCII(data []byte) int {
	length := 0
	for length < len(data) && data[length] >= 0x01 && data[length] <= 0x7F {
		length++
	}
	return length
}

func (validator *modifiedUTF8Validator) Finish() error {
	if validator.sequenceLength != 0 {
		kind := "two"
		if validator.sequenceSize == threeByteSequenceSize {
			kind = "three"
		}
		return fmt.Errorf("modified UTF-8 byte %d: incomplete %s-byte sequence", validator.sequenceStart, kind)
	}
	if validator.highSurrogate != 0 {
		return fmt.Errorf(
			"modified UTF-8 byte %d: high surrogate U+%04X has no low surrogate",
			validator.highSurrogateOffset,
			validator.highSurrogate,
		)
	}
	return nil
}

func (validator *modifiedUTF8Validator) startSequence(value byte, offset, size int) {
	validator.sequence[0] = value
	validator.sequenceStart = offset
	validator.sequenceLength = 1
	validator.sequenceSize = size
}

func (validator *modifiedUTF8Validator) finishSequence() (uint16, error) {
	first := validator.sequence[0]
	second := validator.sequence[1]
	var unit uint16
	if validator.sequenceSize == twoByteSequenceSize {
		unit = uint16(first&twoByteLeadingMask)<<continuationBits | uint16(second&continuationPayloadMask)
		if unit != 0 && unit < minimumTwoByteValue {
			return 0, fmt.Errorf("modified UTF-8 byte %d: overlong two-byte sequence", validator.sequenceStart)
		}
	} else {
		third := validator.sequence[2]
		unit = uint16(first&threeByteLeadingMask)<<threeByteLeadingShift |
			uint16(second&continuationPayloadMask)<<continuationBits |
			uint16(third&continuationPayloadMask)
		if unit < minimumThreeByteValue {
			return 0, fmt.Errorf("modified UTF-8 byte %d: overlong three-byte sequence", validator.sequenceStart)
		}
	}
	validator.sequenceLength = 0
	validator.sequenceSize = 0
	return unit, nil
}

func (validator *modifiedUTF8Validator) consumeUnit(unit uint16, offset int) error {
	if validator.highSurrogate != 0 {
		if unit < 0xDC00 || unit > 0xDFFF {
			return fmt.Errorf(
				"modified UTF-8 byte %d: high surrogate U+%04X followed by U+%04X",
				validator.highSurrogateOffset,
				validator.highSurrogate,
				unit,
			)
		}
		validator.highSurrogate = 0
		return nil
	}
	if unit >= 0xD800 && unit <= 0xDBFF {
		validator.highSurrogate = unit
		validator.highSurrogateOffset = offset
		return nil
	}
	if unit >= 0xDC00 && unit <= 0xDFFF {
		return fmt.Errorf("modified UTF-8 byte %d: unpaired low surrogate U+%04X", offset, unit)
	}
	return nil
}

func decodeModifiedUTF8Unit(data []byte, offset int) (uint16, int, error) {
	if offset >= len(data) {
		return 0, 0, fmt.Errorf("modified UTF-8 byte %d: unexpected end", offset)
	}
	first := data[offset]
	if first >= 0x01 && first <= 0x7F {
		return uint16(first), 1, nil
	}
	if first == 0 {
		return 0, 0, fmt.Errorf("modified UTF-8 byte %d: NUL must use the two-byte encoding", offset)
	}

	if first >= 0xC0 && first <= 0xDF {
		if offset+1 >= len(data) {
			return 0, 0, fmt.Errorf("modified UTF-8 byte %d: incomplete two-byte sequence", offset)
		}
		second := data[offset+1]
		if second&continuationMask != continuationValue {
			return 0, 0, fmt.Errorf("modified UTF-8 byte %d: invalid continuation byte 0x%02X", offset+1, second)
		}
		unit := uint16(first&twoByteLeadingMask)<<continuationBits | uint16(second&continuationPayloadMask)
		if unit == 0 && first == 0xC0 && second == 0x80 {
			return 0, twoByteSequenceSize, nil
		}
		if unit < minimumTwoByteValue {
			return 0, 0, fmt.Errorf("modified UTF-8 byte %d: overlong two-byte sequence", offset)
		}
		return unit, twoByteSequenceSize, nil
	}

	if first >= 0xE0 && first <= 0xEF {
		if offset+2 >= len(data) {
			return 0, 0, fmt.Errorf("modified UTF-8 byte %d: incomplete three-byte sequence", offset)
		}
		second := data[offset+1]
		third := data[offset+2]
		if second&continuationMask != continuationValue {
			return 0, 0, fmt.Errorf("modified UTF-8 byte %d: invalid continuation byte 0x%02X", offset+1, second)
		}
		if third&continuationMask != continuationValue {
			return 0, 0, fmt.Errorf("modified UTF-8 byte %d: invalid continuation byte 0x%02X", offset+twoByteSequenceSize, third)
		}
		unit := uint16(first&threeByteLeadingMask)<<threeByteLeadingShift | uint16(second&continuationPayloadMask)<<continuationBits | uint16(third&continuationPayloadMask)
		if unit < minimumThreeByteValue {
			return 0, 0, fmt.Errorf("modified UTF-8 byte %d: overlong three-byte sequence", offset)
		}
		return unit, threeByteSequenceSize, nil
	}

	return 0, 0, fmt.Errorf("modified UTF-8 byte %d: invalid leading byte 0x%02X", offset, first)
}
