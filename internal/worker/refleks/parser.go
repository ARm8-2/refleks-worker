package refleks

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/xxh3"
)

const (
	runMagic                 = "RFLK"
	runVersion         uint8 = 1
	runCompressionNone uint8 = 0
	runCompressionZstd uint8 = 1

	runHeaderSize   = 4 + 1 + 1 + 8
	runChecksumSize = 8

	statTypeString uint8 = 1
	statTypeInt    uint8 = 2
	statTypeFloat  uint8 = 3
	statTypeBool   uint8 = 4

	maxFileNameBytes = 1024
	maxStringBytes   = 1 << 20
	maxStatsEntries  = 50000
	maxEventRows     = 500000
	maxEventCols     = 128
	maxMousePoints   = 20000000
)

// MousePoint is one recorded mouse trace sample.
type MousePoint struct {
	TS      int64
	X       int32
	Y       int32
	Buttons int32
}

// Environment stores runtime metadata captured with the run.
type Environment struct {
	AppVersion  string
	OS          string
	Arch        string
	OSVersion   string
	SteamID     string
	PersonaName string

	CPUName    string
	CPUCores   int32
	GPUName    string
	RAMTotalMB int32

	DisplayHz    float64
	ScreenWidth  int32
	ScreenHeight int32
	IsWindowed   bool

	MouseName    string
	MouseVID     string
	MousePID     string
	MouseMI      string
	MouseBackend string

	TracePoints   int32
	TraceDuration float64
	SampleRate    int32
}

// File is the decoded representation of a .refleks payload.
type File struct {
	FileName      string
	EpochMilli    int64
	FormatVersion uint8
	Stats         map[string]any
	Events        [][]string
	MouseTrace    []MousePoint
	Env           Environment
}

// Parse decodes one raw .refleks payload.
func Parse(raw []byte) (File, error) {
	if len(raw) < runHeaderSize+runChecksumSize {
		return File{}, fmt.Errorf("invalid refleks file: file too small")
	}
	if string(raw[:4]) != runMagic {
		return File{}, fmt.Errorf("invalid refleks file: bad magic")
	}

	version := raw[4]
	if version != runVersion {
		return File{}, fmt.Errorf("invalid refleks file: unsupported version %d", version)
	}

	compression := raw[5]
	epochMilli := int64(binary.LittleEndian.Uint64(raw[6:14]))
	payload := raw[runHeaderSize : len(raw)-runChecksumSize]

	wantChecksum := binary.LittleEndian.Uint64(raw[len(raw)-runChecksumSize:])
	if gotChecksum := xxh3.Hash(payload); gotChecksum != wantChecksum {
		return File{}, fmt.Errorf("invalid refleks file: checksum mismatch")
	}

	reader, closeReader, err := newPayloadReader(bytes.NewReader(payload), compression)
	if err != nil {
		return File{}, fmt.Errorf("invalid refleks file: %w", err)
	}
	defer closeReader()

	fileName, err := readString(reader, maxFileNameBytes)
	if err != nil {
		return File{}, fmt.Errorf("invalid refleks file: read file name: %w", err)
	}
	if strings.TrimSpace(fileName) == "" {
		return File{}, fmt.Errorf("invalid refleks file: file name is empty")
	}

	stats, err := readStats(reader)
	if err != nil {
		return File{}, fmt.Errorf("invalid refleks file: read stats: %w", err)
	}

	events, err := readEvents(reader)
	if err != nil {
		return File{}, fmt.Errorf("invalid refleks file: read events: %w", err)
	}

	trace, err := readMouseTrace(reader)
	if err != nil {
		return File{}, fmt.Errorf("invalid refleks file: read mouse trace: %w", err)
	}

	env, err := readEnvironment(reader)
	if err != nil {
		return File{}, fmt.Errorf("invalid refleks file: read environment: %w", err)
	}

	if err := ensureEOF(reader); err != nil {
		return File{}, fmt.Errorf("invalid refleks file: %w", err)
	}

	return File{
		FileName:      fileName,
		EpochMilli:    epochMilli,
		FormatVersion: version,
		Stats:         stats,
		Events:        events,
		MouseTrace:    trace,
		Env:           env,
	}, nil
}

func newPayloadReader(r io.Reader, compression uint8) (io.Reader, func(), error) {
	switch compression {
	case runCompressionNone:
		return r, func() {}, nil
	case runCompressionZstd:
		decoder, err := zstd.NewReader(r)
		if err != nil {
			return nil, nil, fmt.Errorf("zstd decoder: %w", err)
		}
		return decoder, decoder.Close, nil
	default:
		return nil, nil, fmt.Errorf("unsupported compression %d", compression)
	}
}

func readStats(r io.Reader) (map[string]any, error) {
	count, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if count > maxStatsEntries {
		return nil, fmt.Errorf("too many stats entries: %d", count)
	}

	stats := make(map[string]any, count)
	for i := uint32(0); i < count; i++ {
		key, err := readString(r, maxStringBytes)
		if err != nil {
			return nil, err
		}

		typeTag, err := readUint8(r)
		if err != nil {
			return nil, err
		}

		switch typeTag {
		case statTypeString:
			value, err := readString(r, maxStringBytes)
			if err != nil {
				return nil, err
			}
			stats[key] = value
		case statTypeInt:
			value, err := readInt64(r)
			if err != nil {
				return nil, err
			}
			stats[key] = value
		case statTypeFloat:
			value, err := readFloat64(r)
			if err != nil {
				return nil, err
			}
			stats[key] = value
		case statTypeBool:
			value, err := readUint8(r)
			if err != nil {
				return nil, err
			}
			stats[key] = value == 1
		default:
			return nil, fmt.Errorf("unsupported stats value type %d", typeTag)
		}
	}

	return stats, nil
}

func readEvents(r io.Reader) ([][]string, error) {
	rows, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if rows > maxEventRows {
		return nil, fmt.Errorf("too many event rows: %d", rows)
	}

	events := make([][]string, rows)
	for i := uint32(0); i < rows; i++ {
		cols, err := readUint32(r)
		if err != nil {
			return nil, err
		}
		if cols > maxEventCols {
			return nil, fmt.Errorf("too many event columns: %d", cols)
		}

		row := make([]string, cols)
		for j := uint32(0); j < cols; j++ {
			value, err := readString(r, maxStringBytes)
			if err != nil {
				return nil, err
			}
			row[j] = value
		}
		events[i] = row
	}

	return events, nil
}

func readMouseTrace(r io.Reader) ([]MousePoint, error) {
	count, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if count > maxMousePoints {
		return nil, fmt.Errorf("too many mouse points: %d", count)
	}

	trace := make([]MousePoint, count)
	for i := uint32(0); i < count; i++ {
		ts, err := readInt64(r)
		if err != nil {
			return nil, err
		}
		x, err := readInt32(r)
		if err != nil {
			return nil, err
		}
		y, err := readInt32(r)
		if err != nil {
			return nil, err
		}
		buttons, err := readInt32(r)
		if err != nil {
			return nil, err
		}
		trace[i] = MousePoint{TS: ts, X: x, Y: y, Buttons: buttons}
	}

	return trace, nil
}

func readEnvironment(r io.Reader) (Environment, error) {
	appVersion, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	osName, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	arch, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	osVersion, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	steamID, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	personaName, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}

	cpuName, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	cpuCores, err := readInt32(r)
	if err != nil {
		return Environment{}, err
	}
	gpuName, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	ramTotalMB, err := readInt32(r)
	if err != nil {
		return Environment{}, err
	}

	displayHz, err := readFloat64(r)
	if err != nil {
		return Environment{}, err
	}
	screenWidth, err := readInt32(r)
	if err != nil {
		return Environment{}, err
	}
	screenHeight, err := readInt32(r)
	if err != nil {
		return Environment{}, err
	}
	isWindowedRaw, err := readUint8(r)
	if err != nil {
		return Environment{}, err
	}

	mouseName, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	mouseVID, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	mousePID, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	mouseMI, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}
	mouseBackend, err := readString(r, maxStringBytes)
	if err != nil {
		return Environment{}, err
	}

	tracePoints, err := readInt32(r)
	if err != nil {
		return Environment{}, err
	}
	traceDuration, err := readFloat64(r)
	if err != nil {
		return Environment{}, err
	}
	sampleRate, err := readInt32(r)
	if err != nil {
		return Environment{}, err
	}

	return Environment{
		AppVersion:    appVersion,
		OS:            osName,
		Arch:          arch,
		OSVersion:     osVersion,
		SteamID:       steamID,
		PersonaName:   personaName,
		CPUName:       cpuName,
		CPUCores:      cpuCores,
		GPUName:       gpuName,
		RAMTotalMB:    ramTotalMB,
		DisplayHz:     displayHz,
		ScreenWidth:   screenWidth,
		ScreenHeight:  screenHeight,
		IsWindowed:    isWindowedRaw == 1,
		MouseName:     mouseName,
		MouseVID:      mouseVID,
		MousePID:      mousePID,
		MouseMI:       mouseMI,
		MouseBackend:  mouseBackend,
		TracePoints:   tracePoints,
		TraceDuration: traceDuration,
		SampleRate:    sampleRate,
	}, nil
}

func readString(r io.Reader, maxLength uint32) (string, error) {
	length, err := readUint32(r)
	if err != nil {
		return "", err
	}
	if length > maxLength {
		return "", fmt.Errorf("string too large: %d", length)
	}
	if length == 0 {
		return "", nil
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func ensureEOF(r io.Reader) error {
	var trailing [1]byte
	count, err := r.Read(trailing[:])
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("trailing payload bytes")
	}
	return nil
}

func readUint32(r io.Reader) (uint32, error) {
	var value uint32
	if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func readUint8(r io.Reader) (uint8, error) {
	var value uint8
	if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func readInt32(r io.Reader) (int32, error) {
	var value int32
	if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func readInt64(r io.Reader) (int64, error) {
	var value int64
	if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func readFloat64(r io.Reader) (float64, error) {
	var value float64
	if err := binary.Read(r, binary.LittleEndian, &value); err != nil {
		return 0, err
	}
	return value, nil
}
