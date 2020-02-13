package msgs

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func mockRow() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 30))
	binary.Write(buf, binary.BigEndian, int16(4))
	binary.Write(buf, binary.BigEndian, int32(3))
	binary.Write(buf, binary.BigEndian, []byte("123"))
	binary.Write(buf, binary.BigEndian, int32(5))
	binary.Write(buf, binary.BigEndian, []byte("hello"))
	binary.Write(buf, binary.BigEndian, int32(1))
	binary.Write(buf, binary.BigEndian, false)
	binary.Write(buf, binary.BigEndian, int32(3))
	binary.Write(buf, binary.BigEndian, []byte("456"))
	return buf.Bytes()
}

func TestChunk(t *testing.T) {
	msgBuf := NewMsgBufferFromBytes(mockRow())
	var dMsg BEDataRowMsg
	msgI, _ := dMsg.CreateFromMsgBody(msgBuf)
	extractor := msgI.(*BEDataRowMsg).Columns()
	if extractor.NumCols != 4 {
		t.Errorf("Expected 4 columns but got %d", extractor.NumCols)
	}
	ch := extractor.Chunk()
	if str := string(ch); str != "123" {
		t.Errorf("Expected 123 got %s", str)
	}
	ch = extractor.Chunk()
	if str := string(ch); str != "hello" {
		t.Errorf("Expected hello got %s", str)
	}
	ch = extractor.Chunk()
	if ch[0] != byte(0) {
		t.Error("Expected false")
	}
	ch = extractor.Chunk()
	if str := string(ch); str != "456" {
		t.Errorf("Expected 456 got %s", str)
	}
}
