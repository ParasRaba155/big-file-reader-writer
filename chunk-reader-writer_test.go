package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
)

var (
	// errNilBuf is when the buffer in the testWriteCloser is nil
	errNilBuf = errors.New("nil buffer in testWriterClose")

	// errReaderError is for testing purpose should be thrown when
	// using with Reader implementation
	errReaderError = errors.New("mock reader error")
)

// testWriteCloser is the stub for test package to implement the io.WriteCloser
// interface
type testWriteCloser struct {
	buf *bytes.Buffer
}

func NewTestWriteCloser(buf []byte) *testWriteCloser {
	return &testWriteCloser{
		buf: bytes.NewBuffer(buf),
	}
}

// Close implements io.WriteCloser.
func (t *testWriteCloser) Close() error {
	if t.buf == nil {
		return errNilBuf
	}
	return nil
}

// Write implements io.WriteCloser.
func (t *testWriteCloser) Write(p []byte) (n int, err error) {
	return t.buf.Write(p)
}

// errReader implements the io.Reader interface and will return 0 bytes Read
// and error with which we initilize the `errReader`
type errReader struct {
	err error
}

func (er *errReader) Read([]byte) (int, error) {
	return 0, er.err
}

var (
	_ io.WriteCloser = (*testWriteCloser)(nil)
	_ io.Reader      = (*errReader)(nil)
)

// genByteArray will return byte array with each element its own index
// modulo 256
func genByteArray(length int) []byte {
	arr := make([]byte, length)
	for i := 0; i < length; i++ {
		arr[i] = byte(i % 256)
	}
	return arr
}

// setupChunkReadWriter will initilise the chunkReadWriter with initial
// buffer of 100 bytes, capacity 100, will initiate bufReader with new reader
// which is byte array of 1000 bytes and destination writer with 1000 bytes
func setupChunkReadWriter() chunkReaderWriter {
	writeTo := make([]byte, 1000)
	readFrom := genByteArray(1000)
	buf := make([]byte, 100)
	rw := chunkReaderWriter{
		dest:      NewTestWriteCloser(writeTo),
		buffer:    buf,
		capacity:  100,
		bufReader: bufio.NewReaderSize(bytes.NewReader(readFrom), 100),
	}
	return rw
}

func captureLogOutput(f func()) string {
	var buf bytes.Buffer
	log.SetOutput(&buf)

	// reset the log output to stderr after capturing the log output
	defer log.SetOutput(os.Stderr)

	f()
	return buf.String()
}

func TestRead(t *testing.T) {
	t.Parallel()
	t.Run("Read chunk", func(t *testing.T) {
		rw := setupChunkReadWriter()
		closeSig := make(chan struct{})
		rw.read(closeSig)
		if rw.bytesRead != 100 {
			t.Errorf("rw.bytesRead = %d, want %d", rw.bytesRead, 100)
		}

		var i uint8
		for i = 0; i < 100; i++ {
			if rw.buffer[i] != i {
				t.Errorf("rw.buffer[%d] = %d, expected rw.buffer[%d] = %d",
					i, rw.buffer[i], i, i)
			}
		}

		if rw.chunkRead != 1 {
			t.Errorf("rw.chunkRead = %d, want %d", rw.chunkRead, 1)
		}

		if len(closeSig) != 0 {
			t.Errorf("len(closeSig) = %d, expected %d", len(closeSig), 0)
		}

		if rw.reachedEOF {
			t.Errorf("rw.reachedEOF = %t, expected %t", true, false)
		}
	})

	t.Run("Read empty buffer", func(t *testing.T) {
		rw := setupChunkReadWriter()
		closeSig := make(chan struct{})
		// create a buffered reader of 0 byte
		rw.bufReader = bufio.NewReaderSize(bytes.NewBuffer([]byte{}), 100)

		// read will send the signal to closeSig as EOF will be reached in this
		// case so to test that we will create new go routine which will just
		// block for receiving from the `closeSig` and then log the fact
		// the signal was received
		rw.read(closeSig)

		// waitgroup for synchronization of the closeSig routine
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			<-closeSig
			t.Log("close signal was received")
			wg.Done()
		}()

		if rw.chunkRead != 0 {
			t.Errorf("rw.chunkRead = %d, want %d", rw.chunkRead, 0)
		}

		if rw.bytesRead != 0 {
			t.Errorf("rw.bytesRead = %d, want %d", rw.bytesRead, 0)
		}

		if !rw.reachedEOF {
			t.Errorf("rw.reachedEOF = %t, expected %t", false, true)
		}

		wg.Wait()
	})

	t.Run("Handles the non EOF Error", func(t *testing.T) {
		rw := setupChunkReadWriter()
		closeSig := make(chan struct{})

		// set the bufReader to our errReader
		rw.bufReader = bufio.NewReaderSize(&errReader{err: errReaderError}, 100)

		logOutput := captureLogOutput(func() {
			rw.read(closeSig)
		})

		if rw.bytesRead != 0 {
			t.Errorf("rw.bytesRead = %d, want %d", rw.bytesRead, 0)
		}

		if rw.reachedEOF {
			t.Errorf("rw.reachedEOF = %t, expected %t", true, false)
		}

		if rw.chunkRead != 0 {
			t.Errorf("rw.chunkRead = %d, want %d", rw.chunkRead, 0)
		}

		expectedLog := fmt.Sprintf(
			"[ERROR] reading the file: %v", errReaderError,
		)

		if !strings.Contains(logOutput, expectedLog) {
			t.Errorf("Got logOutput: '%s' expected: '%s'", logOutput, expectedLog)
		}
	})

	t.Run("Handles the non nil error with 0 bytes read", func(t *testing.T) {
		rw := setupChunkReadWriter()
		closeSig := make(chan struct{})

		// set the bufReader to our errReader
		rw.bufReader = bufio.NewReaderSize(&errReader{err: nil}, 100)

		logOutput := captureLogOutput(func() {
			rw.read(closeSig)
		})

		if rw.bytesRead != 0 {
			t.Errorf("rw.bytesRead = %d, want %d", rw.bytesRead, 0)
		}

		if rw.reachedEOF {
			t.Errorf("rw.reachedEOF = %t, expected %t", true, false)
		}

		if rw.chunkRead != 0 {
			t.Errorf("rw.chunkRead = %d, want %d", rw.chunkRead, 0)
		}

		expectedLog := fmt.Sprintf("[WARN] read 0 byte, [read error=%v]", nil)

		if !strings.Contains(logOutput, expectedLog) {
			t.Errorf("Got logOutput: '%s' expected: '%s'", logOutput, expectedLog)
		}
	})
}

