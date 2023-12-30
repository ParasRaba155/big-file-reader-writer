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

	// errReadError is for testing purpose should be thrown when
	// using with Reader implementation
	errReadError = errors.New("mock reader error")

	// errWriteError is for testing purpose should be thrown when
	// using with Writer implementation
	errWriteError = errors.New("mock reader error")
)

const (
	defaultTestReadWriteBufSize = 100
	defaultTestBufCapacity      = 500
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

// errReader implements the io.Reader interface and will return 0, errReader.err
type errReader struct {
	err error
}

func (er *errReader) Read([]byte) (int, error) {
	return 0, er.err
}

// errWriter implements the io.Writer interface and will return 0, errWriter.err
type errWriter struct {
	err error
}

// Write implements io.Writer.
func (ew *errWriter) Write([]byte) (n int, err error) {
	return 0, ew.err
}

var (
	_ io.WriteCloser = (*testWriteCloser)(nil)
	_ io.Reader      = (*errReader)(nil)
	_ io.Writer      = (*errWriter)(nil)
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

// setupChunkReadWriter will initialize the chunkReadWriter with initial
// buffer of capacity and will create buffReader with bufSize
// it will also create new destination with buffer size of 2 times the capacity
// and reader to mock form data of buffer size 2 times the capacity
//
//nolint:unparam
func setupChunkReadWriter(capacity, bufSize int) chunkReaderWriter {
	writeTo := make([]byte, 2*capacity)
	readFrom := genByteArray(2 * capacity)
	buf := make([]byte, capacity)
	rw := chunkReaderWriter{
		dest:     NewTestWriteCloser(writeTo),
		buffer:   buf,
		capacity: capacity,
		bufReader: bufio.NewReaderSize(
			bytes.NewReader(readFrom), bufSize,
		),
	}
	return rw
}

// captureLogOutput will capture the log output in a buffer
// and will set the log output to Stderr once the function its done
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
		rw := setupChunkReadWriter(
			defaultTestBufCapacity, defaultTestReadWriteBufSize,
		)
		closeSig := make(chan struct{})
		rw.read(closeSig)
		if rw.bytesRead != defaultTestBufCapacity {
			t.Errorf(
				"rw.bytesRead = %d, want %d",
				rw.bytesRead,
				defaultTestBufCapacity,
			)
		}

		for i := 0; i < defaultTestBufCapacity; i++ {
			if rw.buffer[i] != byte(i%256) {
				t.Errorf("rw.buffer[%d] = %d, expected rw.buffer[%d] = %d",
					i, rw.buffer[i], i, i%256)
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
		rw := setupChunkReadWriter(
			defaultTestBufCapacity,
			defaultTestReadWriteBufSize,
		)
		closeSig := make(chan struct{})
		// create a buffered reader of 0 byte
		rw.bufReader = bufio.NewReaderSize(
			bytes.NewBuffer([]byte{}), defaultTestReadWriteBufSize,
		)

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
		rw := setupChunkReadWriter(
			defaultTestBufCapacity,
			defaultTestReadWriteBufSize,
		)
		closeSig := make(chan struct{})

		// set the bufReader to our errReader
		rw.bufReader = bufio.NewReaderSize(
			&errReader{err: errReadError}, defaultTestReadWriteBufSize,
		)

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
			"[ERROR] reading the file: %v", errReadError,
		)

		if !strings.Contains(logOutput, expectedLog) {
			t.Errorf(
				"Got logOutput: '%s' expected: '%s'",
				logOutput,
				expectedLog,
			)
		}
	})

	t.Run("Handles the non nil error with 0 bytes read", func(t *testing.T) {
		rw := setupChunkReadWriter(
			defaultTestBufCapacity,
			defaultTestReadWriteBufSize,
		)
		closeSig := make(chan struct{})

		// set the bufReader to our errReader
		rw.bufReader = bufio.NewReaderSize(
			&errReader{err: nil}, defaultTestReadWriteBufSize,
		)

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
			t.Errorf(
				"Got logOutput: '%s' expected: '%s'",
				logOutput,
				expectedLog,
			)
		}
	})
}

func TestWrite(t *testing.T) {
	t.Parallel()
	t.Run("Write Chunk", func(t *testing.T) {
		rw := setupChunkReadWriter(
			defaultTestBufCapacity,
			defaultTestReadWriteBufSize,
		)
		rw.bufWriter = bufio.NewWriterSize(rw.dest, defaultTestReadWriteBufSize)

		const (
			initialBytesWrote = 525
			initialChunkWrote = 2
			lastReadBytes     = 60
		)

		rw.lastReadBytes = lastReadBytes

		// 525 % 500 (capacity) = 25 bytes have already been read
		// and written from current buffer
		rw.bytesWrote = initialBytesWrote
		rw.chunkWrote = initialChunkWrote

		rw.write()

		if rw.bytesWrote != int64(
			initialBytesWrote+lastReadBytes-(initialBytesWrote%rw.capacity),
		) {
			t.Errorf(
				"rw.bytesWrote = %d, want = %d",
				rw.bytesWrote,
				int64(
					initialBytesWrote+lastReadBytes-(initialBytesWrote%rw.capacity),
				),
			)
		}

		if rw.chunkWrote != initialChunkWrote+1 {
			t.Errorf(
				"rw.chunkWrote = %d, want = %d",
				rw.chunkRead,
				initialChunkWrote+1,
			)
		}

		if rw.lastReadBytes != lastReadBytes {
			t.Errorf(
				"rw.lastReadBytes should not change, rw.lastReadBytes = %d, want = %d",
				rw.lastReadBytes,
				lastReadBytes,
			)
		}
	})

	t.Run("Handler error", func(t *testing.T) {
		rw := setupChunkReadWriter(
			defaultTestBufCapacity, defaultTestReadWriteBufSize,
		)
		rw.bufWriter = bufio.NewWriterSize(
			&errWriter{err: errWriteError}, defaultTestReadWriteBufSize,
		)

		const (
			initialBytesWrote = 130
			initialChunkWrote = 1
			lastReadBytes     = 270
		)
		rw.bytesWrote = initialBytesWrote
		rw.chunkWrote = initialChunkWrote
		rw.lastReadBytes = lastReadBytes

		logOutput := captureLogOutput(func() {
			rw.write()
		})

		if rw.bytesWrote != initialBytesWrote {
			t.Errorf(
				"rw.bytesWrote should not change, rw.bytesWrote = %d, want %d",
				rw.bytesWrote,
				initialBytesWrote,
			)
		}

		if rw.chunkWrote != initialChunkWrote {
			t.Errorf(
				"rw.chunkWrote should not change, rw.chunkWrote = %d, want %d",
				rw.chunkWrote,
				initialChunkWrote,
			)
		}

		expectedLog := fmt.Sprintf(
			"[ERROR] writing the file: %v",
			errWriteError,
		)

		if !strings.Contains(logOutput, expectedLog) {
			t.Errorf(
				"Got logOutput: '%s' expected: '%s'",
				logOutput,
				expectedLog,
			)
		}
	})
}
