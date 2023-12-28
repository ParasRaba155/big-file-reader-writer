package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"time"
)

type chunkReaderWriter struct {
	// destination writer to write to
	dest     io.WriteCloser
	destPath string

	buffer   []byte
	capacity int // capacity of the buffer

	// number of chunks that have been read
	chunkRead int64
	// number of bytes that have been read so far
	bytesRead int64

	// to keep track of the last read bytes
	lastReadBytes int

	// number of chunks that have been written
	chunkWrote int64
	// number of bytes that have been written so far
	bytesWrote int64

	paused bool // to keep track if the file read/write was paused

	bufReader *bufio.Reader

	bufWriter *bufio.Writer
}

func NewChunkReaderWriter(cap int) *chunkReaderWriter {
	// the buffer will be initialized with 0 length initial given capacity
	return &chunkReaderWriter{
		buffer:   make([]byte, cap),
		capacity: cap,
	}
}

// createTempFile creates a temporary file with the random file name
// and same extension as the extension provided in the argument
func createTempFile(ext string) *os.File {
	dest := fmt.Sprintf("file-%03d%s", rand.Intn(101), ext)
	file, err := os.Create(dest)
	if err != nil {
		log.Fatalf("[ERROR] could not create [ file = %s ]: %v", dest, err)
	}
	log.Printf("[DEBUG] created the file: %s", dest)
	return file
}

func (rw *chunkReaderWriter) ReadAndWrite(
	file io.Reader,
	filename string,
	sig <-chan string,
) {
	// creating buffered reader in case if we have paused the reader/writer
	// the reader will not be created
	if rw.bufReader == nil {
		rw.bufReader = bufio.NewReaderSize(file, rw.capacity)
	}

	// creating buffered writer in case if we have paused the reader/writer
	// the writer will not be created
	if rw.bufWriter == nil {
		log.Printf("[DEBUG] bufWriter is nil")
		destFile := createTempFile(path.Ext(filename))
		rw.dest = destFile
		rw.destPath = destFile.Name()
		rw.bufWriter = bufio.NewWriterSize(rw.dest, rw.capacity)
	}

	// close the destination file
	defer func() {
		log.Printf("[DEBUG] closing the dest file")
		// c.bufWriter.Flush()
		err := rw.dest.Close()
		if err != nil {
			log.Printf("[WARN] error in closing the file: %v", err)
		}
	}()

	// signal for closing the destination file
	closingSignal := make(chan struct{})
loop:
	for {
		select {
		// case we receive the pause signal
		// break the loop
		case msg := <-sig:
			log.Printf("[INFO] message received from the signal [msg=%s]", msg)
			log.Printf("[INFO] read stats [chunk=%d] [bytes=%d]", rw.chunkRead, rw.bytesRead)
			log.Printf("[INFO] write stats [chunk=%d] [bytes=%d]", rw.chunkWrote, rw.bytesWrote)
			// mark the flag for pausing the reader writer
			rw.paused = true
			break loop
		// case we receive the close file signal
		// break the loop
		case <-closingSignal:
			log.Printf("[DEBUG] close signal received")
			log.Printf("[DEBUG] number of bytes in buffer: %d", rw.bufWriter.Buffered())
			log.Printf("[DEBUG] number of bytes in available: %d", rw.bufWriter.Available())
			break loop
		// continue reading and writing
		default:
			if !rw.paused {
				rw.read(closingSignal)
				rw.write()
				continue
			}
			log.Printf("[DEBUG] chunk read writer was stopped")
			discardedByte, err := rw.bufReader.Discard(int(rw.bytesRead))
			if err != nil {
				log.Printf("[ERROR] discard error: %v", err)
				break loop
			}
			if err = rw.bufWriter.Flush(); err != nil {
				log.Printf("[WARN] error in flushing the writer: %v", err)
			}
			file, err := os.OpenFile(rw.destPath, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				log.Printf("[ERROR] error in reopening the file with append permission: %v", err)
				return
			}
			rw.bufWriter = bufio.NewWriter(file)
			log.Printf("[DEBUG] [discard=%d] [read=%d]", discardedByte, rw.bytesRead)
			rw.read(closingSignal)
			rw.write()
			rw.paused = false

		}
	}
	log.Printf("[INFO] [read=%d kb] [wrote=%d kb]", rw.bytesRead, rw.bytesWrote)
}

func (rw *chunkReaderWriter) read(closeSignal chan<- struct{}) {
	defer log.Printf("[DEBUG] in reader: %d", rw.bytesRead)
	// elongate the read time
	time.Sleep(time.Second)
	// we have initialized the buffer with 0 length and some capacity
	// so fill the buffer accordingly
	n, err := rw.bufReader.Read(rw.buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		log.Printf("[ERROR] reading the file: %v", err)
		return
	}
	// we have completed reading of file
	// send the close signal and stop rw from further read
	if errors.Is(err, io.EOF) {
		log.Println("[INFO] Reached the End of the File")
		go func() {
			closeSignal <- struct{}{}
		}()
		return
	}

	if n == 0 {
		log.Printf("[WARN] read 0 byte, [read error=%v]", err)
		return
	}
	rw.chunkRead++
	rw.lastReadBytes = n
	rw.bytesRead += int64(n)
}

func (rw *chunkReaderWriter) write() {
	defer log.Printf("[DEBUG] in writer: %d", rw.bytesWrote)

	n, err := rw.bufWriter.Write(rw.buffer[:rw.lastReadBytes])
	if err != nil {
		log.Printf("[ERROR] writing the file: %v", err)
		return
	}
	if n != rw.lastReadBytes {
		log.Printf("[WARN] wrote %d bytes expected %d", n, rw.bytesRead)
		return
	}
	rw.chunkWrote++
	rw.bytesWrote += int64(n)
}
