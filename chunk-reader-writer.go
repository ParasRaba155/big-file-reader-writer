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

	buffer  []byte
	capcity int // capacity of the buffer

	// number of chunks that have been read
	chunkRead int64
	// number of bytes that have been read so far
	bytesRead int64

	// number of chunks that have been written
	chunkWrote int64
	// number of bytes that have been written so far
	bytesWrote int64

	stopped bool // to keep track if the file read/write was stopped

	bufReader *bufio.Reader

	bufWriter *bufio.Writer
}

func NewChunkReaderWriter(cap int) *chunkReaderWriter {
	return &chunkReaderWriter{
		buffer:  make([]byte, 0, cap),
		capcity: cap,
	}
}

func createTempFile(ext string) *os.File {
	dest := fmt.Sprintf("file-%03d%s", rand.Intn(101), ext)
	file, err := os.Create(dest)
	if err != nil {
		log.Fatalf("[ERROR] could not create [ file = %s ]: %v", dest, err)
	}
	log.Printf("[DEBUG] created the file: %s", dest)
	return file
}

func (c *chunkReaderWriter) ReadAndWrite(
	file io.Reader,
	filename string,
	sig <-chan string,
) {
	if c.bufReader == nil {
		c.bufReader = bufio.NewReaderSize(file, c.capcity)
	}

	if c.bufWriter == nil {
		log.Printf("[DEBUG] bufWriter is nil")
		destFile := createTempFile(path.Ext(filename))
		c.dest = destFile
		c.destPath = destFile.Name()
		c.bufWriter = bufio.NewWriterSize(c.dest, c.capcity)
	}
	defer func() {
		log.Printf("[DEBUG] closing the dest file")
		// c.bufWriter.Flush()
		err := c.dest.Close()
		if err != nil {
			log.Printf("[WARN] error in closing the file: %v", err)
		}
	}()
	closingSignal := make(chan struct{})
loop:
	for {
		select {
		case msg := <-sig:
			log.Printf("[INFO] message received from the signal [msg=%s]", msg)
			log.Printf("[INFO] read stats [chunk=%d] [bytes=%d]", c.chunkRead, c.bytesRead)
			log.Printf("[INFO] write stats [chunk=%d] [bytes=%d]", c.chunkWrote, c.bytesWrote)
			c.stopped = true
			break loop
		case <-closingSignal:
			log.Printf("[DEBUG] close signal received")
			log.Printf("[DEBUG] number of bytes in buffer: %d", c.bufWriter.Buffered())
			log.Printf("[DEBUG] number of bytes in availabe: %d", c.bufWriter.Available())
			break loop
		default:
			if !c.stopped {
				c.read(closingSignal)
				c.write(closingSignal)
			} else {
				log.Printf("[DEBUG] chunk read writer was stopped")
				discardedByte, err := c.bufReader.Discard(int(c.bytesRead))
				if err != nil {
					log.Printf("[ERROR] discard error: %v", err)
					break loop
				}
				if err = c.bufWriter.Flush(); err != nil {
					log.Printf("[WARN] error in flushing the writer: %v", err)
				}
				file, err := os.OpenFile(c.destPath, os.O_APPEND|os.O_WRONLY, 0644)
				if err != nil {
					log.Printf("[ERROR] error in reopening the file with append permission: %v", err)
					return
				}
				c.bufWriter = bufio.NewWriter(file)
				log.Printf("[DEBUG] [discard=%d] [read=%d]", discardedByte, c.bytesRead)
				c.read(closingSignal)
				c.write(closingSignal)
				c.stopped = false
			}
		}
	}
	log.Printf("[INFO] [read=%d kb] [wrote=%d kb]", c.bytesRead, c.bytesWrote)
}

func (c *chunkReaderWriter) read(closeSignal chan<- struct{}) {
	defer log.Printf("[DEBUG] in reader: %d", c.bytesRead)
	log.Printf("[DEBUG] reader c.buffer: %v", c.buffer)
	time.Sleep(time.Second)
	n, err := c.bufReader.Read(c.buffer[:cap(c.buffer)])
	if err != nil && !errors.Is(err, io.EOF) {
		log.Printf("[ERROR] reading the file: %v", err)
		return
	}

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
	c.chunkRead++
	c.bytesRead += int64(n)
}

func (c *chunkReaderWriter) write(closeSignal <-chan struct{}) {
	select {
	case <-closeSignal:
		return
	default:
		defer log.Printf("[DEBUG] in writer: %d", c.bytesWrote)
		log.Printf("[DEBUG] write c.buffer: %v", c.buffer)
		n, err := c.bufWriter.Write(c.buffer[0:cap(c.buffer)])
		if err != nil {
			log.Printf("[ERROR] writing the file: %v", err)
			return
		}
		// if n != len(c.buffer) {
		// 	log.Printf("[WARN] wrote %d bytes expected %d", n, len(c.buffer))
		// 	return
		// }
		c.chunkWrote++
		c.bytesWrote += int64(n)
	}
}
