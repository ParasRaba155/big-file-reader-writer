package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

const defaultCapacity = 1024

func main() {
	router := gin.Default()
	gin.SetMode(gin.TestMode)
	h := handler{
		store: NewChunkReaderWriter(defaultCapacity),
		sig:   make(chan string),
	}
	router.POST("/upload", h.uploadHandler)
	router.POST("/status", h.statusHandler)

	if err := router.Run(":8080"); err != nil {
		log.Fatalf("could not run the router: %v", err)
	}
}

type handler struct {
	store *chunkReaderWriter
	sig   chan string
}

func (h *handler) uploadHandler(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		log.Printf("[ERROR] error in Form File: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.store.ReadAndWrite(file, header.Filename, h.sig)

	c.JSON(
		http.StatusOK,
		gin.H{
			"message": "Successfully uploaded the file",
			"read":    h.store.bytesRead,
			"wrote":   h.store.bytesWrote,
		},
	)
}

func (h *handler) statusHandler(c *gin.Context) {
	status, ok := c.GetQuery("status")
	if !ok {
		c.JSON(
			http.StatusBadRequest,
			gin.H{"message": "must provide status in query params"},
		)
		return
	}
	if status != "pause" {
		c.JSON(
			http.StatusBadRequest,
			gin.H{
				"message": fmt.Sprintf(
					"Status has to be 'pause' can not be '%s'",
					status,
				),
			},
		)
		return
	}
	go func(sig chan string) {
		sig <- status
	}(h.sig)
}
