package main

import (
	"github.com/gin-gonic/gin"
	"github.com/karthiklsarma/cedar-listener/stream"
	"github.com/karthiklsarma/cedar-logging/logging"
)

func main() {
	logging.Info("Initializing stream listener")
	router := gin.Default()
	router.GET("/status", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "listener has started",
		})
	})

	stream.InitiateEventListener()
	router.Run(":8080")
}
