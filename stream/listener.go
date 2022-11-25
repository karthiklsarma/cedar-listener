package stream

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/karthiklsarma/cedar-logging/logging"
	"github.com/karthiklsarma/cedar-schema/gen"
	"github.com/karthiklsarma/cedar-storage/storage"
	"google.golang.org/protobuf/proto"
)

type IStreamListener interface {
	InitiateEventListener()
}

type EventhubListener struct {
	stream_connection_string string
	locationSink             storage.IStorageSink
	hub                      *eventhub.Hub
}

func getConnectionString() string {
	return os.Getenv(STREAM_CONN_ENV)
}

func (listener *EventhubListener) SetStreamConnectionString(connectionStr string) {
	listener.stream_connection_string = connectionStr
}

func (listener *EventhubListener) SetLocationSink(sink storage.IStorageSink) {
	listener.locationSink = sink
}

func (listener *EventhubListener) GetHub() *eventhub.Hub {
	return listener.hub
}

func (listener *EventhubListener) InitiateEventListener() {
	if len(listener.stream_connection_string) == 0 {
		logging.Info("stream connection string empty. Fetching...")
		listener.stream_connection_string = getConnectionString()
	}

	var err error
	if listener.hub, err = eventhub.NewHubFromConnectionString(listener.stream_connection_string); err != nil {
		logging.Error(fmt.Sprintf("error initiating eventhub. error: %v", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runtimeInfo, err := listener.hub.GetRuntimeInformation(ctx)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to obtain runtime info for event hub: %v", err))
	}

	if listener.locationSink == nil {
		listener.locationSink = &storage.CosmosSink{}
	}

	if err := listener.locationSink.Connect(); err != nil {
		logging.Fatal(fmt.Sprintf("Failed to connect to storage sink: %v", err))
	}

	listener.processPartition(listener.hub, runtimeInfo.PartitionIDs...)
}

func (listener *EventhubListener) processPartition(hub *eventhub.Hub, partitionIds ...string) {
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(len(partitionIds))
	for _, partitionId := range partitionIds {
		go func(partition string, wg *sync.WaitGroup) {
			defer wg.Done()
			if _, err := hub.Receive(ctx, partition, listener.processMessage, eventhub.ReceiveWithLatestOffset()); err != nil {
				logging.Fatal(fmt.Sprintf("failed to receive event from eventhub: %v", err))
			}
		}(partitionId, &wg)
	}

	wg.Wait()
}

func (listener *EventhubListener) processMessage(c context.Context, event *eventhub.Event) error {
	location := &gen.Location{}
	if err := proto.Unmarshal(event.Data, location); err != nil {
		logging.Error("Failed to unmarshal location")
		return err
	}

	logging.Info(fmt.Sprintf("Received event: %v", location))
	inserted, err := listener.locationSink.InsertLocation(location)
	if err != nil {
		logging.Error(fmt.Sprintf("Unable to insert location [%v] into location sink, error: %v", location, err))
	}

	if !inserted {
		logging.Error(fmt.Sprintf("Unable to insert location [%v] into location sink. Stack %v", location, debug.Stack()))
		return nil
	}

	logging.Info(fmt.Sprintf("Inserted location %v into location sink", location))
	return nil
}
