package stream

import (
	"context"
	"fmt"
	"os"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/karthiklsarma/cedar-logging/logging"
	"github.com/karthiklsarma/cedar-schema/gen"
	"google.golang.org/protobuf/proto"
)

var hub *eventhub.Hub
var stream_connection_string string

func getConnectionString() string {
	return os.Getenv(STREAM_CONN_ENV)
}

func InitiateEventListener() {
	if len(stream_connection_string) == 0 {
		logging.Info("stream connection string empty. Fetching...")
		stream_connection_string = getConnectionString()
	}

	var err error
	if hub == nil {
		logging.Info("hub empty. Initializing...")
		if hub, err = eventhub.NewHubFromConnectionString(stream_connection_string); err != nil {
			logging.Error(fmt.Sprintf("error initiating eventhub. error: %v", err))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runtimeInfo, err := hub.GetRuntimeInformation(ctx)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to obtain runtime info for event hub: %v", err))
	}

	handler := func(c context.Context, event *eventhub.Event) error {
		location := &gen.Location{}
		if err := proto.Unmarshal(event.Data, location); err != nil {
			logging.Error("Failed to unmarshal location")
			return err
		}
		logging.Info(fmt.Sprintf("Received event: %v", location))
		return nil
	}

	for _, partitionID := range runtimeInfo.PartitionIDs {
		if _, err := hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset()); err != nil {
			logging.Error(fmt.Sprintf("failed to receive event from eventhub: %v", err))
		}
	}
}
