package tests

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/karthiklsarma/cedar-listener/stream"
	"github.com/karthiklsarma/cedar-logging/logging"
	"github.com/karthiklsarma/cedar-schema/gen"
	"github.com/karthiklsarma/cedar-storage/storage"
	"google.golang.org/protobuf/proto"
)

type TestStorageSink struct {
	contact_point      string
	cassandra_port     string
	cassandra_user     string
	cassandra_password string
	cosmos_session     *gocql.Session
}

func (testSink *TestStorageSink) Connect() error {
	testSink.contact_point = test_contact_point
	testSink.cassandra_port = test_cassandra_port
	testSink.cassandra_user = test_cassandra_user
	testSink.cassandra_password = test_cassandra_password

	logging.Debug(
		fmt.Sprintf(
			"Connecting to Cosmos DB with contact point: [%v], port [%v], user: [%v]", testSink.contact_point, testSink.cassandra_port, testSink.cassandra_user))

	if testSink.cosmos_session == nil || testSink.cosmos_session.Closed() {
		var err error
		testSink.cosmos_session, err = getSession(testSink.contact_point, testSink.cassandra_port, testSink.cassandra_user, testSink.cassandra_password)
		if err != nil {
			logging.Fatal(fmt.Sprintf("error fetching cosmos session: %v", err))
			return err
		}
	}

	return nil
}

func (testSink *TestStorageSink) TestConnect(contact_point, cassandra_port, cassandra_user, cassandra_password string) error {
	return errors.New("Not implemented")
}

func (testSink *TestStorageSink) InsertLocation(location *gen.Location) (bool, error) {
	if testSink.cosmos_session == nil {
		logging.Error("Please connect before inserting location.")
		return false, fmt.Errorf("Please connect before inserting location.")
	}
	err := testSink.cosmos_session.Query(storage.INSERT_LOCATION_QUERY).Bind(location.Id, location.Lat, location.Lng, location.Timestamp, location.Device).Exec()
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to insert location into location table: %v", err))
		return false, err
	}

	logging.Info("successfully inserted location into location table.")
	return true, nil
}

func (testSink *TestStorageSink) InsertUser(user *gen.User) (bool, error) {
	if testSink.cosmos_session == nil {
		logging.Error("Please connect before inserting user.")
		return false, fmt.Errorf("Please connect before inserting user.")
	}

	err := testSink.cosmos_session.Query(storage.INSERT_USER_QUERY).Bind(
		uuid.New().String(), time.Now().UTC().Unix(), user.Username, user.Firstname, user.Lastname, user.Password, user.Email, user.Phone).Exec()
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to insert user %v into user table: %v", user.Username, err))
		return false, err
	}

	logging.Info(fmt.Sprintf("Successfully inserted user %v into user table.", user.Username))
	return true, nil
}

func (testSink TestStorageSink) Authenticate(email, password string) (bool, error) {
	if testSink.cosmos_session == nil {
		logging.Error("Please connect before inserting location.")
		return false, fmt.Errorf("Please connect before inserting location.")
	}

	var dbpassword string
	err := testSink.cosmos_session.Query(storage.AUTHENTICATION_QUERY, email).Consistency(gocql.One).Scan(&dbpassword)
	if err != nil {
		logging.Error(fmt.Sprintf("Authentication failed: %v", err))
		return false, err
	}

	return password == dbpassword, nil
}

func getSession(contactPoint, cassandraPort, user, password string) (*gocql.Session, error) {
	clusterConfig := gocql.NewCluster(contactPoint)
	port, err := strconv.Atoi(cassandraPort)
	if err != nil {
		logging.Error(fmt.Sprintf("Couldn't convert cosmos cassandra port to int, err: %v", err))
		return nil, err
	}
	clusterConfig.Port = port
	clusterConfig.ProtoVersion = 4
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{Username: user, Password: password}
	clusterConfig.SslOpts = &gocql.SslOptions{Config: &tls.Config{InsecureSkipVerify: true}}
	clusterConfig.ConnectTimeout = 10 * time.Second
	clusterConfig.Timeout = 10 * time.Second
	clusterConfig.DisableInitialHostLookup = true

	session, err := clusterConfig.CreateSession()
	if err != nil {
		logging.Fatal(fmt.Sprintf("Failed to connect to Azure Cosmos DB, err : %v", err))
		return nil, err
	}

	return session, nil
}

func ListenerTest(t *testing.T) {
	listener := &stream.EventhubListener{}
	storagesink := &TestStorageSink{}
	listener.SetStreamConnectionString(test_stream_connection_string)
	listener.SetLocationSink(storagesink)
	listener.InitiateEventListener()
	hub := listener.GetHub()
	location := &gen.Location{Id: "1", Lat: 1.0, Lng: 1.0, Timestamp: uint64(time.Now().Unix()), Device: "test"}
	location_bytes, _ := proto.Marshal(location)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	hub.Send(ctx, eventhub.NewEvent(location_bytes))
	iter := storagesink.cosmos_session.Query("select * from cedarcosmoskeyspace.cedarlocation").Iter()
	iter.Close()
	if iter.NumRows() == 0 {
		t.Fail()
	}
}
