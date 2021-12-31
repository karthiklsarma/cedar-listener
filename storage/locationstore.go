package storage

import (
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/karthiklsarma/cedar-logging/logging"
	"github.com/karthiklsarma/cedar-schema/gen"
)

const (
	COSMOSDB_CONTACT_POINT = "COSMOSDB_CONTACT_POINT"
	COSMOSDB_PORT          = "COSMOSDB_PORT"
	COSMOSDB_USER          = "COSMOSDB_USER"
	COSMOSDB_PASSWORD      = "COSMOSDB_PASSWORD"
)

var (
	contact_point      string
	cassandra_port     string
	cassandra_user     string
	cassandra_password string
)

var cosmos_session *gocql.Session

const INSERT_QUERY = "INSERT INTO cedarcosmoskeyspace.cedarlocation (id, lat, lng, timestamp, device) VALUES (?, ?, ?, ?, ?)"

func Connect() {
	if len(contact_point) == 0 {
		contact_point = os.Getenv(COSMOSDB_CONTACT_POINT)
	}

	if len(cassandra_port) == 0 {
		cassandra_port = os.Getenv(COSMOSDB_PORT)
	}

	if len(cassandra_user) == 0 {
		cassandra_user = os.Getenv(COSMOSDB_USER)
	}

	if len(cassandra_password) == 0 {
		cassandra_password = os.Getenv(COSMOSDB_PASSWORD)
	}

	logging.Debug(
		fmt.Sprintf(
			"Connecting to Cosmos DB with contact point: [%v], port [%v], user: [%v]", contact_point, cassandra_port, cassandra_user))

	if cosmos_session == nil || cosmos_session.Closed() {
		cosmos_session = getSession(contact_point, cassandra_port, cassandra_user, cassandra_password)
	}
}

func InsertLocation(location *gen.Location) error {
	if cosmos_session == nil {
		logging.Fatal("Please connect before inserting location.")
	}
	err := cosmos_session.Query(INSERT_QUERY).Bind(location.Id, location.Lat, location.Lng, location.Timestamp, location.Device).Exec()
	if err != nil {
		logging.Fatal(fmt.Sprintf("Failed to insert location into location table: %v", err))
		return err
	}

	logging.Info("successfully inserted location into location table.")
	return nil
}

func getSession(contactPoint, cassandraPort, user, password string) *gocql.Session {
	clusterConfig := gocql.NewCluster(contactPoint)
	port, err := strconv.Atoi(cassandraPort)
	if err != nil {
		logging.Fatal(fmt.Sprintf("Couldn't convert cosmos cassandra port to int, err: %v", err))
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
	}

	return session
}
