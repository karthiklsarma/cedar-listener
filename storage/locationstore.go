package storage

import (
	"fmt"
	"os"

	"github.com/karthiklsarma/cedar-logging/logging"
)

const (
	COSMOSDB_CASSANDRA_CONTACT_POINT = ""
	COSMOSDB_CASSANDRA_PORT          = ""
	COSMOSDB_CASSANDRA_USER          = ""
	COSMOSDB_CASSANDRA_PASSWORD      = ""
)

func Connect() {
	contact_point := os.Getenv(COSMOSDB_CASSANDRA_CONTACT_POINT)
	cassandra_port := os.Getenv(COSMOSDB_CASSANDRA_PORT)
	cassandra_user := os.Getenv(COSMOSDB_CASSANDRA_USER)
	cassandra_password := os.Getenv(COSMOSDB_CASSANDRA_PASSWORD)

	logging.Debug(
		fmt.Sprintf(
			"Connecting to Cosmos DB with contact point: [%v], port [%v], user: [%v], password: [%v]", contact_point, cassandra_port, cassandra_user, cassandra_password))
}
