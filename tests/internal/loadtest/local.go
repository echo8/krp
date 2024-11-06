package loadtest

import (
	"context"
	"fmt"
	"time"

	"github.com/echo8/krp/tests/internal/testutil"
	"github.com/testcontainers/testcontainers-go/network"
)

func RunLocal() {
	ctx := context.Background()
	network, _ := network.New(ctx)
	broker, _ := testutil.NewKafkaContainer(ctx, "broker", "9094", network.Name)
	defer broker.Terminate(ctx)
	krp, err := testutil.NewKrpContainer(ctx, network.Name, `addr: ":8080"
endpoints:
  first:
    routes:
      - topic: topic1
        producer: confluent
producers:
  confluent:
    type: kafka
    clientConfig:
      bootstrap.servers: broker:9092`)
	if err != nil {
		panic(err)
	}
	defer krp.Terminate(ctx)

	mp, _ := krp.MappedPort(ctx, "8080/tcp")
	res := GenerateLoad(&LoadConfig{
		Rps:      10,
		Duration: 10 * time.Second,
		URL:      fmt.Sprintf("http://localhost:%s/first", mp.Port()),
	})
	fmt.Println(res)
}
