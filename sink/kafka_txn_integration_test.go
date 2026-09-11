package sink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	segmentkafka "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTxnKafkaMarkerProbeIntegration(t *testing.T) {
	brokersText := os.Getenv("KAFKA_BROKERS")
	if brokersText == "" {
		t.Skip("KAFKA_BROKERS is not set")
	}
	brokers := strings.Split(brokersText, ",")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	suffix := time.Now().UnixNano()
	markerTopic := fmt.Sprintf("weibo-marker-probe-%d", suffix)
	outputTopic := fmt.Sprintf("weibo-marker-output-%d", suffix)
	conn, err := segmentkafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.CreateTopics(
		segmentkafka.TopicConfig{Topic: markerTopic, NumPartitions: 2, ReplicationFactor: 1},
		segmentkafka.TopicConfig{Topic: outputTopic, NumPartitions: 1, ReplicationFactor: 1},
	); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	conn.Close()
	defer func() {
		if cleanup, err := segmentkafka.Dial("tcp", brokers[0]); err == nil {
			_ = cleanup.DeleteTopics(markerTopic, outputTopic)
			_ = cleanup.Close()
		}
	}()

	txnID := fmt.Sprintf("weibo-marker-txn-%d", suffix)
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.TransactionalID(txnID))
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	producer.ProduceSync(ctx, &kgo.Record{Topic: markerTopic, Key: []byte(txnID), Value: []byte("committed")})
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	producer.ProduceSync(ctx, &kgo.Record{Topic: markerTopic, Key: []byte(txnID), Value: []byte("aborted")})
	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	producer.Close()

	probe := NewTxnKafkaSink(TxnKafkaBrokers(brokers...), TxnKafkaTopic(outputTopic), TxnKafkaTransactionalID(txnID), TxnKafkaMarkerTopic(markerTopic))
	for id, want := range map[string]bool{"committed": true, "aborted": false, "absent": false} {
		got, err := probe.WasCommitted(ctx, id)
		if err != nil {
			t.Fatalf("probe %s: %v", id, err)
		}
		if got != want {
			t.Fatalf("probe %s: got %v want %v", id, got, want)
		}
	}
}
