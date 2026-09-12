package source

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// TestKafkaMultiTopicCheckpointRestore is run by kafka-e2e. It proves that two
// topics sharing partition zero survive one checkpoint and that a fresh
// consumer group is reset to those checkpoint positions before reading.
func TestKafkaMultiTopicCheckpointRestore(t *testing.T) {
	brokersText := os.Getenv("KAFKA_BROKERS")
	if brokersText == "" {
		t.Skip("KAFKA_BROKERS is not set")
	}
	brokers := strings.Split(brokersText, ",")
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topicA, topicB := "weibo-restore-a-"+suffix, "weibo-restore-b-"+suffix
	group1, group2 := "weibo-capture-"+suffix, "weibo-restore-"+suffix
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("connect Kafka: %v", err)
	}
	if err := conn.CreateTopics(
		kafka.TopicConfig{Topic: topicA, NumPartitions: 1, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: topicB, NumPartitions: 1, ReplicationFactor: 1},
	); err != nil {
		conn.Close()
		t.Fatalf("create topics: %v", err)
	}
	conn.Close()
	t.Cleanup(func() {
		cleanup, dialErr := kafka.Dial("tcp", brokers[0])
		if dialErr == nil {
			_ = cleanup.DeleteTopics(topicA, topicB)
			_ = cleanup.Close()
		}
	})
	waitForTopicLeader(t, brokers[0], topicA)
	waitForTopicLeader(t, brokers[0], topicB)

	write := func(topic string, values ...string) {
		t.Helper()
		// Use a fresh transport so negative metadata cached while the topic was
		// being created cannot leak into this producer.
		transport := &kafka.Transport{MetadataTTL: 100 * time.Millisecond}
		defer transport.CloseIdleConnections()
		writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, RequiredAcks: kafka.RequireAll, Transport: transport}
		defer writer.Close()
		messages := make([]kafka.Message, len(values))
		for i, value := range values {
			messages[i] = kafka.Message{Value: []byte(value)}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := writer.WriteMessages(ctx, messages...); err != nil {
			t.Fatalf("write %s: %v", topic, err)
		}
	}

	write(topicA, "a0", "a1")
	write(topicB, "b0", "b1")

	first := NewKafkaSource(
		KafkaBrokers(brokers...), KafkaTopics(topicA, topicB), KafkaGroupID(group1),
		KafkaStartFrom(OffsetEarliest), KafkaExactlyOnce(),
	)
	got1 := consumeN(t, first, 4)
	sort.Strings(got1)
	if strings.Join(got1, ",") != "a0,a1,b0,b1" {
		t.Fatalf("initial records = %v", got1)
	}
	checkpointData, err := first.CheckpointOffset()
	if err != nil {
		t.Fatal(err)
	}
	positions := decodePositionMap(t, checkpointData, "")
	if positions[topicA+"/0"] != 2 || positions[topicB+"/0"] != 2 || len(positions) != 2 {
		t.Fatalf("checkpoint positions = %v", positions)
	}

	write(topicA, "a2")
	write(topicB, "b2")

	second := NewKafkaSource(
		KafkaBrokers(brokers...), KafkaTopics(topicA, topicB), KafkaGroupID(group2),
		KafkaStartFrom(OffsetEarliest), KafkaExactlyOnce(),
	)
	if err := second.RestoreOffset(checkpointData); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got2 := consumeN(t, second, 2)
	sort.Strings(got2)
	if strings.Join(got2, ",") != "a2,b2" {
		t.Fatalf("records after restore = %v; checkpoint positions were not applied", got2)
	}
}

func waitForTopicLeader(t *testing.T, broker, topic string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, 0)
		cancel()
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("topic %s leader did not become ready: %v", topic, lastErr)
}

func consumeN(t *testing.T, src *KafkaSource, count int) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	out := make(chan types.Record)
	errCh := make(chan error, 1)
	go func() { errCh <- src.Run(ctx, out) }()
	values := make([]string, 0, count)
	for len(values) < count {
		select {
		case record := <-out:
			values = append(values, string(record.Value))
		case err := <-errCh:
			cancel()
			t.Fatalf("Kafka source stopped early: %v", err)
		case <-ctx.Done():
			cancel()
			t.Fatalf("timed out after %d/%d records", len(values), count)
		}
	}
	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("stop Kafka source: %v", err)
	}
	return values
}
