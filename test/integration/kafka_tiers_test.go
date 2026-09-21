package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	segmentkafka "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/auth"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// ---------------------------------------------------------------------------
// Offline half: runs on every PR, no broker needed.
// ---------------------------------------------------------------------------

// Authenticated clusters (SASL/TLS) must be expressible on both the source
// and the transactional sink without connecting and without leaking
// credentials into Describe output.
func TestTier_KafkaAuthTLS(t *testing.T) {
	sasl := auth.SASLConfig{Mechanism: auth.SASLScramSHA256, Username: "svc", Password: "secret"}
	src := source.NewKafkaSource(
		source.KafkaBrokers("broker.hosted.example:9092"),
		source.KafkaTopic("orders"), source.KafkaGroupID("g"),
		source.KafkaSASL(sasl), source.KafkaTLS(auth.TLSConfig{}),
	)
	if src == nil {
		t.Fatal("NewKafkaSource returned nil")
	}
	tx := sink.NewTxnKafkaSink(
		sink.TxnKafkaBrokers("broker.hosted.example:9092"),
		sink.TxnKafkaTopic("out"), sink.TxnKafkaTransactionalID("id"),
		sink.TxnKafkaSASL(sasl), sink.TxnKafkaTLS(auth.TLSConfig{}),
	)
	for k, v := range tx.Describe().Props {
		if v == "secret" || v == "svc" {
			t.Errorf("credential leaked into Describe prop %q", k)
		}
	}
	if got := tx.Describe().Props["sasl"]; got != "SCRAM-SHA-256" {
		t.Errorf("sasl prop = %q, want SCRAM-SHA-256", got)
	}
}

// The topic-aware checkpoint envelope must survive a save/restore cycle
// without a broker, reject ambiguous legacy restores, and keep an empty
// checkpoint restorable (fresh job start).
func TestTier_KafkaPositionEnvelope(t *testing.T) {
	fresh := source.NewKafkaSource(
		source.KafkaBrokers("localhost:9092"),
		source.KafkaTopics("orders", "payments"), source.KafkaGroupID("g"),
	)
	empty, err := fresh.CheckpointOffset()
	if err != nil {
		t.Fatalf("CheckpointOffset on fresh source: %v", err)
	}
	if err := fresh.RestoreOffset(empty); err != nil {
		t.Fatalf("RestoreOffset of own checkpoint: %v", err)
	}
	// Ambiguous legacy single-partition restore against two topics must fail.
	if err := fresh.RestoreOffset([]byte(`{"0":12}`)); err == nil {
		t.Fatal("ambiguous multi-topic legacy restore unexpectedly succeeded")
	}
	// Single-topic legacy restores stay readable.
	single := source.NewKafkaSource(
		source.KafkaBrokers("localhost:9092"),
		source.KafkaTopic("orders"), source.KafkaGroupID("g"),
	)
	if err := single.RestoreOffset([]byte(`{"0":12}`)); err != nil {
		t.Fatalf("single-topic legacy restore: %v", err)
	}
}

// Losing the broker must surface as a prompt error, never a hang: the
// source Run and the transaction-marker probe both hit a closed port.
func TestTier_KafkaBrokerLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	src := source.NewKafkaSource(
		source.KafkaBrokers("127.0.0.1:1"),
		source.KafkaTopic("orders"), source.KafkaGroupID("g-loss"),
		source.KafkaStartFrom(source.OffsetEarliest),
	)
	out := make(chan types.Record, 1)
	runErr := make(chan error, 1)
	runCtx, stop := context.WithTimeout(ctx, 4*time.Second)
	defer stop()
	go func() { runErr <- src.Run(runCtx, out) }()
	select {
	case err := <-runErr:
		if err == nil {
			t.Error("source against a dead broker returned nil error")
		}
	case <-time.After(8 * time.Second):
		t.Error("source against a dead broker hung instead of failing")
	}

	probe := sink.NewTxnKafkaSink(
		sink.TxnKafkaBrokers("127.0.0.1:1"),
		sink.TxnKafkaTopic("out"), sink.TxnKafkaTransactionalID("loss-probe"),
	)
	probeCtx, probeStop := context.WithTimeout(ctx, 10*time.Second)
	defer probeStop()
	if _, err := probe.WasCommitted(probeCtx, "absent"); err == nil {
		t.Error("WasCommitted against a dead broker returned nil error")
	}
}

// ---------------------------------------------------------------------------
// Live half: needs KAFKA_BROKERS (merge/nightly with a Kafka service).
// ---------------------------------------------------------------------------

func liveBrokers(t *testing.T) []string {
	t.Helper()
	text := os.Getenv("KAFKA_BROKERS")
	if text == "" {
		t.Skip("KAFKA_BROKERS is not set")
	}
	return strings.Split(text, ",")
}

func liveTopic(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func createLiveTopics(t *testing.T, brokers []string, cfgs ...segmentkafka.TopicConfig) {
	t.Helper()
	conn, err := segmentkafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial Kafka: %v", err)
	}
	defer conn.Close()
	topics := make([]string, len(cfgs))
	for i, c := range cfgs {
		topics[i] = c.Topic
	}
	if err := conn.CreateTopics(cfgs...); err != nil {
		t.Fatalf("create topics: %v", err)
	}
	t.Cleanup(func() {
		if cleanup, err := segmentkafka.Dial("tcp", brokers[0]); err == nil {
			_ = cleanup.DeleteTopics(topics...)
			_ = cleanup.Close()
		}
	})
	for _, c := range cfgs {
		waitLiveLeader(t, brokers[0], c.Topic)
	}
}

func waitLiveLeader(t *testing.T, broker, topic string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		conn, err := segmentkafka.DialLeader(ctx, "tcp", broker, topic, 0)
		cancel()
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("topic %s leader did not become ready", topic)
}

func produceLive(t *testing.T, brokers []string, topic string, values ...string) {
	t.Helper()
	w := &segmentkafka.Writer{
		Addr: segmentkafka.TCP(brokers...), Topic: topic,
		RequiredAcks: segmentkafka.RequireAll,
	}
	defer w.Close()
	msgs := make([]segmentkafka.Message, len(values))
	for i, v := range values {
		msgs[i] = segmentkafka.Message{Value: []byte(v)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// A topic created moments ago (createLiveTopics already confirmed its
	// leader is up) can still make the Writer's own metadata lookup return
	// "unknown topic or partition" until that propagates; kafka-go documents
	// this class of error as retriable. Nothing is produced when it happens
	// here (the error comes from the pre-send metadata call), so a plain
	// retry of the whole write is safe.
	for {
		err := w.WriteMessages(ctx, msgs...)
		if err == nil {
			return
		}
		var kerr segmentkafka.Error
		if !errors.As(err, &kerr) || !kerr.Temporary() {
			t.Fatalf("produce %s: %v", topic, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("produce %s: %v", topic, err)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func consumeLive(t *testing.T, src *source.KafkaSource, count int) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	out := make(chan types.Record)
	errCh := make(chan error, 1)
	go func() { errCh <- src.Run(ctx, out) }()
	values := make([]string, 0, count)
	for len(values) < count {
		select {
		case r := <-out:
			values = append(values, string(r.Value))
		case err := <-errCh:
			t.Fatalf("source stopped early: %v", err)
		case <-ctx.Done():
			t.Fatalf("timed out after %d/%d records", len(values), count)
		}
	}
	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("stop source: %v", err)
	}
	return values
}

// Two topics sharing partition 0 survive one checkpoint: a fresh consumer
// group restored from it reads only what arrived after the checkpoint.
func TestLive_KafkaMultiTopicRecovery(t *testing.T) {
	brokers := liveBrokers(t)
	suffix := time.Now().UnixNano()
	topicA, topicB := liveTopic("weibo-tier-a"), liveTopic("weibo-tier-b")
	group1 := fmt.Sprintf("weibo-tier-capture-%d", suffix)
	group2 := fmt.Sprintf("weibo-tier-restore-%d", suffix)
	createLiveTopics(t, brokers,
		segmentkafka.TopicConfig{Topic: topicA, NumPartitions: 1, ReplicationFactor: 1},
		segmentkafka.TopicConfig{Topic: topicB, NumPartitions: 1, ReplicationFactor: 1},
	)

	produceLive(t, brokers, topicA, "a0", "a1")
	produceLive(t, brokers, topicB, "b0", "b1")

	first := source.NewKafkaSource(
		source.KafkaBrokers(brokers...), source.KafkaTopics(topicA, topicB),
		source.KafkaGroupID(group1), source.KafkaStartFrom(source.OffsetEarliest),
		source.KafkaExactlyOnce(),
	)
	got1 := consumeLive(t, first, 4)
	sort.Strings(got1)
	if strings.Join(got1, ",") != "a0,a1,b0,b1" {
		t.Fatalf("initial records = %v", got1)
	}
	checkpointData, err := first.CheckpointOffset()
	if err != nil {
		t.Fatalf("CheckpointOffset: %v", err)
	}

	produceLive(t, brokers, topicA, "a2")
	produceLive(t, brokers, topicB, "b2")

	second := source.NewKafkaSource(
		source.KafkaBrokers(brokers...), source.KafkaTopics(topicA, topicB),
		source.KafkaGroupID(group2), source.KafkaStartFrom(source.OffsetEarliest),
		source.KafkaExactlyOnce(),
	)
	if err := second.RestoreOffset(checkpointData); err != nil {
		t.Fatalf("RestoreOffset: %v", err)
	}
	got2 := consumeLive(t, second, 2)
	sort.Strings(got2)
	if strings.Join(got2, ",") != "a2,b2" {
		t.Fatalf("records after restore = %v; checkpoint positions were not applied", got2)
	}
}

// Committed markers probe true, aborted and absent probe false — the
// transaction-recovery contract the crash matrix depends on.
func TestLive_KafkaTransactions(t *testing.T) {
	brokers := liveBrokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	suffix := time.Now().UnixNano()
	markerTopic := fmt.Sprintf("weibo-tier-marker-%d", suffix)
	outputTopic := fmt.Sprintf("weibo-tier-txnout-%d", suffix)
	createLiveTopics(t, brokers,
		segmentkafka.TopicConfig{Topic: markerTopic, NumPartitions: 2, ReplicationFactor: 1},
		segmentkafka.TopicConfig{Topic: outputTopic, NumPartitions: 1, ReplicationFactor: 1},
	)

	txnID := fmt.Sprintf("weibo-tier-txn-%d", suffix)
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.TransactionalID(txnID))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
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

	probe := sink.NewTxnKafkaSink(
		sink.TxnKafkaBrokers(brokers...), sink.TxnKafkaTopic(outputTopic),
		sink.TxnKafkaTransactionalID(txnID), sink.TxnKafkaMarkerTopic(markerTopic),
	)
	for id, want := range map[string]bool{"committed": true, "aborted": false, "absent": false} {
		got, err := probe.WasCommitted(ctx, id)
		if err != nil {
			t.Fatalf("probe %s: %v", id, err)
		}
		if got != want {
			t.Fatalf("probe %s: got %v, want %v", id, got, want)
		}
	}
}

// Growing a topic 1 → 3 partitions mid-stream must not lose records: the
// source picks up the new partitions and consumes everything.
func TestLive_KafkaPartitionExpansion(t *testing.T) {
	brokers := liveBrokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	topic := liveTopic("weibo-tier-expand")
	group := fmt.Sprintf("weibo-tier-expand-g-%d", time.Now().UnixNano())
	createLiveTopics(t, brokers,
		segmentkafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
	)
	produceLive(t, brokers, topic, "m0", "m1")

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if resps, err := kadm.NewClient(cl).CreatePartitions(ctx, 2, topic); err != nil {
		t.Fatalf("expand partitions: %v", err)
	} else if err := resps.Error(); err != nil {
		t.Fatalf("expand partitions: %v", err)
	}

	produceLive(t, brokers, topic, "m2", "m3", "m4", "m5")
	src := source.NewKafkaSource(
		source.KafkaBrokers(brokers...), source.KafkaTopic(topic),
		source.KafkaGroupID(group), source.KafkaStartFrom(source.OffsetEarliest),
	)
	got := consumeLive(t, src, 6)
	sort.Strings(got)
	if strings.Join(got, ",") != "m0,m1,m2,m3,m4,m5" {
		t.Fatalf("records after expansion = %v", got)
	}
}

// Two readers in one consumer group split the partitions: together they
// receive every record exactly once (rebalance tier).
func TestLive_KafkaRebalance(t *testing.T) {
	brokers := liveBrokers(t)
	topic := liveTopic("weibo-tier-rebalance")
	group := fmt.Sprintf("weibo-tier-rebal-g-%d", time.Now().UnixNano())
	createLiveTopics(t, brokers,
		segmentkafka.TopicConfig{Topic: topic, NumPartitions: 2, ReplicationFactor: 1},
	)
	want := []string{"r0", "r1", "r2", "r3", "r4", "r5"}
	produceLive(t, brokers, topic, want...)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	collect := func(src *source.KafkaSource, ch chan<- string) {
		defer close(ch)
		out := make(chan types.Record)
		errCh := make(chan error, 1)
		go func() { errCh <- src.Run(ctx, out) }()
		for {
			select {
			case r, ok := <-out:
				if !ok {
					return
				}
				select {
				case ch <- string(r.Value):
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			case <-errCh:
				return
			}
		}
	}
	mkSrc := func() *source.KafkaSource {
		return source.NewKafkaSource(
			source.KafkaBrokers(brokers...), source.KafkaTopic(topic),
			source.KafkaGroupID(group), source.KafkaStartFrom(source.OffsetEarliest),
		)
	}
	ch1, ch2 := make(chan string, 16), make(chan string, 16)
	go collect(mkSrc(), ch1)
	go collect(mkSrc(), ch2)

	seen := map[string]int{}
	deadline := time.Now().Add(55 * time.Second)
	for len(seen) < len(want) && time.Now().Before(deadline) {
		select {
		case v, ok := <-ch1:
			if ok {
				seen[v]++
			}
		case v, ok := <-ch2:
			if ok {
				seen[v]++
			}
		case <-time.After(500 * time.Millisecond):
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("rebalanced readers saw %d/%d distinct records: %v", len(seen), len(want), seen)
	}
	for _, w := range want {
		if seen[w] != 1 {
			t.Errorf("record %q delivered %d times, want exactly once", w, seen[w])
		}
	}
}
