package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := "redis-service:6379"
	broadcastChannel := "broadcast-channel"
	lockKey := "lock-key"
	lockTTL := 10 * time.Second
	meanSleepDuration := 5

	podUid := getEnvVariables()
	redisClient := newRedisClient(redisAddr)
	defer redisClient.Close()
	ctx := context.Background()

	ch := subscribeToChannel(ctx, redisClient, broadcastChannel)
	go handleMessages(ch, podUid)
	runMainLoop(ctx, redisClient, podUid, broadcastChannel, lockKey, lockTTL, meanSleepDuration)
}

func getEnvVariables() string {
	log.Printf("Getting environment variables")
	podUid, podUidExists := os.LookupEnv("POD_UID")
	if !podUidExists {
		log.Fatalf("Missing POD_UID env variable")
	}

	return podUid
}

func newRedisClient(redisAddr string) *redis.Client {
	log.Printf("Creating redis client")
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})
	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	return redisClient
}

type messageData struct {
	PodUid string    `json:"podUid"`
	Time   time.Time `json:"time"`
}

func subscribeToChannel(ctx context.Context, redisClient *redis.Client, broadcastChannel string) <-chan *redis.Message {
	log.Printf("Subscribing to channel '%s'", broadcastChannel)
	pubsub := redisClient.Subscribe(ctx, broadcastChannel)
	if _, err := pubsub.Receive(ctx); err != nil {
		log.Fatalf("Failed to subscribe to channel '%s': %v", broadcastChannel, err)
	}
	ch := pubsub.Channel()

	return ch
}

func handleMessages(ch <-chan *redis.Message, podUid string) {
	log.Printf("Handling messages")
	for msg := range ch {
		var data messageData
		err := json.Unmarshal([]byte(msg.Payload), &data)
		if err != nil {
			log.Fatalf("Failed to handle message. Failed to unmarshalling message: %v", err)
		}
		if data.PodUid != podUid {
			log.Printf("Received signal: %v", data)
		}
	}
}

func runMainLoop(ctx context.Context, redisClient *redis.Client, podUid string, broadcastChannel string, lockKey string, lockTTL time.Duration, meanSleepDuration int) {
	log.Printf("Starting main loop")
	for {
		sleepTime := time.Duration(rand.Intn(2*meanSleepDuration)) * time.Second
		log.Printf("Waiting for %s seconds ...", sleepTime)
		time.Sleep(sleepTime)
		attemptLockAndBroadcast(ctx, redisClient, podUid, broadcastChannel, lockKey, lockTTL)
	}
}

func attemptLockAndBroadcast(ctx context.Context, redisClient *redis.Client, podUid string, broadcastChannel string, lockKey string, lockTTL time.Duration) {
	log.Printf("Attempting to acquire lock and broadcast")
	acquired, err := redisClient.SetNX(ctx, lockKey, podUid, lockTTL).Result()
	if err != nil {
		log.Fatalf("Error in acquiring lock: %v", err)
		return
	}
	if !acquired {
		log.Printf("Failed to acquire lock")
		return
	}

	broadcastSignal(ctx, redisClient, podUid, broadcastChannel)
	releaseLock(ctx, redisClient, lockKey)
}

func broadcastSignal(ctx context.Context, redisClient *redis.Client, podUid string, broadcastChannel string) {
	log.Printf("Broadcasting signal")
	data := messageData{
		PodUid: podUid,
		Time:   time.Now(),
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("Failed to broadcast signal. Failed to serialize message data to JSON: %v", err)
	}

	err = redisClient.Publish(ctx, broadcastChannel, string(jsonData)).Err()
	if err != nil {
		log.Fatalf("Failed to broadcast signal: %v", err)
	}
	time.Sleep(time.Second * 2) // Hold lock for a short time
}

func releaseLock(ctx context.Context, redisClient *redis.Client, lockKey string) {
	log.Printf("Releasing lock")
	unlock, err := redisClient.Del(ctx, lockKey).Result()
	if err != nil {
		log.Fatalf("Failed to release lock: %v. Unlock value: %d", err, unlock)
	} else {
		log.Printf("Released lock")
	}
}
