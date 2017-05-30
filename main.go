package main

import (
	"fmt"
	"github.com/satori/go.uuid"
	"gopkg.in/redis.v5"
	"math/rand"
	"os"
	"time"
)

var client *redis.Client

const (
	queueKey     = "queue"
	generatorKey = "generator"
	errorsKey    = "errors"
	lockTime     = 10 * time.Second
)

var pid = uuid.NewV4().String()

func createMessageGenerator() func() int {
	i := 0

	return func() int {
		i++
		return i
	}
}

func sendMessage(message int) error {
	err := client.Watch(func(tx *redis.Tx) error {
		currentPid, err := tx.Get(generatorKey).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		if currentPid != pid {
			return redis.TxFailedErr
		}

		_, err = tx.RPush(queueKey, message)
		return err
	}, generatorKey)

	return err
}

func generator() error {
	fmt.Println("start generator")

	getMessage := createMessageGenerator()
	tick := time.Tick(1 * time.Nanosecond)
	reclaim := time.Tick(500 * time.Millisecond)
	for {
		select {
		case <-tick:
			err := sendMessage(getMessage())

			if err == redis.TxFailedErr {
				fmt.Println("Generator is someone else")
				return nil
			}

			if err != nil {
				return err
			}
		case <-reclaim:
			err := client.Expire(generatorKey, lockTime).Err()

			if err != nil {
				return err
			}
		}
	}
}

func eventHandler(msg string) {
	time.Sleep(1000 * time.Millisecond)

	if rand.Intn(100) > 84 {
		fmt.Println("Got errror", msg)
		writeError(msg)
	}
}

func writeError(msg string) {
	err := client.RPush(errorsKey, msg).Err()
	if err != nil {
		fmt.Println("Error write error", err)
	}
}

func reader() error {
	checkGenerator := time.Tick(500 * time.Millisecond)
	for {
		select {
		case <-checkGenerator:
			free, err := client.SetNX(generatorKey, pid, lockTime).Result()
			if err != nil {
				return err
			}

			if free {
				return nil
			}
		default:
			result, err := client.BLPop(1000*time.Millisecond, queueKey).Result()
			if err == redis.Nil {
				continue
			}

			if err != nil {
				return err
			}

			go eventHandler(result[1])
		}
	}
}

func printErrors() {
	errors, err := client.LRange(errorsKey, 0, -1).Result()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err := client.Del(errorsKey).Err()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(errors)
}

func main() {
	client = redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "",
		DB:       0,
	})

	if len(os.Args) > 1 && os.Args[1] == "getErrors" {
		printErrors()
		return
	}

	isGenerator := false
	tick := time.Tick(500 * time.Millisecond)
	for {
		select {
		case <-tick:
			if isGenerator {
				err := generator()
				if err != nil {
					fmt.Println("Generator err", err)
				} else {
					isGenerator = false
				}
			} else {
				err := reader()
				if err != nil {
					fmt.Println("Reader err", err)
				} else {
					isGenerator = true
				}
			}
		}
	}
}
