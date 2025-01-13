package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/go-estar/logger"
	"github.com/go-estar/mqtt"
	"os"
	"strings"
)

func defaultHandler(topic string, msg []byte) error {
	fmt.Println("defaultHandler", topic, string(msg))
	return nil
}

func main() {
	client := mqtt.New(&mqtt.Config{
		Addr:                  "127.0.0.1:1883",
		UserName:              "server",
		Password:              "s28wfCMn##Y!znu6",
		ClientId:              "server1",
		CleanSession:          false,
		ClientLogger:          logger.NewZap("mqtt-client", "info"),
		PubLogger:             logger.NewZap("mqtt-pub", "info"),
		SubLogger:             logger.NewZap("mqtt-sub", "info"),
		DefaultPublishHandler: defaultHandler,
	})

	client.Subscribe("connect/+", defaultHandler, mqtt.SubWithLogLevel("info"))
	client.Subscribe("disconnect/+", defaultHandler, mqtt.SubWithLogLevel("info"))

	if err := client.Subscribe("ping/+", func(topic string, msg []byte) error {
		fmt.Println("sub", topic, string(msg))
		//return errors.New("fucked")
		arr := strings.Split(topic, "/")
		if len(arr) != 2 {
			return errors.New("message format mismatch")
		}
		if err := client.Publish("pong/"+arr[1], "res"); err != nil {
			return err
		}
		return nil
	}, mqtt.SubWithLogLevel("error")); err != nil {
		fmt.Println(err)
		return
	}

	if err := client.Publish("order/342", "test"); err != nil {
		fmt.Println(err)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		arr := strings.Split(scanner.Text(), "@")
		if len(arr) != 2 {
			fmt.Println("message format mismatch")
			continue
		}
		if err := client.Publish(arr[0], arr[1]); err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("pub", arr[0], arr[1])
	}
}
