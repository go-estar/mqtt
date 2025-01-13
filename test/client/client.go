package main

import (
	"bufio"
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
	clientId := "342"
	client := mqtt.New(&mqtt.Config{
		Addr:                  "127.0.0.1:1883",
		UserName:              "pos",
		Password:              "M9o!ejN@1fm#oH#M",
		ClientId:              clientId,
		CleanSession:          false,
		ClientLogger:          logger.NewZap("mqtt-client", "info"),
		PubLogger:             logger.NewZap("mqtt-pub", "info"),
		SubLogger:             logger.NewZap("mqtt-sub", "info"),
		DefaultPublishHandler: defaultHandler,
	})

	client.Subscribe("pong/"+clientId, defaultHandler, mqtt.SubWithLogLevel("info"))
	client.Subscribe("order/"+clientId, defaultHandler, mqtt.SubWithLogLevel("info"))

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if scanner.Text() == "disconnect" {
			client.Disconnect()
			continue
		}
		arr := strings.Split(scanner.Text(), "@")
		if len(arr) != 2 {
			fmt.Println("message format mismatch")
			continue
		}
		if err := client.Publish(arr[0]+"/"+clientId, arr[1]); err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("pub", arr[0]+"/"+clientId, arr[1])
	}

}
