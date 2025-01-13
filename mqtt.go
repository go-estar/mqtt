package mqtt

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/avast/retry-go/v4"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	baseError "github.com/go-estar/base-error"
	"github.com/go-estar/config"
	"github.com/go-estar/logger"
	"net/url"
	"reflect"
	"time"
)

type LocalRetry struct {
	DelayTypeFunc retry.DelayTypeFunc
	Attempts      uint
}

func NewLocalRetry(defaultDelay time.Duration, maxDelay time.Duration, attempts uint) *LocalRetry {
	return &LocalRetry{
		Attempts: attempts,
		DelayTypeFunc: func(n uint, err error, config *retry.Config) time.Duration {
			delay := defaultDelay * time.Duration(n+1)
			if delay > maxDelay {
				delay = maxDelay
			}
			return delay
		},
	}
}

func NewDefaultLocalRetry(attempts uint) *LocalRetry {
	return NewLocalRetry(time.Second, time.Second*10, attempts)
}

type Config struct {
	ClientId          string
	Addr              string
	UserName          string
	Password          string
	CleanSession      bool
	DefaultSubHandler Handler
	Debug             bool
	ClientLogger      logger.Logger
	PubLogger         logger.Logger
	SubLogger         logger.Logger
}

type Option func(*Config)

func WithDefaultSubHandler(val Handler) Option {
	return func(opts *Config) {
		opts.DefaultSubHandler = val
	}
}

func NewWithConfig(c *config.Config, opts ...Option) *Client {
	conf := &Config{
		ClientId:     c.GetString("application.name"),
		Addr:         c.GetString("mqtt.addr"),
		UserName:     c.GetString("mqtt.userName"),
		Password:     c.GetString("mqtt.password"),
		CleanSession: c.GetBool("mqtt.cleanSession"),
		Debug:        c.GetBool("mqtt.debug"),
		ClientLogger: logger.NewZapWithConfig(c, "mqtt-client", "info"),
		PubLogger:    logger.NewZapWithConfig(c, "mqtt-pub", "info"),
		SubLogger:    logger.NewZapWithConfig(c, "mqtt-sub", "info"),
	}
	for _, apply := range opts {
		apply(conf)
	}
	return New(conf)
}

func New(c *Config) *Client {
	if c == nil {
		panic("config 必须设置")
	}
	if c.UserName == "" {
		panic("UserName 必须设置")
	}
	if c.Password == "" {
		panic("Password 必须设置")
	}
	if c.ClientId == "" {
		panic("ClientId 必须设置")
	}
	if c.DefaultSubHandler == nil {
		panic("DefaultSubHandler 必须设置")
	}
	if c.PubLogger == nil {
		panic("PubLogger 必须设置")
	}
	if c.SubLogger == nil {
		panic("SubLogger 必须设置")
	}

	if c.ClientLogger != nil {
		mqtt.ERROR = &ErrorLogger{c.ClientLogger}
		mqtt.CRITICAL = &ErrorLogger{c.ClientLogger}
		mqtt.WARN = &InfoLogger{c.ClientLogger}
		if c.Debug {
			mqtt.DEBUG = &InfoLogger{c.ClientLogger}
		}
	}

	opts := mqtt.NewClientOptions().AddBroker("tcp://" + c.Addr)
	opts.SetUsername(c.UserName)
	opts.SetPassword(c.Password)
	opts.SetClientID(c.ClientId)
	opts.SetCleanSession(c.CleanSession)
	//默认为false 收到消息自动ACK， 改为无err时ACK
	opts.SetAutoAckDisabled(true)
	//默认为true 收到消息逐条执行Handler 可能造成消息阻塞
	opts.SetOrderMatters(false)
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		if c.ClientLogger != nil {
			c.ClientLogger.Info("OnConnect")
		}
	})
	opts.SetReconnectingHandler(func(mqtt.Client, *mqtt.ClientOptions) {
		if c.ClientLogger != nil {
			c.ClientLogger.Info("Reconnecting")
		}
	})
	opts.SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
		defer func() {
			if c.ClientLogger != nil {
				c.ClientLogger.Info("ConnectionAttempt")
			}
		}()
		return tlsCfg
	})
	opts.SetConnectionLostHandler(func(mqtt.Client, error) {
		if c.ClientLogger != nil {
			c.ClientLogger.Info("ConnectionLost")
		}
	})

	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		go SubscribeHandler(client, msg, &SubConfig{
			Qos:       1,
			LogLevel:  "info",
			SubLogger: c.SubLogger,
		}, c.DefaultSubHandler)
	})
	//opts.SetWill("disconnect/"+config.ClientId, "", 1, false)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("mqtt connect " + c.Addr)
	return &Client{
		Config: c,
		Client: client,
	}
}

type Handler = func(topic string, msg []byte) error

type Client struct {
	*Config
	mqtt.Client
}

func (c *Client) MessageFormat(data interface{}) ([]byte, error) {
	var body []byte
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Type() == reflect.TypeOf(body) {
		body = data.([]byte)
	} else {
		if reflect.TypeOf(data).Kind() == reflect.String {
			body = []byte(data.(string))
		} else {
			return json.Marshal(data)
		}
	}
	return body, nil
}

type PubConfig struct {
	Qos        byte
	LogLevel   string
	LocalRetry *LocalRetry
}
type PubOption func(*PubConfig)

func PubWithQos(val byte) PubOption {
	return func(c *PubConfig) {
		c.Qos = val
	}
}
func PubWithLogLevel(val string) PubOption {
	return func(c *PubConfig) {
		c.LogLevel = val
	}
}
func PubWithLocalRetry(val *LocalRetry) PubOption {
	return func(c *PubConfig) {
		c.LocalRetry = val
	}
}

func (c *Client) Publish(topic string, data interface{}, opts ...PubOption) (err error) {
	conf := &PubConfig{
		Qos:      1,
		LogLevel: "info",
	}
	for _, apply := range opts {
		apply(conf)
	}

	message, err := c.MessageFormat(data)
	defer func() {
		if err != nil || conf.LogLevel == "info" {
			c.PubLogger.Info(string(message),
				logger.NewField("topic", topic),
				logger.NewField("error", err),
			)
		}
	}()
	if err != nil {
		return err
	}

	if conf.LocalRetry != nil {
		err = retry.Do(
			func() error {
				token := c.Client.Publish(topic, conf.Qos, false, message)
				token.Wait()
				return token.Error()
			},
			retry.Attempts(conf.LocalRetry.Attempts),
			retry.DelayType(conf.LocalRetry.DelayTypeFunc),
			retry.LastErrorOnly(true),
			retry.RetryIf(func(err error) bool {
				return err != nil && !baseError.IsNotSystemError(err)
			}),
		)
	}

	token := c.Client.Publish(topic, conf.Qos, false, message)
	token.Wait()
	return token.Error()
}

type SubConfig struct {
	Qos        byte
	LogLevel   string
	SubLogger  logger.Logger
	LocalRetry *LocalRetry
}
type SubOption func(*SubConfig)

func SubWithQos(val byte) SubOption {
	return func(c *SubConfig) {
		c.Qos = val
	}
}
func SubWithLogLevel(val string) SubOption {
	return func(c *SubConfig) {
		c.LogLevel = val
	}
}
func SubWithLocalRetry(val *LocalRetry) SubOption {
	return func(c *SubConfig) {
		c.LocalRetry = val
	}
}

func (c *Client) Subscribe(topic string, handler Handler, opts ...SubOption) error {
	conf := &SubConfig{
		Qos:      1,
		LogLevel: "info",
	}
	for _, apply := range opts {
		apply(conf)
	}
	if conf.SubLogger == nil {
		conf.SubLogger = c.SubLogger
	}
	token := c.Client.Subscribe(topic, conf.Qos, func(client mqtt.Client, msg mqtt.Message) {
		go SubscribeHandler(client, msg, conf, handler)
	})
	token.Wait()
	return token.Error()
}

func (c *Client) Disconnect() {
	c.Client.Disconnect(250)
}

func SubscribeHandler(client mqtt.Client, m mqtt.Message, conf *SubConfig, handler Handler) {
	var (
		ignoreErr  error
		handlerErr error
		startTime  = time.Now()
	)
	defer func() {
		latency := time.Since(startTime).Milliseconds()
		if handlerErr != nil || conf.LogLevel == "info" {
			conf.SubLogger.Info(
				string(m.Payload()),
				logger.NewField("startTime", startTime),
				logger.NewField("latency", latency),
				logger.NewField("topic", m.Topic()),
				logger.NewField("id", fmt.Sprintf("%d", m.MessageID())),
				logger.NewField("ignore_err", ignoreErr),
				logger.NewField("error", handlerErr),
			)
		}
	}()

	if conf.LocalRetry != nil {
		handlerErr = retry.Do(
			func() error {
				return handler(m.Topic(), m.Payload())
			},
			retry.Attempts(conf.LocalRetry.Attempts),
			retry.DelayType(conf.LocalRetry.DelayTypeFunc),
			retry.LastErrorOnly(true),
			retry.RetryIf(func(err error) bool {
				return err != nil && !baseError.IsNotSystemError(err)
			}),
		)
	} else {
		handlerErr = handler(m.Topic(), m.Payload())
	}

	if handlerErr != nil && baseError.IsNotSystemError(handlerErr) {
		ignoreErr = handlerErr
		handlerErr = nil
	}
	if handlerErr == nil {
		m.Ack()
	}
}
