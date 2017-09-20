// Package nanowireplugin provides a Go interface to the Nanowire pipeline system. It's a very
// simple library to implement as it only consists of a single function which binds a user-defined
// callback function to the internal Nanowire work queue.
package nanowireplugin

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"time"

	monitor "bitbucket.org/spotlightdatateam/hcc_lightstream_monitor/client"
	"bitbucket.org/spotlightdatateam/hcc_lightstream_monitor/shared"
	"bitbucket.org/spotlightdatateam/hcc_nmo_go"
	"bitbucket.org/spotlightdatateam/lsqlib"

	"github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config stores credentials for various services
type Config struct {
	AmqpHost    string
	AmqpPort    string
	AmqpMgrPort string
	AmqpUser    string
	amqpPass    string
	MinioScheme string
	MinioHost   string
	MinioPort   string
	MinioAccess string
	minioSecret string
	MonitorURL  string
}

// pluginHandler stores state and client handlers for the plugin
type pluginHandler struct {
	config   *Config
	receiver *lsqlib.Receiver
	sender   *lsqlib.Sender
	minio    *minio.Client
	monitor  *monitor.Client
	name     string
	callback TaskEvent
	next     string
}

// TaskEvent is a callback function signature triggered when a new file is ready to be processed
type TaskEvent func(nmo *nmo.NMO, jsonld map[string]interface{}, url *url.URL) map[string]interface{}

// V3Payload wraps NMO with an empty JSONLD object
type V3Payload struct {
	NMO    *nmo.NMO               `json:"nmo"`
	JSONLD map[string]interface{} `json:"jsonld"`
}

// Bind links a user function to the Nanowire pipeline system and calls it whenever a task is ready
func Bind(callback TaskEvent, name string) (err error) {
	plugin := pluginHandler{
		config: &Config{
			AmqpHost:    configStrFromEnv("AMQP_HOST"),
			AmqpPort:    configStrFromEnv("AMQP_PORT"),
			AmqpMgrPort: configStrFromEnv("AMQP_MANAGER_PORT"),
			AmqpUser:    configStrFromEnv("AMQP_USER"),
			amqpPass:    configStrFromEnv("AMQP_PASS"),
			MinioScheme: configStrFromEnv("MINIO_SCHEME"),
			MinioHost:   configStrFromEnv("MINIO_HOST"),
			MinioPort:   configStrFromEnv("MINIO_PORT"),
			MinioAccess: configStrFromEnv("MINIO_ACCESS"),
			minioSecret: configStrFromEnv("MINIO_SECRET"),
			MonitorURL:  configStrFromEnv("MONITOR_URL"),
		},
	}

	minioSecure := false
	if plugin.config.MinioScheme == "https" {
		minioSecure = true
	}

	plugin.minio, err = minio.New(plugin.config.MinioHost+":"+plugin.config.MinioPort, plugin.config.MinioAccess, plugin.config.minioSecret, minioSecure)
	if err != nil {
		return errors.Wrap(err, "failed to create new Minio client")
	}
	_, err = plugin.minio.ListBuckets()
	if err != nil {
		return errors.Wrap(err, "failed to communicate with Minio")
	}
	plugin.minio.SetAppInfo(name, "0.1.0") // todo: 0.1.0

	plugin.monitor, err = monitor.NewClient(plugin.config.MonitorURL)
	if err != nil {
		return errors.Wrap(err, "failed to connect to monitor")
	}

	plugin.name = name
	plugin.callback = callback

	plugin.receiver, err = lsqlib.NewQueueReceiver(context.Background(), plugin.config.AmqpHost, plugin.config.AmqpPort, plugin.config.AmqpMgrPort, plugin.config.AmqpUser, plugin.config.amqpPass, &lsqlib.ReceiverConfig{
		Handler:          plugin.requestHandler,
		LogHandler:       queueLogHandler,
		ErrorHandler:     queueErrorHandler,
		PerJobNaming:     false,
		Identifier:       name,
		ConsumerKey:      name,
		ConcurrentQueues: 1,
	})
	if err != nil {
		return errors.Wrap(err, "failed to create new queue receiver")
	}

	return nil
}

func (plugin *pluginHandler) requestHandler(ctx context.Context, delivery amqp.Delivery) {
	logger.Debug("consumed message",
		zap.Uint64("delivery_tag", delivery.DeliveryTag))

	payload := new(V3Payload)

	err := json.Unmarshal(delivery.Body, payload)
	if err != nil {
		logger.Warn("failed to unmarshal delivery body",
			zap.Error(err),
			zap.Uint64("delivery_tag", delivery.DeliveryTag))
		plugin.monitor.SetTaskStatus(
			shared.JobID(payload.NMO.Job.JobID),
			shared.TaskID(payload.NMO.Task.TaskID),
			shared.Position(plugin.name+".consumed"),
			"failed to unmarshal delivery body")
		delivery.Reject(false)
		return
	}

	errs := payload.NMO.Validate()
	if errs != nil {
		logger.Warn("failed to validate nmo",
			zap.Errors("errors", errs),
			zap.Uint64("delivery_tag", delivery.DeliveryTag))
		plugin.monitor.SetTaskStatus(
			shared.JobID(payload.NMO.Job.JobID),
			shared.TaskID(payload.NMO.Task.TaskID),
			shared.Position(plugin.name+".consumed"),
			"failed to validate nmo")
		delivery.Reject(false)
		return
	}

	plugin.monitor.SetTaskStatus(
		shared.JobID(payload.NMO.Job.JobID),
		shared.TaskID(payload.NMO.Task.TaskID),
		shared.Position(plugin.name+".consumed"),
		"")

	err = plugin.requestProcessor(payload)
	if err != nil {
		plugin.monitor.SetTaskStatus(
			shared.JobID(payload.NMO.Job.JobID),
			shared.TaskID(payload.NMO.Task.TaskID),
			shared.Position(plugin.name+".done"),
			err.Error())
		err = delivery.Reject(false)
		if err != nil {
			logger.Fatal("failed to reject",
				zap.String("job_id", payload.NMO.Job.JobID),
				zap.String("task_id", payload.NMO.Task.TaskID),
				zap.Uint64("delivery_tag", delivery.DeliveryTag),
				zap.Error(err))
		}
	}
	err = delivery.Ack(false)
	if err != nil {
		logger.Fatal("failed to ack",
			zap.String("job_id", payload.NMO.Job.JobID),
			zap.String("task_id", payload.NMO.Task.TaskID),
			zap.Uint64("delivery_tag", delivery.DeliveryTag),
			zap.Error(err))
	}
}

func (plugin *pluginHandler) requestProcessor(payload *V3Payload) error {
	this := getThisPlugin(plugin.name, payload.NMO.Job.Workflow)
	if this == -1 {
		return errors.New("plugin does not exist in received message workflow")
	}

	path := filepath.Join(payload.NMO.Task.TaskID, "input", "source", payload.NMO.Source.Name)

	exists, err := plugin.minio.BucketExists(payload.NMO.Job.JobID)
	if err != nil {
		return errors.Wrap(err, "failed to check if job bucket exists")
	}
	if !exists {
		return errors.New("job_id does not have a bucket")
	}

	url, err := plugin.minio.PresignedGetObject(payload.NMO.Job.JobID, path, time.Hour, nil)
	if err != nil {
		return errors.Wrap(err, "failed to generate presigned url for source file")
	}

	for k, v := range payload.NMO.Job.Workflow[this].Env {
		err = os.Setenv(k, v)
		if err != nil {
			return errors.Wrap(err, "failed to set environment variable")
		}
	}
	result := plugin.callback(payload.NMO, payload.JSONLD, url)

	if result != nil {
		payload.JSONLD = result

		logger.Debug("finished running user code with nil result",
			zap.String("job_id", payload.NMO.Job.JobID),
			zap.String("task_id", payload.NMO.Task.TaskID))
	} else {
		logger.Debug("finished running user code",
			zap.String("job_id", payload.NMO.Job.JobID),
			zap.String("task_id", payload.NMO.Task.TaskID))
	}

	payloadRaw, err := json.Marshal(&payload)
	if err != nil {
		return errors.Wrap(err, "failed to marshal payload")
	}

	nextPlugin := getNextPlugin(plugin.name, payload.NMO.Job.Workflow)
	if nextPlugin != "" {
		if payload.JSONLD == nil {
			return errors.New("payload jsonld is nil but not final plugin in pipeline")
		}

		if plugin.next != nextPlugin {
			if plugin.sender != nil {
				errs := plugin.sender.Close()
				if errs != nil {
					logger.Fatal("failed to close sender",
						zap.Errors("errors", errs))
				}
			}

			plugin.sender, err = lsqlib.NewQueueSender(context.Background(), plugin.config.AmqpHost, plugin.config.AmqpPort, plugin.config.AmqpMgrPort, plugin.config.AmqpUser, plugin.config.amqpPass, &lsqlib.SenderConfig{
				LogHandler:   queueLogHandler,
				ErrorHandler: queueErrorHandler,
				PerJobNaming: false,
				Identifier:   nextPlugin,
			})
			if err != nil {
				return errors.Wrap(err, "failed to create new queue sender")
			}
		}
		plugin.next = nextPlugin

		err = plugin.sender.Send(0, "", payloadRaw)
		if err != nil {
			return errors.Wrap(err, "failed to send payload")
		}
	} else {
		logger.Debug("plugin has no children",
			zap.String("job_id", payload.NMO.Job.JobID),
			zap.String("task_id", payload.NMO.Task.TaskID))
	}

	return nil
}

func getNextPlugin(name string, workflow nmo.WorkFlow) string {
	found := false
	for _, wp := range workflow {
		if !found {
			if wp.Config.Name == name {
				found = true
			}
		} else {
			return wp.Config.Name
		}
	}
	return ""
}

func getThisPlugin(name string, workflow nmo.WorkFlow) int {
	for i, wp := range workflow {
		if wp.Config.Name == name {
			return i
		}
	}
	return -1
}

func queueLogHandler(fmt string, args ...interface{}) {
	logger.Debug("amqp log: "+fmt, zap.Any("args", args))
}

func queueErrorHandler(err error, fatal bool) {
	var f func(msg string, fields ...zapcore.Field)
	if fatal {
		f = logger.Fatal
	} else {
		f = logger.Error
	}

	f("queue manager encountered error",
		zap.Error(err))
}

func configStrFromEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		logger.Fatal("environment variable not set",
			zap.String("name", key))
	}
	return value
}
