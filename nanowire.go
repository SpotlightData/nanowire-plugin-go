package nanowireplugin

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/minio/minio-go"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"bitbucket.org/spotlightdatateam/hcc_nmo_go"
	"bitbucket.org/spotlightdatateam/lsqlib"
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
}

// Plugin stores state and client handlers for the plugin
type Plugin struct {
	config   *Config
	receiver *lsqlib.Receiver
	sender   *lsqlib.Sender
	minio    *minio.Client
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
	plugin := Plugin{
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

	active := make(map[string]struct{})

	requestHandler := func(ctx context.Context, delivery amqp.Delivery) {
		logger.Debug("consumed message",
			zap.Uint64("delivery_tag", delivery.DeliveryTag))

		payload := V3Payload{}

		err := json.Unmarshal(delivery.Body, &payload)
		if err != nil {
			logger.Warn("failed to unmarshal delivery body",
				zap.Error(err),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			delivery.Reject(false)
			return
		}

		errs := payload.NMO.Validate()
		if errs != nil {
			logger.Warn("failed to validate nmo",
				zap.Errors("errors", errs),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			delivery.Reject(false)
			return
		}

		if !ensureThisPlugin(name, payload.NMO.Job.Workflow) {
			logger.Warn("plugin does not exist in its own workflow",
				zap.String("job_id", payload.NMO.Job.JobID),
				zap.String("task_id", payload.NMO.Task.TaskID),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			delivery.Reject(false)
			return
		}

		nextPlugin := getNextPlugin(name, payload.NMO.Job.Workflow)
		if nextPlugin == "" {
			logger.Debug("plugin has no children",
				zap.String("job_id", payload.NMO.Job.JobID),
				zap.String("task_id", payload.NMO.Task.TaskID),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
		}

		path := filepath.Join(payload.NMO.Task.TaskID, "input", "source", payload.NMO.Source.Name)

		exists, err := plugin.minio.BucketExists(payload.NMO.Job.JobID)
		if err != nil {
			logger.Warn("failed to check if job bucket exists",
				zap.Error(err),
				zap.String("job_id", payload.NMO.Job.JobID),
				zap.String("task_id", payload.NMO.Task.TaskID),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			delivery.Reject(false)
			return
		}
		if !exists {
			logger.Warn("job_id does not have a bucket",
				zap.String("job_id", payload.NMO.Job.JobID),
				zap.String("task_id", payload.NMO.Task.TaskID),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			delivery.Reject(false)
			return
		}

		url, err := plugin.minio.PresignedGetObject(payload.NMO.Job.JobID, path, time.Hour, nil)
		if err != nil {
			logger.Warn("failed to generate presigned url for source file",
				zap.Error(err),
				zap.String("job_id", payload.NMO.Job.JobID),
				zap.String("task_id", payload.NMO.Task.TaskID),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			delivery.Reject(false)
			return
		}

		payload.JSONLD = callback(payload.NMO, payload.JSONLD, url)

		if payload.JSONLD == nil {
			logger.Warn("result from plugin is nil",
				zap.String("job_id", payload.NMO.Job.JobID),
				zap.String("task_id", payload.NMO.Task.TaskID),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
		}

		payloadRaw, err := json.Marshal(&payload)
		if err != nil {
			logger.Warn("failed to marshal payload",
				zap.String("job_id", payload.NMO.Job.JobID),
				zap.String("task_id", payload.NMO.Task.TaskID),
				zap.Uint64("delivery_tag", delivery.DeliveryTag))
			delivery.Reject(false)
			return
		}

		if nextPlugin != "" {
			if _, ok := active[payload.NMO.Job.JobID]; !ok {
				plugin.sender, err = lsqlib.NewQueueSender(context.Background(), plugin.config.AmqpHost, plugin.config.AmqpPort, plugin.config.AmqpMgrPort, plugin.config.AmqpUser, plugin.config.amqpPass, &lsqlib.SenderConfig{
					LogHandler:   queueLogHandler,
					ErrorHandler: queueErrorHandler,
					PerJobNaming: false,
					Identifier:   nextPlugin,
				})
				if err != nil {
					logger.Warn("failed to create new queue sender",
						zap.Error(err),
						zap.String("job_id", payload.NMO.Job.JobID),
						zap.String("task_id", payload.NMO.Task.TaskID),
						zap.Uint64("delivery_tag", delivery.DeliveryTag))
					delivery.Reject(false)
					return
				}
			} else {
				active[payload.NMO.Job.JobID] = struct{}{}
			}

			err = plugin.sender.Send(0, "", payloadRaw)
			if err != nil {
				logger.Warn("failed to send payload",
					zap.Error(err),
					zap.String("job_id", payload.NMO.Job.JobID),
					zap.String("task_id", payload.NMO.Task.TaskID),
					zap.Uint64("delivery_tag", delivery.DeliveryTag))
				delivery.Reject(false)
				return
			}
		}
		delivery.Ack(false)
	}

	plugin.receiver, err = lsqlib.NewQueueReceiver(context.Background(), plugin.config.AmqpHost, plugin.config.AmqpPort, plugin.config.AmqpMgrPort, plugin.config.AmqpUser, plugin.config.amqpPass, &lsqlib.ReceiverConfig{
		Handler:          requestHandler,
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

func ensureThisPlugin(name string, workflow nmo.WorkFlow) bool {
	for _, wp := range workflow {
		if wp.Config.Name == name {
			return true
		}
	}
	return false
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
