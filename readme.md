# nanowireplugin
--
    import "github.com/SpotlightData/nanowire-plugin-go"

Package nanowireplugin provides a Go interface to the Nanowire pipeline system.
It's a very simple library to implement as it only consists of a single function
which binds a user-defined callback function to the internal Nanowire work
queue.

## Usage

#### func  Bind

```go
func Bind(callback TaskEvent, name string) (err error)
```
Bind links a user function to the Nanowire pipeline system and calls it whenever
a task is ready

#### type Config

```go
type Config struct {
	AmqpHost    string
	AmqpPort    string
	AmqpMgrPort string
	AmqpUser    string

	MinioScheme string
	MinioHost   string
	MinioPort   string
	MinioAccess string
}
```

Config stores credentials for various services

#### type TaskEvent

```go
type TaskEvent func(nmo *nmo.NMO, jsonld map[string]interface{}, url *url.URL) map[string]interface{}
```

TaskEvent is a callback function signature triggered when a new file is ready to
be processed

#### type V3Payload

```go
type V3Payload struct {
	NMO    *nmo.NMO               `json:"nmo"`
	JSONLD map[string]interface{} `json:"jsonld"`
}
```

V3Payload wraps NMO with an empty JSONLD object
