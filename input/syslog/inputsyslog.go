package inputsyslog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"gopkg.in/mcuadros/go-syslog.v2"

	"github.com/Sirupsen/logrus"

	"github.com/tsaikd/KDGoLib/errutil"
	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/logevent"
)

// ModuleName is the name used in config file
const ModuleName = "syslog"

// errors
var (
	ErrorNoValidFormat = errutil.NewFactory("no valid format found")
	ErrorNoValidSocket = errutil.NewFactory("no valid socket type found")
)

// InputConfig holds the output configuration json fields
type InputConfig struct {
	config.InputConfig
	Socket  string `json:"socket"`  // Type of socket, must be one of ["tcp", "udp", "unixgram"].
	Address string `json:"address"` // For UDP/TCP, address must have the form `host:port`. For Unigram socket, the address must be a file system path.
	Format  string `json:"format"`  // RFC to use to decode syslog message. Must be one of ["automatic", "RFC3164", "RFC5424", "RFC6587"]. Default: "automatic".
	Mutate  map[string]string
}

// DefaultInputConfig returns an InputConfig struct with default values
func DefaultInputConfig() InputConfig {
	return InputConfig{
		InputConfig: config.InputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		Format: "automatic",
	}
}

// InitHandler initialize the input plugin
func InitHandler(confraw *config.ConfigRaw) (config.TypeInputConfig, error) {
	conf := DefaultInputConfig()
	if err := config.ReflectConfig(confraw, &conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

// Start wraps the actual function starting the plugin
func (i *InputConfig) Start() {
	i.Invoke(i.start)
}

func (i *InputConfig) start(logger *logrus.Logger, evchan chan logevent.LogEvent) {
	channel := make(syslog.LogPartsChannel)
	handler := syslog.NewChannelHandler(channel)
	server := syslog.NewServer()
	server.SetHandler(handler)

	switch strings.ToLower(i.Format) {
	case "automatic":
		server.SetFormat(syslog.Automatic)
	case "rfc3164":
		server.SetFormat(syslog.RFC3164)
	case "rfc5424":
		server.SetFormat(syslog.RFC5424)
	case "rfc6587":
		server.SetFormat(syslog.RFC6587)
	default:
		logger.Fatal(ErrorNoValidFormat)
	}

	// TODO: server.ListenTCPTLS
	switch strings.ToLower(i.Socket) {
	case "tcp":
		server.ListenTCP(i.Address)
	case "udp":
		server.ListenUDP(i.Address)
	case "unixgram":
		// Remove existing socket
		os.Remove(i.Address)
		// Create socket
		server.ListenUnixgram(i.Address)
		// Set socket permissions.
		if err := os.Chmod(i.Address, 0777); err != nil {
			logger.Fatal(err)
		}
	default:
		logger.Fatal(ErrorNoValidSocket)
	}

	server.Boot()

	go func(channel syslog.LogPartsChannel) {
		for event := range channel {
			for old, new := range i.Mutate {
				event[new] = event[old]
				delete(event, old)
			}
			fmt.Println(event)
			ts := event["timestamp"].(time.Time)
			delete(event, "timestamp")

			evchan <- logevent.LogEvent{
				Timestamp: ts,
				Extra:     event,
			}
		}
	}(channel)

	server.Wait()
}

func parse(conn net.Conn, logger *logrus.Logger, evchan chan logevent.LogEvent) {
	defer conn.Close()

	// Duplicate buffer to be able to read it even after failed json decoding
	var streamCopy bytes.Buffer
	stream := io.TeeReader(conn, &streamCopy)

	dec := json.NewDecoder(stream)
	for {
		// Assume first the message is JSON and try to decode it
		var jsonMsg map[string]interface{}
		if err := dec.Decode(&jsonMsg); err == io.EOF {
			break
		} else if err != nil {
			// If decoding fail, split raw message by line
			// and send a log event per line
			for {
				line, err := streamCopy.ReadString('\n')
				evchan <- logevent.LogEvent{
					Timestamp: time.Now(),
					Message:   line,
				}
				if err != nil {
					break
				}
			}
			break
		}

	}
}
