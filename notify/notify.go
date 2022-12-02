package notify

import (
	"fmt"
	"log"
)

var notifyEngine NotifyEngine

func init() {
	notifyEngine.Init()
}

type NotifyChannel interface {
	PushMessage(string, ...interface{}) error
}

type Config struct {
	NotifyChannel NotifyChannel
	Prefix        []string
}

type NotifyEngine struct {
	NotifyChannel
	prefix  string
	buffQue chan string
}

func (n *NotifyEngine) Init() {
	n.buffQue = make(chan string, 500)
	go func() {

		for msg := range n.buffQue {
			if n.NotifyChannel == nil {
				continue
			}
			if err := n.NotifyChannel.PushMessage("", msg); err != nil {
				log.Print(err)
			}
		}

	}()
}

func (n *NotifyEngine) PushMessage(format string, a ...interface{}) {
	var msg string
	if format != "" {
		msg = n.prefix + fmt.Sprintf(format, a...)
	} else {
		msg = n.prefix + fmt.Sprintln(a...)
	}

	n.buffQue <- msg
}

func SetConfig(cfg Config) {
	for _, str := range cfg.Prefix {
		notifyEngine.prefix += str + " / "
	}

	notifyEngine.NotifyChannel = cfg.NotifyChannel
}

func PushMessage(format string, a ...interface{}) {
	notifyEngine.PushMessage(format, a...)
}
