package exchange

import "sync"

type PubSub struct {
	mu          sync.RWMutex
	subscribers map[string][]chan string
}

func NewPubSub() *PubSub {
	return &PubSub{
		subscribers: make(map[string][]chan string),
	}
}

func (p *PubSub) Subscribe(channel string) (chan string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	subChannel := make(chan string)
	p.subscribers[channel] = append(p.subscribers[channel], subChannel)

	return subChannel, len(p.subscribers[channel])
}

func (p *PubSub) Publish(channel string, message string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	subscribers, ok := p.subscribers[channel]
	if !ok {
		return 0
	}

	for _, subChannel := range subscribers {
		subChannel <- message
	}

	return len(subscribers)
}
