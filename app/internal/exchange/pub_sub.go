package exchange

import "sync"

type PubSub struct {
	mu          sync.RWMutex
	subscribers map[string]chan string
}

func NewPubSub() *PubSub {
	return &PubSub{
		subscribers: make(map[string]chan string),
	}
}

func (p *PubSub) Subscribe(channel string) chan string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.subscribers[channel]; !ok {
		p.subscribers[channel] = make(chan string)
	}

	return p.subscribers[channel]
}

func (p *PubSub) Publish(channel string, message string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if _, ok := p.subscribers[channel]; ok {
		p.subscribers[channel] <- message
		return len(p.subscribers[channel])
	}

	return 0
}
