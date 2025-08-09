package transaction

import "sync"

type Transaction struct {
	Commands []CommandQueue
	mu       sync.Mutex
}

type CommandQueue struct {
	Name string
	Args []string
}

func NewTransaction() *Transaction {
	return &Transaction{Commands: []CommandQueue{},
		mu: sync.Mutex{}}
}

func (t *Transaction) AddCommand(name string, args []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Commands = append(t.Commands, CommandQueue{
		Name: name,
		Args: args,
	})
}

func (t *Transaction) Execute(command CommandQueue) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return nil
}
