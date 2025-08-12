package db

import "sync"

type ListStore struct {
	List                map[string][]string
	Mu                  sync.Mutex
	blockingListClients map[string][]chan string
	listMu              sync.Mutex
}

func NewListStore() *ListStore {
	return &ListStore{
		List:                make(map[string][]string),
		blockingListClients: make(map[string][]chan string),
	}
}

func (ls *ListStore) AddBlockingClient(key string, clientChan chan string) {
	ls.listMu.Lock()
	defer ls.listMu.Unlock()
	ls.blockingListClients[key] = append(ls.blockingListClients[key], clientChan)
}

func (ls *ListStore) RemoveBlockingClient(key string, clientChan chan string) {
	ls.listMu.Lock()
	defer ls.listMu.Unlock()
	if clients, ok := ls.blockingListClients[key]; ok {
		for i, ch := range clients {
			if ch == clientChan {
				close(ch)
				ls.blockingListClients[key] = append(clients[:i], clients[i+1:]...)
				return
			}
		}
	}
}

func (ls *ListStore) WakeBlockingClient(key string, value string) {
	ls.listMu.Lock()
	defer ls.listMu.Unlock()
	if clients, ok := ls.blockingListClients[key]; ok && len(clients) > 0 {
		clientChan := clients[0]
		clientChan <- value
		ls.blockingListClients[key] = clients[1:]
	}
}

func (ls *ListStore) LPop(key string, count int) []string {
	ls.Mu.Lock()
	defer ls.Mu.Unlock()

	list, ok := ls.List[key]
	if !ok || len(list) == 0 {
		return nil
	}

	if count <= 0 {
		return []string{}
	}

	if count >= len(list) {
		removedElements := list
		delete(ls.List, key)
		return removedElements
	}

	removedElements := list[:count]
	ls.List[key] = list[count:]
	return removedElements
}

func (ls *ListStore) LPush(key string, elements []string) int {
	ls.Mu.Lock()
	defer ls.Mu.Unlock()

	oldList := ls.List[key]
	if _, ok := ls.List[key]; !ok {
		ls.List[key] = make([]string, 0)
	}

	newList := make([]string, len(elements)+len(oldList))
	for i := range elements {
		newList[i] = elements[len(elements)-1-i]
	}
	copy(newList[len(elements):], oldList)

	ls.List[key] = newList
	if len(elements) > 0 {
		ls.WakeBlockingClient(key, elements[len(elements)-1])
	}
	return len(newList)
}

func (ls *ListStore) RPush(key string, elements []string) int {
	ls.Mu.Lock()
	defer ls.Mu.Unlock()

	if _, ok := ls.List[key]; !ok {
		ls.List[key] = make([]string, 0)
	}

	oldLength := len(ls.List[key])
	ls.List[key] = append(ls.List[key], elements...)
	if len(ls.List[key]) > oldLength && len(elements) > 0 {
		ls.WakeBlockingClient(key, elements[0])
	}
	return len(ls.List[key])
}
