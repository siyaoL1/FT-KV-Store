package main

import "sync"

func ConcurrentMutex(url string, fetcher Fethcer, fs *fetchState) {
	if fs.testAndSet[url] {
		return
	}
	urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}
	var done sync.WaitGroup
	for _, u := range urls {
		done.Add(1)
		// We capture the closure which set the done.Done() to run after the recursive call
		go func(u string) {
			defer done.Done()
			ConcurrentMutex(u, fetcher, fs)
		}(u)
	}
	done.Wait()
}


// struct containing the lock and map
type fetchState struct {
	mu sunc.Mutex
	fetched map[string]bool
}

func makeState() *fetchState {
	return &fetchState(fetched: make(map[string]bool))
}

func (fs *fetchState) testAndSet(url string) bool {
	// Need to be locked to update map
	fs.mu.Lock()
	defer fs.mu.Unlock()
	r := fs.fetched[url]
	fs.fetched[url] = true
	return r
}