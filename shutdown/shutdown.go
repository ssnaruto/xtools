package shutdown

import "sync"

var wg = &sync.WaitGroup{}

func Add(numberWorker int) {
	wg.Add(numberWorker)
}

func Done() {
	wg.Done()
}

func Wait() {
	wg.Wait()
}
