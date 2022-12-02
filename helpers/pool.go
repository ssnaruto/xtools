package helpers

func NewPool(tasks *chan Job, numberOfWorker int, handle func(Job)) Pool {
	p := Pool{
		tasks:      tasks,
		goroutines: numberOfWorker,
		Handle:     handle,
	}
	p.Run()
	return p
}

type Job struct {
	Type string
	Data interface{}
}

type Pool struct {
	tasks      *chan Job
	goroutines int
	Handle     func(Job)
}

func (p *Pool) Run() {
	for i := 1; i <= p.goroutines; i++ {
		go p.worker()
	}
}
func (p *Pool) worker() {
	for job := range *p.tasks {
		p.Handle(job)
	}
}
