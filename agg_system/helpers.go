package agg_system

import (
	"sync"
	"time"
)

func NewCronJob(triggerInSeconds int) CronJob {
	return CronJob{
		triggerInSeconds: triggerInSeconds,
		wg:               &sync.WaitGroup{},
	}
}

type CronJob struct {
	triggerInSeconds int
	stopSignal       bool
	jobs             []func()
	wg               *sync.WaitGroup
}

func (c *CronJob) Add(job func()) {
	c.jobs = append(c.jobs, job)
}

func (c *CronJob) Start() {
	c.Stop()
	c.wg.Add(1)
	c.stopSignal = false
	timeCounter := 0

	for {
		if c.stopSignal {
			break
		}

		if timeCounter == c.triggerInSeconds {
			timeCounter = 0
		}
		if timeCounter == 0 {
			for _, job := range c.jobs {
				job()
			}
		}

		time.Sleep(1 * time.Second)
		timeCounter++
	}
	c.wg.Done()
}

func (c *CronJob) Stop() {
	c.stopSignal = true
	c.wg.Wait()
}
