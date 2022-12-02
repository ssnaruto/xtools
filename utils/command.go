package utils

import (
	"io/ioutil"
	"os/exec"
)

type Command struct {
	Cmd       string
	OutHandle func(string, int) bool // return true to stop commands
	ErrHandle func(string, int) bool // return true to stop commands
}

type Exec struct {
	commands []Command
}

func (e *Exec) AddCommand(c Command) {
	e.commands = append(e.commands, c)
}

func (e *Exec) Run() {

	for step, command := range e.commands {

		cmd := exec.Command("/bin/sh", "-c", command.Cmd)
		stdOut, _ := cmd.StdoutPipe()
		stdErr, _ := cmd.StderrPipe()
		cmd.Start()

		errLog, _ := ioutil.ReadAll(stdErr)
		outLog, _ := ioutil.ReadAll(stdOut)

		if len(errLog) > 0 {
			stopSignal := command.ErrHandle(string(errLog), step+1)
			if stopSignal == true {
				break
			}
		} else {
			stopSignal := command.OutHandle(string(outLog), step+1)
			if stopSignal == true {
				break
			}
		}

		cmd.Wait()

	}

}

func NewExec() Exec {
	return Exec{}
}

func NewCommand(cmd string) Command {
	return Command{
		Cmd: cmd,
		OutHandle: func(mss string, step int) bool {
			return false
		},
		ErrHandle: func(mss string, step int) bool {
			return false
		},
	}
}
