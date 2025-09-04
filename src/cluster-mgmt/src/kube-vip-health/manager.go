package main

import (
	"log"
	"os"
	"os/exec"
	"sync"
	"time"
)

type Task interface {
	// Start the background task. Caller should pass a channel which the
	// task closes on exit, optionally passing an error that occurred prior to
	// close. Passing nil without closing the channel signals task start successfully
	Start(taskCh chan error)
}

type ProcTask struct {
	Cmd  string
	Args []string
}

func (p ProcTask) Start(taskCh chan error) {
	cmd := exec.Command(p.Cmd, p.Args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	defer close(taskCh)
	log.Printf("Starting cmd: %s %v", p.Cmd, p.Args)
	if err := cmd.Start(); err != nil {
		log.Printf("Failed to start cmd, %v", err)
		taskCh <- err
	} else {
		taskCh <- nil // signal started
		taskCh <- cmd.Wait()
	}
}

type HealthManager struct {
	mu     sync.Mutex
	cancel chan bool // non-nil indicates running manager

	probes map[string]*Probe
	server *HealthServer
	task   Task
	taskCh chan error
}

func NewHealthManager(task Task, server *HealthServer, probes []*Probe) *HealthManager {
	p := make(map[string]*Probe)
	for _, probe := range probes {
		p[probe.Name] = probe
	}
	return &HealthManager{
		mu:     sync.Mutex{},
		probes: p,
		server: server,
		task:   task,
	}
}

// Start starts the Manager in a go routine and returns a channel that is closed when all the probes reach an OK state.
func (m *HealthManager) RunForever() {
	m.mu.Lock()
	if m.cancel != nil {
		panic("Start called on running HealthManager")
	}
	m.cancel = make(chan bool)
	m.mu.Unlock()
	m.run(m.cancel)
}

// Main manager loop. Starts all probes, then listens for probe state transitions. Once all probes are healthy,
// the manager starts the main task which should never be restarted.
func (m *HealthManager) run(cancel chan bool) {
	transitionCh := make(chan int, 8) // probes send 1 to indicate transition to error state, -1 to transition healthy
	failedProbeCount := 0
	for _, p := range m.probes {
		if !p.ok {
			failedProbeCount += 1
		}
	}
	// launch probe go routines for non-blocking + independent periods
	for _, p := range m.probes {
		go func(p *Probe) {
			for {
				select {
				case <-cancel:
					return
				case <-time.After(p.Period):
					if transitioned := p.ProbeAndTransition(); transitioned {
						if p.ok {
							transitionCh <- -1
						} else {
							transitionCh <- 1
						}
					}
				}
			}
		}(p)
	}
	m.taskCh = make(chan error)
	taskStarted := false
	for {
		select {
		case <-cancel:
			return
		case taskErr, ok := <-m.taskCh:
			if !ok || taskErr != nil {
				close(cancel) // stop probes
				if taskErr != nil {
					log.Printf("health manager's task exited with err: %v", taskErr)
					os.Exit(1)
				}
				log.Printf("health manager's task exited normally")
				return
			}
			// null transition used to signal that the task started ok, triggers server start
			transitionCh <- 0
		case transition := <-transitionCh:
			failedProbeCount += transition
			if failedProbeCount > 0 {
				m.server.Stop() // idempotent
				continue
			}
			if !taskStarted {
				log.Printf("Initial health checks ok, starting main task")
				go m.task.Start(m.taskCh)
				taskStarted = true
			} else {
				if err := m.server.Start(); err != nil {
					log.Fatal("Failed to start health server ", err)
				}
			}
		}
	}
}

func (m *HealthManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		close(m.cancel)
		m.cancel = nil
	}
}
