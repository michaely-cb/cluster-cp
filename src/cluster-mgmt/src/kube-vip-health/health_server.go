package main

import (
	"log"
	"net"
	"strconv"
)

type HealthServer struct {
	port int

	srv net.Listener // nil srv means server is stopped
}

// Non-threadsafe health server
func NewHealthServer(port int) *HealthServer {
	return &HealthServer{port: port}
}

func (s *HealthServer) Start() error {
	if s.srv != nil {
		return nil
	}

	srv, err := net.Listen("tcp", ":"+strconv.Itoa(s.port))
	if err != nil {
		return err
	}
	s.srv = srv
	go s.run(srv)
	return nil
}

func (s *HealthServer) run(srv net.Listener) {
	for s.srv != nil {
		conn, err := srv.Accept()
		if err != nil {
			if s.srv == nil {
				break // srv.Close was called by Stop()
			} else {
				log.Fatal("Unexpected error from health server Accept: ", err.Error())
			}
		}
		if conn != nil {
			_ = conn.Close()
		}
	}
}

func (s *HealthServer) Stop() {
	if s.srv == nil {
		return
	}
	srv := s.srv
	s.srv = nil
	_ = srv.Close()
}
