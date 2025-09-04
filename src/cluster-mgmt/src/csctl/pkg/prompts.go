package pkg

import (
	"bufio"
	"fmt"
	"os"
	"time"
	"unicode"
)

func ConfirmYNWithTimeout(defaultVal bool, timeoutSec int, promptFormat string, optvars ...interface{}) bool {
	var timeout time.Duration
	timeout = time.Duration(timeoutSec) * time.Second
	ch := make(chan bool, 1)
	defer close(ch)
	go func() {
		fmt.Printf(promptFormat, optvars...)
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}
		char = unicode.ToLower(char)
		if char == 'y' {
			ch <- true
		} else if char == 'n' {
			ch <- false
		} else {
			ch <- defaultVal
		}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case confirmedVal := <-ch:
		return confirmedVal
	case <-timer.C:
		fmt.Printf("\nResponse timed out after %d seconds.\n", timeoutSec)
		return defaultVal
	}
}
