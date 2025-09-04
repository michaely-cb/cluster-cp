/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import "time"

// DummyWorkQueue implements RateLimitingInterface but actually does nothing.
type DummyWorkQueue struct{}

// Add WorkQueue Add method
func (dummy *DummyWorkQueue) Add(item interface{}) {}

// Len WorkQueue Len method
func (dummy *DummyWorkQueue) Len() int { return 0 }

// Get WorkQueue Get method
func (dummy *DummyWorkQueue) Get() (item interface{}, shutdown bool) { return nil, false }

// Done WorkQueue Done method
func (dummy *DummyWorkQueue) Done(item interface{}) {}

// ShutDown WorkQueue ShutDown method
func (dummy *DummyWorkQueue) ShutDown() {}

// ShuttingDown WorkQueue ShuttingDown method
func (dummy *DummyWorkQueue) ShuttingDown() bool { return true }

// ShutDownWithDrain WorkQueue ShutDownWithDrain method
func (dummy *DummyWorkQueue) ShutDownWithDrain() {}

// AddAfter WorkQueue AddAfter method
func (dummy *DummyWorkQueue) AddAfter(item interface{}, duration time.Duration) {}

// AddRateLimited WorkQueue AddRateLimited method
func (dummy *DummyWorkQueue) AddRateLimited(item interface{}) {}

// Forget WorkQueue Forget method
func (dummy *DummyWorkQueue) Forget(item interface{}) {}

// NumRequeues WorkQueue NumRequeues method
func (dummy *DummyWorkQueue) NumRequeues(item interface{}) int { return 0 }
