package main

import "go.temporal.io/sdk/worker"

// workerOptions sets the load worker's task pollers; zero keeps the SDK
// default. A load test's ceiling can be the worker's pollers rather than
// the server, so they are a knob.
func workerOptions(workflowPollers, activityPollers int) worker.Options {
	return worker.Options{
		MaxConcurrentWorkflowTaskPollers: workflowPollers,
		MaxConcurrentActivityTaskPollers: activityPollers,
	}
}
