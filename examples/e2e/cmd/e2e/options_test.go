package main

import "testing"

func TestWorkerOptions_ZeroKeepsSDKDefaults(t *testing.T) {
	o := workerOptions(0, 0)
	if o.MaxConcurrentWorkflowTaskPollers != 0 || o.MaxConcurrentActivityTaskPollers != 0 {
		t.Errorf("zero pollers changed the SDK defaults: %+v", o)
	}
}

func TestWorkerOptions_SetsPollers(t *testing.T) {
	o := workerOptions(16, 24)
	if o.MaxConcurrentWorkflowTaskPollers != 16 || o.MaxConcurrentActivityTaskPollers != 24 {
		t.Errorf("pollers = %d workflow, %d activity", o.MaxConcurrentWorkflowTaskPollers, o.MaxConcurrentActivityTaskPollers)
	}
}
