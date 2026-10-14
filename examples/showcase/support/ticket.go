// Package support holds the support-desk domain: tickets, triage rules
// and service-level targets. It has no Temporal or MongoDB dependency.
package support

import (
	"strings"
	"time"
)

// Ticket categories.
const (
	CategoryOutage  = "outage"
	CategoryBilling = "billing"
	CategoryAccount = "account"
	CategoryGeneral = "general"
)

// Ticket lifecycle states, also the TicketStatus search attribute.
const (
	StatusOpen      = "open"
	StatusAssigned  = "assigned"
	StatusEscalated = "escalated"
	StatusResolved  = "resolved"
)

// Ticket is what a customer submits.
type Ticket struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	Subject    string `json:"subject"`
	Body       string `json:"body"`
}

var rules = []struct {
	category string
	priority int
	words    []string
}{
	{CategoryOutage, 1, []string{"outage", "down", "unavailable", "failing", "data loss"}},
	{CategoryBilling, 2, []string{"refund", "charged", "invoice", "billing", "payment"}},
	{CategoryAccount, 2, []string{"log in", "login", "password", "locked", "sso", "2fa"}},
}

// Triage picks a category and priority (1 = most urgent) from the text.
func Triage(subject, body string) (category string, priority int) {
	text := strings.ToLower(subject + " " + body)
	for _, r := range rules {
		for _, w := range r.words {
			if strings.Contains(text, w) {
				return r.category, r.priority
			}
		}
	}
	return CategoryGeneral, 3
}

// SLA is the time to first response for a priority: the base for P1,
// doubling for each lower level.
func SLA(priority int, base time.Duration) time.Duration {
	if priority < 1 {
		priority = 1
	}
	return base << (priority - 1)
}

// Escalate raises a priority by one level, capped at P1.
func Escalate(priority int) int {
	if priority <= 1 {
		return 1
	}
	return priority - 1
}
