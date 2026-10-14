package main

import "github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/examples/showcase/support"

func supportTicket(id, customer, subject, body string) support.Ticket {
	return support.Ticket{ID: id, CustomerID: customer, Subject: subject, Body: body}
}
