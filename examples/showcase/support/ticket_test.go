package support

import (
	"testing"
	"time"
)

func TestTriage_CategoryAndPriorityFromText(t *testing.T) {
	cases := []struct {
		subject, body string
		cat           string
		pri           int
	}{
		{"Refund not received", "I was charged twice on my invoice", CategoryBilling, 2},
		{"Cannot log in", "password reset email never arrives", CategoryAccount, 2},
		{"Production cluster down", "outage, all requests failing", CategoryOutage, 1},
		{"Question about the API", "how do I paginate results?", CategoryGeneral, 3},
	}
	for _, c := range cases {
		cat, pri := Triage(c.subject, c.body)
		if cat != c.cat || pri != c.pri {
			t.Errorf("Triage(%q) = %s/P%d, want %s/P%d", c.subject, cat, pri, c.cat, c.pri)
		}
	}
}

func TestSLA_ScalesWithPriorityAndBase(t *testing.T) {
	base := time.Minute
	if SLA(1, base) >= SLA(2, base) || SLA(2, base) >= SLA(3, base) {
		t.Fatalf("higher priority must have a shorter SLA: %v %v %v", SLA(1, base), SLA(2, base), SLA(3, base))
	}
	if SLA(1, base) != base {
		t.Fatalf("P1 SLA must equal the base, got %v", SLA(1, base))
	}
}

func TestEscalate_RaisesPriorityButNotAboveOne(t *testing.T) {
	if Escalate(3) != 2 || Escalate(1) != 1 {
		t.Fatal("escalation must raise priority by one level, capped at P1")
	}
}
