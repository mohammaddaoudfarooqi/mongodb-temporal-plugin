// Package stub centralises the "not yet implemented" error every scaffold
// store returns. Keeping it in one place avoids drift if the wording or
// the error type has to change later in the implementation, and makes the
// scaffold's behavior trivially greppable during the migration to real
// stores.
package stub

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
)

// Err returns a serviceerror.Unimplemented wrapping the store + method
// name. Upstream test harnesses inspect the error message, so including
// the caller name makes conformance-suite baseline diffs readable.
func Err(store, method string) error {
	return serviceerror.NewUnimplemented(
		fmt.Sprintf("mongodb-temporal-plugin: %s.%s not implemented", store, method),
	)
}
