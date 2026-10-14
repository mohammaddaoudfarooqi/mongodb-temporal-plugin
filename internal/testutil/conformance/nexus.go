package conformance

import (
	"testing"

	"go.temporal.io/server/common/persistence"

	"github.com/mohammaddaoudfarooqi/mongodb-temporal-plugin/internal/nexusstore"
)

// runNexusStore exercises endpoint CRUD with
// table-version CAS.
func runNexusStore(t *testing.T, f Fixture) {
	t.Run("CreateOrUpdate_Then_Get", func(t *testing.T) {
		tc := newTestContext(t, f)
		nx := nexusstore.New(tc.DB)

		id := uniqStr("ep")
		err := nx.CreateOrUpdateNexusEndpoint(tc.Ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: 0,
			Endpoint: persistence.InternalNexusEndpoint{
				ID: id, Version: 0, Data: dataBlob("ep"),
			},
		})
		fatalOnErr(t, "CreateOrUpdateNexusEndpoint", err)

		got, err := nx.GetNexusEndpoint(tc.Ctx, &persistence.GetNexusEndpointRequest{ID: id})
		fatalOnErr(t, "GetNexusEndpoint", err)
		if string(got.Data.Data) != "ep" {
			t.Errorf("endpoint blob drift: %q", got.Data.Data)
		}
	})

	t.Run("List_PaginatesCleanly", func(t *testing.T) {
		tc := newTestContext(t, f)
		nx := nexusstore.New(tc.DB)

		for i := 0; i < 3; i++ {
			err := nx.CreateOrUpdateNexusEndpoint(tc.Ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
				LastKnownTableVersion: int64(i),
				Endpoint: persistence.InternalNexusEndpoint{
					ID: uniqStr("ep"), Version: 0, Data: dataBlob("ep"),
				},
			})
			fatalOnErr(t, "CreateOrUpdate", err)
		}
		resp, err := nx.ListNexusEndpoints(tc.Ctx, &persistence.ListNexusEndpointsRequest{
			LastKnownTableVersion: 0,
			PageSize:              100,
		})
		fatalOnErr(t, "List", err)
		if len(resp.Endpoints) < 3 {
			t.Errorf("expected >=3 endpoints; got %d", len(resp.Endpoints))
		}
	})

	t.Run("Delete_StaleTableVersion_Fails", func(t *testing.T) {
		tc := newTestContext(t, f)
		nx := nexusstore.New(tc.DB)

		id := uniqStr("ep")
		err := nx.CreateOrUpdateNexusEndpoint(tc.Ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: 0,
			Endpoint:              persistence.InternalNexusEndpoint{ID: id, Version: 0, Data: dataBlob("ep")},
		})
		fatalOnErr(t, "create", err)

		// Wrong table version → refuse.
		err = nx.DeleteNexusEndpoint(tc.Ctx, &persistence.DeleteNexusEndpointRequest{
			ID: id, LastKnownTableVersion: 999,
		})
		if err == nil {
			t.Error("expected table-version CAS error; got nil")
		}
	})
}
