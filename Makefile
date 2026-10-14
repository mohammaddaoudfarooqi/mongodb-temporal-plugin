# mongodb-temporal-plugin — build & test entrypoints.
#
# Targets are a contract with CI: .github/workflows/ci.yml invokes these,
# so changes here change CI behavior.

GO      ?= go
PKG     := ./...

.PHONY: help
help:
	@echo "Common targets:"
	@echo "  make vet           - go vet"
	@echo "  make staticcheck   - staticcheck (requires install)"
	@echo "  make unit          - unit tests (no external services)"
	@echo "  make integration   - integration tests (requires Docker)"
	@echo "  make conformance   - upstream Temporal conformance suite"
	@echo "  make lint          - guard scripts for known regressions"
	@echo "  make fuzz          - fuzz the visibility query converters"
	@echo "  make dco           - check DCO sign-off on HEAD commits"
	@echo "  make build         - build server + tool binaries"
	@echo "  make qualify-lint - fmt/validate/shellcheck the AWS + Atlas release qualification"
	@echo "  make qualify-test - offline tests of the release qualification (no cloud access)"

.PHONY: vet
vet:
	$(GO) vet $(PKG)

.PHONY: staticcheck
staticcheck:
	staticcheck $(PKG)

# COVERED scopes the coverage gate to packages that contain non-stub
# logic. Pure-stub packages (every `NewStub()` that returns
# serviceerror.Unimplemented) add zero testable lines; including them
# deflates the gate without informing us about real code health.
#
# Two scopes exist:
#  - COVERED_UNIT        packages with unit tests — these carry the
#                        unit-only gate on PRs.
#  - COVERED_COMBINED    packages exercised by unit OR integration —
#                        the combined coverage gate uses this. Stores
#                        that live behind testcontainers show up here
#                        because they are reached only by -tags=integration.
COVERED_UNIT     := ./internal/config/...,./internal/mongo/...,./internal/observe/...,./internal/plugin/...,./internal/tools/...,./internal/visibility/converter/...,./pkg/...
COVERED_COMBINED := $(COVERED_UNIT),./internal/clustermetastore/...,./internal/executionstore/...,./internal/metadatastore/...,./internal/nexusstore/...,./internal/queuestore/...,./internal/schema/...,./internal/shardstore/...,./internal/taskstore/...,./internal/visibility/...
# back-compat alias (the unit-only target and coverage-gate key off this)
COVERED := $(COVERED_UNIT)

.PHONY: unit
unit:
	$(GO) test -short -race -cover -coverprofile=coverage.out -coverpkg=$(COVERED) $(PKG)

# coverage-gate enforces the unit-test floor (about 60% is achievable
# with unit tests alone: much of the store code is only reachable
# against a real MongoDB). The 80% target is the combined unit +
# integration gate (coverage-combined-gate).
.PHONY: coverage-gate
coverage-gate: coverage.out
	@threshold=60.0; \
	total=$$($(GO) tool cover -func=coverage.out | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	if [ -z "$$total" ]; then echo "coverage-gate: no coverage.out; run 'make unit' first"; exit 1; fi; \
	awk -v t=$$total -v th=$$threshold 'BEGIN { if (t+0 < th+0) { printf "coverage-gate FAIL: %s%% < %s%% (unit-only)\n", t, th; exit 1 } else { printf "coverage-gate OK: %s%% >= %s%% (unit-only)\n", t, th } }'

.PHONY: integration
integration:
	$(GO) test -race -tags=integration $(PKG)

# integration-coverage runs the integration suite and writes a coverage
# profile scoped to COVERED_COMBINED (the superset of packages that
# either unit or integration exercises). The unit profile is rerun with
# the same scope so both profiles cover the same keyspace before merge.
.PHONY: integration-coverage
integration-coverage:
	$(GO) test -short -race -cover -coverprofile=coverage.out             -coverpkg=$(COVERED_COMBINED) $(PKG)
	$(GO) test        -race -tags=integration -cover -coverprofile=coverage-integration.out -coverpkg=$(COVERED_COMBINED) $(PKG)

# coverage-combined-gate enforces a regression-detection floor on the
# merged unit+integration profile (see ci/scripts/coverage-merge.sh).
#
# The target is >=80%, but today the combined profile sits
# at ~65% because non-driver internals (visibility converter,
# serializer adapters, schema/apply) have unit-test gaps that neither
# unit nor integration exercises end-to-end. Setting the threshold at
# 80% now would produce red CI that reports a known gap, not a
# regression — which is the opposite of what a gate is for.
#
# Threshold is set just under the current reading (65.0%) so any
# regression in existing coverage trips the gate immediately. Raising
# it toward 80% is planned work.
.PHONY: coverage-combined-gate
coverage-combined-gate: coverage.out coverage-integration.out
	@THRESHOLD=65.0 ci/scripts/coverage-merge.sh

.PHONY: conformance
conformance:
	$(GO) test -race -tags=conformance -timeout=45m $(PKG)

# lint bundles guard scripts that keep known regressions out:
#   lint-rejectjs   every query dispatcher rejects server-side JavaScript
#   lint-translate  every index setup routes driver errors through Translate
#   lint-test-seams no *ForTest[s] symbols in production code
#   lint-observe    every public store method calls observeOp
# The docker image lint runs post-build in CI, not here.
.PHONY: lint
lint: lint-prevention

# fuzz drives adversarial visibility queries through both converters;
# accepted queries must produce only allow-listed operators, never
# JavaScript. FUZZTIME per target (default 30s).
FUZZTIME ?= 30s
.PHONY: fuzz
fuzz:
	$(GO) test -run '^$$' -fuzz FuzzBSONConverter_OnlySafeOperators -fuzztime $(FUZZTIME) ./internal/visibility/converter/
	$(GO) test -run '^$$' -fuzz FuzzSearchConverter_NoJavaScript -fuzztime $(FUZZTIME) ./internal/visibility/converter/

.PHONY: lint-prevention
lint-prevention:
	@ci/scripts/lint-rejectjs.sh
	@ci/scripts/lint-translate.sh
	@ci/scripts/lint-test-seams.sh
	@ci/scripts/lint-observe.sh

.PHONY: dco
dco:
	@if git log -1 --format='%B' | grep -q '^Signed-off-by: '; then \
	  echo 'DCO OK'; \
	else \
	  echo 'FAIL: HEAD commit missing Signed-off-by'; exit 1; \
	fi

.PHONY: build
build:
	$(GO) build -o bin/temporal-mongodb-server ./cmd/temporal-mongodb-server
	$(GO) build -o bin/temporal-mongodb-tool   ./cmd/temporal-mongodb-tool

# AWS + Atlas release qualification (deploy/aws-qualify). Both
# targets are offline: mock providers and stubbed tools, no credentials.
QUALIFY := deploy/aws-qualify

.PHONY: qualify-lint
qualify-lint:
	terraform -chdir=$(QUALIFY)/terraform fmt -check -recursive
	terraform -chdir=$(QUALIFY)/terraform init -backend=false -input=false >/dev/null
	terraform -chdir=$(QUALIFY)/terraform validate
	shellcheck -x -P SCRIPTDIR $(QUALIFY)/scripts/qualify.sh $(QUALIFY)/scripts/lib/*.sh \
		$(QUALIFY)/remote/*.sh $(QUALIFY)/tests/stubs/* examples/e2e/scripts/run-native-server.sh
	cd examples/e2e && $(GO) vet ./...

.PHONY: qualify-test
qualify-test:
	terraform -chdir=$(QUALIFY)/terraform test
	bats $(QUALIFY)/tests examples/e2e/scripts/run-native-server.bats
	cd examples/e2e && $(GO) test -race ./...

.PHONY: clean
clean:
	rm -rf bin dist coverage.*

# Maintainer-only targets, present in the internal repository only.
-include internal.mk
