module example.local/neutron-api

go 1.26.0

require github.com/neutron-build/neutron/go v0.3.0

require (
	github.com/gabriel-vasile/mimetype v1.4.12 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.30.1 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

// The example tracks the SDK in this repository for local development.
// Remove this replacement when consuming the tagged SDK independently.
replace github.com/neutron-build/neutron/go => ../../../../go
