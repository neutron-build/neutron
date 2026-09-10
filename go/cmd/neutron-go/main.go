package main

import (
	"os"

	"github.com/neutron-build/neutron/go/neutroncli"
)

func main() {
	os.Exit(neutroncli.Run())
}
