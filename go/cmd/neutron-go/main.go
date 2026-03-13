package main

import (
	"os"

	"github.com/neutron-dev/neutron-go/neutroncli"
)

func main() {
	os.Exit(neutroncli.Run())
}
