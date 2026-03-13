// Package neutroncli implements the neutron-go CLI tool.
//
// Commands:
//   - neutron-go new <name>  — scaffold a new Neutron Go project
//   - neutron-go gen <schema.sql> — generate Go structs from SQL schema
//   - neutron-go dev — hot-reload development server (delegates to air)
//
// NOTE: The global 'neutron' command is the universal CLI (Go binary).
// Go-specific commands are invoked via 'neutron-go'.
package neutroncli

import (
	"fmt"
	"os"
)

const Version = "0.1.0"

func Run() int {
	if len(os.Args) < 2 {
		printUsage()
		return 1
	}

	switch os.Args[1] {
	case "new":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: neutron-go new <project-name>")
			return 1
		}
		return cmdNew(os.Args[2])
	case "gen":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: neutron-go gen <schema.sql>")
			return 1
		}
		return cmdGen(os.Args[2])
	case "dev":
		return cmdDev()
	case "version":
		fmt.Printf("neutron-go %s\n", Version)
		return 0
	case "help", "-h", "--help":
		printUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		return 1
	}
}

func printUsage() {
	fmt.Println(`Neutron Go CLI — the full-stack Go framework

Usage:
  neutron-go <command> [arguments]

Commands:
  new <name>         Create a new Neutron Go project
  gen <schema.sql>   Generate Go structs from SQL schema
  dev                Start development server with hot reload
  version            Print version
  help               Show this help

NOTE: The global 'neutron' command is the universal CLI.
Go-specific commands use 'neutron-go'.`)
}
