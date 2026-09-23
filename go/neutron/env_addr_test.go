package neutron

import (
	"net"
	"strconv"
	"strings"
	"testing"
)

// Contract §6: Run("") derives its address from NEUTRON_HOST / NEUTRON_PORT;
// an explicit address always wins; unset variables keep the old ":8080".
func TestListenAddrFromEnv(t *testing.T) {
	cases := []struct {
		name       string
		host, port string // "" = unset
		cfgAddr    string // "" = New()'s default config
		arg        string
		want       string
	}{
		{name: "unset keeps default", want: ":8080"},
		{name: "port only", port: "18181", want: ":18181"},
		{name: "host only", host: "127.0.0.1", want: "127.0.0.1:8080"},
		{name: "host and port", host: "127.0.0.1", port: "9001", want: "127.0.0.1:9001"},
		{name: "ipv6 host", host: "::1", port: "9001", want: "[::1]:9001"},
		{name: "bracketed ipv6 host", host: "[::1]", port: "9001", want: "[::1]:9001"},
		{name: "explicit arg beats env", host: "127.0.0.1", port: "9001", arg: ":7000", want: ":7000"},
		{name: "env overrides config port", port: "9100", cfgAddr: "127.0.0.1:9000", want: "127.0.0.1:9100"},
		{name: "config used when env unset", cfgAddr: "127.0.0.1:9000", want: "127.0.0.1:9000"},
		{name: "leading zero normalized", port: "08080", want: ":8080"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NEUTRON_HOST", tc.host)
			t.Setenv("NEUTRON_PORT", tc.port)
			var opts []Option
			if tc.cfgAddr != "" {
				opts = append(opts, WithConfig(&Config{Server: ServerConfig{Addr: tc.cfgAddr}}))
			}
			got, err := New(opts...).listenAddr(tc.arg)
			if err != nil {
				t.Fatalf("listenAddr: %v", err)
			}
			if got != tc.want {
				t.Errorf("listenAddr = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestListenAddrInvalidPort(t *testing.T) {
	for _, port := range []string{"abc", "0", "65536", "-1", "+80", "80x", "99999999999"} {
		t.Run(port, func(t *testing.T) {
			t.Setenv("NEUTRON_PORT", port)
			_, err := New().listenAddr("")
			if err == nil || !strings.Contains(err.Error(), "NEUTRON_PORT") {
				t.Fatalf("listenAddr with NEUTRON_PORT=%q: err = %v, want NEUTRON_PORT error", port, err)
			}
			// Run must fail the same way, before binding anything.
			if err := New().Run(""); err == nil || !strings.Contains(err.Error(), "NEUTRON_PORT") {
				t.Fatalf("Run(\"\") with NEUTRON_PORT=%q: err = %v", port, err)
			}
		})
	}
}

// Run("") must actually listen on NEUTRON_PORT: with that port occupied, Run
// returns the bind error instead of starting on :8080.
func TestRunBindsNeutronPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)

	t.Setenv("NEUTRON_HOST", "127.0.0.1")
	t.Setenv("NEUTRON_PORT", port)
	err = New().Run("")
	if err == nil {
		t.Fatal("Run returned nil; expected a bind error on the occupied port")
	}
	if !strings.Contains(err.Error(), "127.0.0.1:"+port) {
		t.Fatalf("Run error %q does not name 127.0.0.1:%s", err, port)
	}
}
