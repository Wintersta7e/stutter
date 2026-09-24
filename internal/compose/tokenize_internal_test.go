package compose

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestTokenizerFindsExactEndpoints(t *testing.T) {
	t.Parallel()

	const key = "VALUE"

	const (
		firstDB    = "db1"
		natsScheme = "nats"
		pgURL      = "postgres"
	)

	cases := []struct {
		name, key, value string
		userinfo         []string
		want             []ref
	}{
		{
			name: "pg URL with a host list", value: "postgres://u:p@db1:5432,db2:5433/app",
			userinfo: []string{"u:p"},
			want: []ref{
				{Scheme: pgURL, Host: firstDB, Port: 5432, List: 1},
				{Scheme: pgURL, Host: "db2", Port: 5433, List: 1},
			},
		},
		{
			name: "nats seed list", value: "nats://u:s3cr3t@bus1:4222,nats://bus2:4222",
			userinfo: []string{"u:s3cr3t", "s3cr3t"},
			want: []ref{
				{Scheme: natsScheme, Host: "bus1", Port: 4222, List: 1},
				{Scheme: natsScheme, Host: "bus2", Port: 4222, List: 1},
			},
		},
		{
			name: "two nats values", value: "nats://a:4222 nats://b:4222",
			want: []ref{
				{Scheme: natsScheme, Host: "a", Port: 4222, List: 1},
				{Scheme: natsScheme, Host: "b", Port: 4222, List: 2},
			},
		},
		{
			name: "jdbc", value: "jdbc:postgresql://db:5432/app",
			want: []ref{{Scheme: "postgresql", Host: "db", Port: 5432, List: 1, JDBC: true}},
		},
		{
			name: "libpq keywords", value: "host=db port=5433",
			want: []ref{{Host: "db", Port: 5433, KeywordPG: true}},
		},
		{
			name:  "libpq keywords with a quoted password and a host list",
			value: "host=db1,db2 port=5432,5433 password='pa ss' sslmode=verify-full", userinfo: []string{"pa ss"},
			want: []ref{
				{Host: firstDB, Port: 5432, KeywordPG: true, TLS: true},
				{Host: "db2", Port: 5433, KeywordPG: true, TLS: true},
			},
		},
		{
			name: "password holding @ : , /", value: "postgres://user:p@ss:w,rd/x@db:5432/app",
			userinfo: []string{"user:p@ss:w,rd/x", "p@ss:w,rd/x"},
			want:     []ref{{Scheme: pgURL, Host: "db", Port: 5432, List: 1}},
		},
		{
			name: "v6 literal", value: "postgres://app:hunter2@[fd00::5]:5432/db", userinfo: []string{"hunter2"},
			want: []ref{{Scheme: pgURL, Host: "fd00::5", Port: 5432, List: 1}},
		},
		{
			name: "sslmode", value: "postgresql://db/app?sslmode=require",
			want: []ref{{Scheme: "postgresql", Host: "db", List: 1, TLS: true}},
		},
		{
			name: "PGHOST", key: "PGHOST", value: firstDB + ",/run/postgresql",
			want: []ref{{Host: firstDB, KeywordPG: true}, {Host: "/run/postgresql", KeywordPG: true, Socket: true}},
		},
		{name: "PGPORT", key: "PGPORT", value: "5433", want: []ref{{Port: 5433, KeywordPG: true}}},
		{
			name: "socket host", value: "host=/var/run/postgresql dbname=app",
			want: []ref{{Host: "/var/run/postgresql", KeywordPG: true, Socket: true}},
		},
		{name: "bare host:port", value: "cache:6379", want: []ref{{Host: cacheService, Port: 6379}}},
		{name: "listen address", value: "0.0.0.0:8080", want: []ref{{Host: "0.0.0.0", Port: 8080}}},
		{
			name: "URL and bare token in one argument", value: "--listen=127.0.0.1:6000,http://api.test:8080/x",
			want: []ref{
				{Scheme: "http", Host: "api.test", Port: 8080, List: 1},
				{Host: "127.0.0.1", Port: 6000},
			},
		},
		{
			name: "rediss with a password only", value: "rediss://:pw0rd@cache:6380/0", userinfo: []string{"pw0rd"},
			want: []ref{{Scheme: "rediss", Host: cacheService, Port: 6380, List: 1}},
		},
		{name: "bare v6 literal", value: "fd00::7", want: []ref{{Host: "fd00::7"}}},
		{name: "not a reference", value: "hello world at 12:30; a=b"},
	}

	hits := 0

	for _, tc := range cases {
		name := key
		if tc.key != "" {
			name = tc.key
		}

		for index := range tc.want {
			tc.want[index].Key = name
		}

		got := references(name, tc.value)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: references(%q) =\n%+v\nwant\n%+v", tc.name, tc.value, got, tc.want)
		}

		printed := fmt.Sprintf("%+v %v %#v", got, got, got)
		for _, secret := range tc.userinfo {
			if strings.Contains(printed, secret) {
				hits++

				t.Errorf("%s: the result carries userinfo %q: %s", tc.name, secret, printed)
			}
		}
	}

	t.Logf("cases=%d userinfo-hits=%d", len(cases), hits)

	if len(cases) == 0 {
		t.Fatal("cases=0")
	}
}
