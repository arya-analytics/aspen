module github.com/arya-analytics/aspen

go 1.18

require (
	github.com/arya-analytics/x v0.0.0-20220516233935-c9dbaa7263d1
	github.com/cockroachdb/errors v1.8.1
	github.com/cockroachdb/pebble v0.0.0-20220513193540-b8c9a560bed5
	github.com/onsi/ginkgo/v2 v2.1.4
	github.com/onsi/gomega v1.19.0
	github.com/sirupsen/logrus v1.8.1
	go.uber.org/zap v1.21.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/grpc v1.35.0
	google.golang.org/protobuf v1.26.0
)

replace github.com/arya-analytics/x v0.0.0-20220516233935-c9dbaa7263d1 => ./x

require (
	github.com/DataDog/zstd v1.4.5 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/klauspost/compress v1.11.7 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kr/text v0.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f // indirect
	golang.org/x/sys v0.0.0-20220319134239-a9b59b0215f8 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20210226172003-ab064af71705 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
