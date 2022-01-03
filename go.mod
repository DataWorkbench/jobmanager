module github.com/DataWorkbench/jobmanager

go 1.15

require (
	github.com/DataWorkbench/common v0.0.0-20220101124902-b2a749288a1e
	github.com/DataWorkbench/glog v0.0.0-20210809050640-4960fd6de6ab
	github.com/DataWorkbench/gproto v0.0.0-20211231143935-cc4425b8f334
	github.com/DataWorkbench/loader v0.0.0-20201119073611-6f210eb11a8c
	github.com/go-playground/validator/v10 v10.4.1
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v1.1.1
	github.com/stretchr/testify v1.7.0
	google.golang.org/grpc v1.38.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	gorm.io/gorm v1.20.6
)

replace (
	github.com/DataWorkbench/common v0.0.0-20220101124902-b2a749288a1e => /Users/apple/develop/go/src/tmp/common
	github.com/DataWorkbench/gproto v0.0.0-20211231143935-cc4425b8f334 => /Users/apple/develop/go/src/tmp/gproto
)
