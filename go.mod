module github.com/tgulacsi/camproxy

require (
	github.com/go-kit/kit v0.7.0
	github.com/go-logfmt/logfmt v0.3.0 // indirect
	github.com/go-stack/stack v1.7.0 // indirect
	github.com/kr/logfmt v0.0.0-20140226030751-b84e30acd515 // indirect
	github.com/pkg/errors v0.8.0
	github.com/tgulacsi/camproxy/camutil v0.0.0-20180826070011-90374f165122
	perkeep.org v0.0.0-20180824152313-dd2d82c2500c
)

replace github.com/tgulacsi/camproxy/camutil v0.0.0-20180826070011-90374f165122 => ./camutil
