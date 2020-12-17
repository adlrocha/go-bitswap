module github.com/ipfs/go-bitswap

require (
	contrib.go.opencensus.io/exporter/jaeger v0.2.1
	github.com/cskr/pubsub v1.0.2
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.5 //
	github.com/google/uuid v1.1.1
	github.com/ipfs/go-block-format v0.0.2
	github.com/ipfs/go-cid v0.0.5
	github.com/ipfs/go-datastore v0.4.4
	github.com/ipfs/go-detect-race v0.0.1
	github.com/ipfs/go-ipfs-blockstore v0.1.4
	github.com/ipfs/go-ipfs-blocksutil v0.0.1
	github.com/ipfs/go-ipfs-delay v0.0.1
	github.com/ipfs/go-ipfs-exchange-interface v0.0.1
	github.com/ipfs/go-ipfs-routing v0.1.0
	github.com/ipfs/go-ipfs-util v0.0.1
	github.com/ipfs/go-log v1.0.4
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/ipfs/go-peertaskqueue v0.2.0
	github.com/jbenet/goprocess v0.1.4
	github.com/libp2p/go-buffer-pool v0.0.2
	github.com/libp2p/go-libp2p v0.8.3
	github.com/libp2p/go-libp2p-core v0.5.2
	github.com/libp2p/go-libp2p-loggables v0.1.0
	github.com/libp2p/go-libp2p-netutil v0.1.0
	github.com/libp2p/go-libp2p-testing v0.1.1
	github.com/libp2p/go-msgio v0.0.4
	github.com/multiformats/go-multiaddr v0.2.1
	github.com/multiformats/go-multistream v0.1.1
	go.opencensus.io v0.22.4
	go.uber.org/zap v1.14.1
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	google.golang.org/appengine v1.6.5
)

// replace github.com/ipfs/go-bitswap => github.com/adlrocha/go-bitswap v0.2.19
go 1.12
