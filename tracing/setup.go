package tracing

import (
	"os"

	"contrib.go.opencensus.io/exporter/jaeger"
	"go.opencensus.io/trace"
)

func SetupJaegerTracing(serviceName string) *jaeger.Exporter {
	agentEndpointURI := os.Getenv("JAEGER_ENDPOINT")

	if agentEndpointURI == "" {
		agentEndpointURI = "localhost:6831"
	}

	je, err := jaeger.NewExporter(jaeger.Options{
		AgentEndpoint: agentEndpointURI,
		ServiceName:   serviceName,
	})
	if err != nil {
		panic(err)
	}
	trace.RegisterExporter(je)
	trace.ApplyConfig(trace.Config{
		DefaultSampler: trace.AlwaysSample(),
	})
	return je
}
