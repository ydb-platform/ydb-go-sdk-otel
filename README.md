# ydb_opentelemetry adapter

Opentelemetry traces over ydb-go-sdk events 

## Usage
```go
import (
    "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/trace"
    jaegerConfig "github.com/uber/jaeger-client-go/config"
	
    ydbOtel "github.com/ydb-platform/ydb-go-sdk-otel"
)

...
    // init jaeger client
    tracer, closer, err := jaegerConfig.Configuration{
        ServiceName: serviceName,
        Sampler: &jaegerConfig.SamplerConfig{
            Type:  "const",
            Param: 1,
        },
        Reporter: &jaegerConfig.ReporterConfig{
            LogSpans:            true,
            BufferFlushInterval: 1 * time.Second,
            LocalAgentHostPort:  tracerURL,
        },
    }.NewTracer()
    if err != nil {
        panic(err)
    }

    db, err := ydb.Open(
        ctx,
        os.Getenv("YDB_CONNECTION_STRING"),
        ydbOtel.WithTraces(
            tracer,
            ydbOtel.WithDetails(trace.DetailsAll),
        ),
    )

```
