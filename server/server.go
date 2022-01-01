package server

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/gtrace"
	"github.com/DataWorkbench/common/metrics"
	"github.com/DataWorkbench/common/utils/buildinfo"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/pkg/jobpb"
	"github.com/DataWorkbench/jobmanager/config"
	"github.com/DataWorkbench/jobmanager/executor"
	"github.com/DataWorkbench/jobmanager/utils"
	"google.golang.org/grpc"
	"io"
	"os/signal"
	"syscall"

	"os"
	"time"
)

func Start() (err error) {
	fmt.Printf("%s pid=%d program_build_info: %s\n",
		time.Now().Format(time.RFC3339Nano), os.Getpid(), buildinfo.JSONString)

	var cfg *config.Config
	if cfg, err = config.Load(); err != nil {
		return
	}

	lp := glog.NewDefault().WithLevel(glog.Level(cfg.LogLevel))
	ctx := glog.WithContext(context.Background(), lp)

	var (
		rpcServer      *grpcwrap.Server
		metricServer   *metrics.Server
		tracer         gtrace.Tracer
		tracerCloser   io.Closer
		engineConn     *grpcwrap.ClientConn
		resourceConn   *grpcwrap.ClientConn
		udfConn        *grpcwrap.ClientConn
		engineClient   utils.EngineClient
		resourceClient utils.ResourceClient
		udfClient      utils.UdfClient
	)

	defer func() {
		rpcServer.GracefulStop()
		_ = metricServer.Shutdown(ctx)
		if tracerCloser != nil {
			_ = tracerCloser.Close()
		}
		_ = lp.Close()
	}()

	if tracer, tracerCloser, err = gtrace.New(cfg.Tracer); err != nil {
		return
	}

	if rpcServer, err = grpcwrap.NewServer(ctx, cfg.GRPCServer, grpcwrap.ServerWithTracer(tracer)); err != nil {
		return
	}

	if engineConn, err = grpcwrap.NewConn(ctx, cfg.EngineManagerServer, grpcwrap.ClientWithTracer(tracer)); err != nil {
		return
	}

	if engineClient, err = utils.NewEngineClient(engineConn); err != nil {
		return
	}

	if resourceConn, err = grpcwrap.NewConn(ctx, cfg.ResourceManagerServer, grpcwrap.ClientWithTracer(tracer)); err != nil {
		return
	}

	if resourceClient, err = utils.NewResourceClient(resourceConn); err != nil {
		return
	}

	if udfConn, err = grpcwrap.NewConn(ctx, cfg.UdfManagerServer, grpcwrap.ClientWithTracer(tracer)); err != nil {
		return
	}

	if udfClient, err = utils.NewUdfClient(udfConn); err != nil {
		return
	}

	rpcServer.Register(func(s *grpc.Server) {
		zeppelinConfig := zeppelin.ClientConfig{
			ZeppelinRestUrl: cfg.ZeppelinAddress,
			Timeout:         5000,
			RetryCount:      0,
			QueryInterval:   2000,
		}
		flinkConfig := flink.ClientConfig{
			Timeout:       5000,
			RetryCount:    0,
			QueryInterval: 0,
		}
		jobpb.RegisterJobmanagerServer(s, NewJobManagerServer(executor.NewJobManagerService(ctx, udfClient, engineClient, resourceClient,
			lp, zeppelinConfig, flinkConfig)))
	})

	sigGroup := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM}
	sigChan := make(chan os.Signal, len(sigGroup))
	signal.Notify(sigChan, sigGroup...)

	blockChan := make(chan struct{})
	go func() {
		err = rpcServer.ListenAndServe()
		blockChan <- struct{}{}
	}()

	if metricServer, err = metrics.NewServer(ctx, cfg.MetricsServer); err != nil {
		return
	}

	go func() {
		if err = metricServer.ListenAndServe(); err != nil {
			return
		}
	}()

	go func() {
		sig := <-sigChan
		lp.Info().String("receive system signal", sig.String()).Fire()
		blockChan <- struct{}{}
	}()

	<-blockChan
	return
}
