package server

import (
	"context"
	"fmt"
	"github.com/DataWorkbench/jobmanager/utils"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DataWorkbench/common/gormwrap"
	"github.com/DataWorkbench/common/gtrace"
	"github.com/DataWorkbench/common/utils/buildinfo"
	"github.com/DataWorkbench/glog"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/metrics"
	"github.com/DataWorkbench/gproto/pkg/jobpb"

	"github.com/DataWorkbench/common/functions"
	"github.com/DataWorkbench/jobmanager/config"
	"github.com/DataWorkbench/jobmanager/executor"
)

// Start for start the http server
func Start() (err error) {
	fmt.Printf("%s pid=%d program_build_info: %s\n",
		time.Now().Format(time.RFC3339Nano), os.Getpid(), buildinfo.JSONString)

	var cfg *config.Config

	cfg, err = config.Load()
	if err != nil {
		return
	}

	// init parent logger
	lp := glog.NewDefault().WithLevel(glog.Level(cfg.LogLevel))
	ctx := glog.WithContext(context.Background(), lp)

	var (
		db               *gorm.DB
		rpcServer        *grpcwrap.Server
		metricServer     *metrics.Server
		tracer           gtrace.Tracer
		tracerCloser     io.Closer
		jobdevConn       *grpcwrap.ClientConn
		jobdevClient     functions.JobdevClient
		engineConn       *grpcwrap.ClientConn
		engineClient     utils.EngineClient
		jobWatcherConn   *grpcwrap.ClientConn
		jobWatcherClient executor.JobWatcherClient
	)

	defer func() {
		rpcServer.GracefulStop()
		_ = metricServer.Shutdown(ctx)
		if tracerCloser != nil {
			_ = tracerCloser.Close()
		}
		_ = lp.Close()
	}()

	tracer, tracerCloser, err = gtrace.New(cfg.Tracer)
	if err != nil {
		return
	}

	// init gorm.DB
	db, err = gormwrap.NewMySQLConn(ctx, cfg.MySQL, gormwrap.WithTracer(tracer))
	if err != nil {
		return
	}

	// init grpc.Server
	rpcServer, err = grpcwrap.NewServer(ctx, cfg.GRPCServer, grpcwrap.ServerWithTracer(tracer))
	if err != nil {
		return
	}

	jobdevConn, err = grpcwrap.NewConn(ctx, cfg.JobDeveloperServer, grpcwrap.ClientWithTracer(tracer))
	if err != nil {
		return
	}

	jobdevClient, err = functions.NewJobdevClient(jobdevConn)
	if err != nil {
		return
	}

	engineConn, err = grpcwrap.NewConn(ctx, cfg.EngineManagerServer, grpcwrap.ClientWithTracer(tracer))
	if err != nil {
		return
	}

	engineClient, err = utils.NewEngineClient(engineConn)
	if err != nil {
		return
	}

	jobWatcherConn, err = grpcwrap.NewConn(ctx, cfg.JobWatcherServer, grpcwrap.ClientWithTracer(tracer))
	if err != nil {
		return
	}

	jobWatcherClient, err = executor.NewJobWatcherClient(jobWatcherConn)
	if err != nil {
		return
	}

	rpcServer.Register(func(s *grpc.Server) {
		jobpb.RegisterJobmanagerServer(s, NewJobManagerServer(executor.NewJobManagerExecutor(db, engineClient, jobdevClient, ctx, lp, jobWatcherClient, cfg.ZeppelinAddress)))
	})

	// handle signal
	sigGroup := []os.Signal{syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM}
	sigChan := make(chan os.Signal, len(sigGroup))
	signal.Notify(sigChan, sigGroup...)

	blockChan := make(chan struct{})

	// run grpc server
	go func() {
		err = rpcServer.ListenAndServe()
		blockChan <- struct{}{}
	}()

	// init prometheus server
	metricServer, err = metrics.NewServer(ctx, cfg.MetricsServer)
	if err != nil {
		return err
	}

	go func() {
		if err := metricServer.ListenAndServe(); err != nil {
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
