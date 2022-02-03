package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gorm.io/gorm"

	"github.com/DataWorkbench/common/flink"
	"github.com/DataWorkbench/common/getcd"
	"github.com/DataWorkbench/common/gormwrap"
	"github.com/DataWorkbench/common/grpcwrap"
	"github.com/DataWorkbench/common/gtrace"
	"github.com/DataWorkbench/common/metrics"
	"github.com/DataWorkbench/common/utils/buildinfo"
	"github.com/DataWorkbench/common/zeppelin"
	"github.com/DataWorkbench/glog"
	"github.com/DataWorkbench/gproto/xgo/service/pbsvcdeveloper"
	"github.com/DataWorkbench/jobmanager/config"
	"github.com/DataWorkbench/jobmanager/service"
	"github.com/DataWorkbench/jobmanager/utils"
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
		db             *gorm.DB
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
		etcdClient     *getcd.Client
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
	ctx = gtrace.ContextWithTracer(ctx, tracer)

	db, err = gormwrap.NewMySQLConn(ctx, cfg.MySQL)
	if err != nil {
		return
	}

	if rpcServer, err = grpcwrap.NewServer(ctx, cfg.GRPCServer); err != nil {
		return
	}

	if engineConn, err = grpcwrap.NewConn(ctx, cfg.EngineManagerServer); err != nil {
		return
	}

	if engineClient, err = utils.NewEngineClient(engineConn); err != nil {
		return
	}

	if resourceConn, err = grpcwrap.NewConn(ctx, cfg.ResourceManagerServer); err != nil {
		return
	}

	if resourceClient, err = utils.NewResourceClient(resourceConn); err != nil {
		return
	}

	if udfConn, err = grpcwrap.NewConn(ctx, cfg.UdfManagerServer); err != nil {
		return
	}

	if udfClient, err = utils.NewUdfClient(udfConn); err != nil {
		return
	}

	flinkClient := flink.NewClient(ctx, nil)

	zeppelinClient := zeppelin.NewClient(ctx, nil, cfg.ZeppelinAddress)

	//if etcdClient, err = getcd.NewClient(ctx, cfg.ETCD); err != nil {
	//	return
	//}

	jobSvc := NewJobManagerServer(service.NewJobManagerService(ctx, db, udfClient, engineClient,
		resourceClient, flinkClient, zeppelinClient, etcdClient))

	rpcServer.RegisterService(&pbsvcdeveloper.JobManage_ServiceDesc, jobSvc)

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
