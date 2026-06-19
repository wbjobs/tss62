package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"logmonitor/internal/alert"
	"logmonitor/internal/collector"
	"logmonitor/internal/config"
	"logmonitor/internal/notifier"
	redisstore "logmonitor/internal/redis"
	"logmonitor/internal/rule"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	var (
		engine    *rule.Engine
		reloadMu  sync.Mutex
	)

	reloadFn := func(cfg *config.AppConfig) {
		reloadMu.Lock()
		defer reloadMu.Unlock()
		engine.Reload(cfg)
		log.Println("rules reloaded")
	}

	cfgLoader, err := config.NewConfigLoader(*configPath, reloadFn)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}
	defer cfgLoader.Close()

	cfg := cfgLoader.Get()

	store, err := redisstore.NewStore(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("init redis store failed: %v", err)
	}
	defer store.Close()

	engine = rule.NewEngine(cfg)
	alertMgr := alert.NewManager()

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		for range ch {
			reloadFn(cfgLoader.Get())
		}
	}()

	collectorSrv := collector.NewServer(engine, store, alertMgr, 128, 65536)
	notifierSrv := notifier.NewServer(alertMgr)

	errCh := make(chan error, 2)
	go func() {
		errCh <- collectorSrv.Start(cfg.CollectorPort)
	}()
	go func() {
		errCh <- notifierSrv.Start(cfg.AdminPort)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		log.Fatalf("server error: %v", err)
	case sig := <-sigCh:
		log.Printf("received signal %v, shutting down", sig)
	}
}
