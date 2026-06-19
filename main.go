package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"logmonitor/internal/alert"
	"logmonitor/internal/clientmgr"
	"logmonitor/internal/collector"
	"logmonitor/internal/config"
	"logmonitor/internal/notifier"
	"logmonitor/internal/notify"
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

	dingTalkCfgs := make([]notify.DingTalkConfig, 0, len(cfg.DingTalk))
	for _, d := range cfg.DingTalk {
		dingTalkCfgs = append(dingTalkCfgs, notify.DingTalkConfig{
			WebhookURL: d.WebhookURL,
			Secret:     d.Secret,
		})
	}
	weChatCfgs := make([]notify.WeChatConfig, 0, len(cfg.WeChat))
	for _, w := range cfg.WeChat {
		weChatCfgs = append(weChatCfgs, notify.WeChatConfig{
			WebhookURL: w.WebhookURL,
		})
	}
	notifyMgr := notify.NewManager(dingTalkCfgs, weChatCfgs)
	defer notifyMgr.Close()
	alertMgr.SetNotifier(notifyMgr)

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		for range ch {
			reloadFn(cfgLoader.Get())
		}
	}()

	clientMgr := clientmgr.NewManager()

	collectorSrv := collector.NewServer(engine, store, alertMgr, clientMgr, 128, 65536)
	notifierSrv := notifier.NewServer(alertMgr)

	errCh := make(chan error, 3)
	go func() {
		errCh <- collectorSrv.Start(cfg.CollectorPort)
	}()
	go func() {
		errCh <- notifierSrv.Start(cfg.AdminPort)
	}()
	go func() {
		if cfg.ManagerPort > 0 {
			log.Printf("manager api listening on :%d", cfg.ManagerPort)
			errCh <- clientMgr.Start(cfg.ManagerPort)
		}
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
