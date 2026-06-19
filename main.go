package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
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

	cfgLoader, err := config.NewConfigLoader(*configPath, func(cfg *config.AppConfig) {
		log.Println("config reloaded")
	})
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

	engine := rule.NewEngine(cfg)
	alertMgr := alert.NewManager()

	alertRules := make(map[string][3]int)
	for _, r := range cfg.Rules {
		if !r.Enabled {
			continue
		}
		cooldown := r.AlertConfig.CooldownSeconds
		if cooldown <= 0 {
			cooldown = 30
		}
		alertRules[r.ID] = [3]int{
			r.AlertConfig.WindowSeconds,
			r.AlertConfig.Threshold,
			cooldown,
		}
	}
	alertMgr.ReloadRules(alertRules)

	cfgLoader2 := cfgLoader
	engine2 := engine
	alertMgr2 := alertMgr
	_ = cfgLoader2
	_ = engine2
	_ = alertMgr2

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		for range ch {
			c := cfgLoader.Get()
			engine.Reload(c)
			ar := make(map[string][3]int)
			for _, r := range c.Rules {
				if !r.Enabled {
					continue
				}
				cooldown := r.AlertConfig.CooldownSeconds
				if cooldown <= 0 {
					cooldown = 30
				}
				ar[r.ID] = [3]int{
					r.AlertConfig.WindowSeconds,
					r.AlertConfig.Threshold,
					cooldown,
				}
			}
			alertMgr.ReloadRules(ar)
			log.Println("rules reloaded via SIGHUP")
		}
	}()

	collectorSrv := collector.NewServer(engine, store, alertMgr, 128)
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
