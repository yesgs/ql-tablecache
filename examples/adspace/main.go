// Command adspace reads one ad space from the first available shared snapshot.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/yesgs/ql-tablecache"
)

type AdSpace struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

func loadAdSpaces(db *sql.DB) tablecache.Loader[int64, AdSpace] {
	return func(ctx context.Context) (map[int64]AdSpace, error) {
		rows, err := db.QueryContext(ctx, "SELECT id, name, enabled FROM ad_space_config")
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		out := make(map[int64]AdSpace)
		for rows.Next() {
			var id int64
			var v AdSpace
			if err := rows.Scan(&id, &v.Name, &v.Enabled); err != nil {
				return nil, err
			}
			out[id] = v
		}
		return out, rows.Err()
	}
}

func run(id int64) error {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return fmt.Errorf("set MYSQL_DSN to the DSN for the config database")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	ro, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	ro.ContextTimeoutEnabled = true
	rdb := redis.NewClient(ro)
	defer rdb.Close()

	cache, err := tablecache.New(rdb, tablecache.Options{
		Namespace: "demo:ad-space:v1",
		OnError:   func(err error) { log.Print(err) },
	}, loadAdSpaces(db))
	if err != nil {
		return err
	}
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cache.Run(runCtx) }()
	defer func() { stop(); <-done }()

	startCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := cache.WaitReady(startCtx); err != nil {
		return err
	}
	item, ok := cache.Get(id)
	if !ok {
		return fmt.Errorf("ad space %d absent from snapshot", id)
	}
	fmt.Printf("id=%d name=%q enabled=%t\n", id, item.Name, item.Enabled)
	return nil
}

func main() {
	id := flag.Int64("id", 1, "ad space ID")
	flag.Parse()
	if err := run(*id); err != nil {
		log.Fatal(err)
	}
}
