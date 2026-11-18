// Command adspace-relations reads an ad space and its child records from one snapshot.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	ID        int64      `json:"id"`
	Name      string     `json:"name"`
	Enabled   bool       `json:"enabled"`
	Creatives []Creative `json:"creatives"`
	Rules     []Rule     `json:"rules"`
}

type Creative struct {
	ID    int64  `json:"id"`
	Title string `json:"title"`
}

type Rule struct {
	ID     int64  `json:"id"`
	Region string `json:"region"`
}

func loadAdSpaces(db *sql.DB) tablecache.Loader[int64, AdSpace] {
	return func(ctx context.Context) (map[int64]AdSpace, error) {
		// All three InnoDB reads share the snapshot established by the first SELECT.
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			return nil, fmt.Errorf("begin snapshot read: %w", err)
		}
		defer tx.Rollback()

		spaces, err := readAdSpaces(ctx, tx)
		if err != nil {
			return nil, fmt.Errorf("read ad spaces: %w", err)
		}
		if err := readCreatives(ctx, tx, spaces); err != nil {
			return nil, fmt.Errorf("read creatives: %w", err)
		}
		if err := readRules(ctx, tx, spaces); err != nil {
			return nil, fmt.Errorf("read rules: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit snapshot read: %w", err)
		}
		return spaces, nil
	}
}

func readAdSpaces(ctx context.Context, tx *sql.Tx) (map[int64]AdSpace, error) {
	rows, err := tx.QueryContext(ctx, "SELECT id, name, enabled FROM demo_ad_spaces")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	spaces := make(map[int64]AdSpace)
	for rows.Next() {
		// Keep empty child collections as arrays in the JSON snapshot.
		space := AdSpace{Creatives: []Creative{}, Rules: []Rule{}}
		if err := rows.Scan(&space.ID, &space.Name, &space.Enabled); err != nil {
			return nil, err
		}
		spaces[space.ID] = space
	}
	return spaces, rows.Err()
}

func readCreatives(ctx context.Context, tx *sql.Tx, spaces map[int64]AdSpace) error {
	rows, err := tx.QueryContext(ctx, "SELECT ad_space_id, id, title FROM demo_ad_creatives ORDER BY ad_space_id, id")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var spaceID int64
		var creative Creative
		if err := rows.Scan(&spaceID, &creative.ID, &creative.Title); err != nil {
			return err
		}
		space, ok := spaces[spaceID]
		if !ok {
			return fmt.Errorf("creative %d refers to unknown ad space %d", creative.ID, spaceID)
		}
		space.Creatives = append(space.Creatives, creative)
		spaces[spaceID] = space
	}
	return rows.Err()
}

func readRules(ctx context.Context, tx *sql.Tx, spaces map[int64]AdSpace) error {
	rows, err := tx.QueryContext(ctx, "SELECT ad_space_id, id, region FROM demo_ad_rules ORDER BY ad_space_id, id")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var spaceID int64
		var rule Rule
		if err := rows.Scan(&spaceID, &rule.ID, &rule.Region); err != nil {
			return err
		}
		space, ok := spaces[spaceID]
		if !ok {
			return fmt.Errorf("rule %d refers to unknown ad space %d", rule.ID, spaceID)
		}
		space.Rules = append(space.Rules, rule)
		spaces[spaceID] = space
	}
	return rows.Err()
}

func run(id int64) error {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		return errors.New("set MYSQL_DSN to the DSN for the demo database")
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
	options, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	options.ContextTimeoutEnabled = true
	rdb := redis.NewClient(options)
	defer rdb.Close()

	cache, err := tablecache.New(rdb, tablecache.Options{
		Namespace: "demo:ad-space-relations:v1",
		OnError:   func(err error) { log.Print(err) },
	}, loadAdSpaces(db))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := cache.Run(ctx); err != nil {
			log.Print(err)
		}
	}()
	defer func() {
		cancel()
		<-done
	}()
	if err := cache.WaitReady(ctx); err != nil {
		return fmt.Errorf("wait for snapshot: %w", err)
	}

	space, ok := cache.Get(id)
	if !ok {
		return fmt.Errorf("ad space %d absent from snapshot", id)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(space)
}

func main() {
	id := flag.Int64("id", 6111, "ad space ID")
	flag.Parse()
	if err := run(*id); err != nil {
		log.Fatal(err)
	}
}
