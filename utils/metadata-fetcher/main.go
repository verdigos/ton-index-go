package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/semaphore"
	"io"
	"log"
	"math"
	"net/http"
	"time"
)

var gate *semaphore.Weighted
var client *http.Client

var max_retries int
var initial_backoff time.Duration
var backoff_multiplier float64
var max_backoff time.Duration
var stalled_task_interval time.Duration

type BackgroundTask struct {
	Id    int64
	Type  string
	Retry int
	Data  map[string]interface{}
}

type FetchTask struct {
	Address string
	Type    string
	Retry   int
	TaskId  int64
}

type AddressMetadata struct {
	Address     *string
	Type        *string
	Name        *string
	Symbol      *string
	Description *string
	Image       *string
	Extra       map[string]interface{}
}

func (receiver AddressMetadata) hasAnyData() bool {
	return receiver.Name != nil || receiver.Description != nil || receiver.Image != nil
}

func fetchTasks(ctx context.Context, pool *pgxpool.Pool) ([]FetchTask, error) {
	// Acquire a connection from the pool
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %v", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, `
        SELECT id, type, data, retries  FROM background_tasks
        WHERE status = 'ready'
        AND type = 'fetch_metadata' AND retries <= $1
        AND (retry_at <= EXTRACT(EPOCH FROM NOW())::bigint OR retry_at is NULL)
        LIMIT 100
    `, max_retries)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tasks: %v", err)
	}
	defer rows.Close()

	var tasks []FetchTask
	for rows.Next() {
		var task BackgroundTask
		if err := rows.Scan(&task.Id, &task.Type, &task.Data, &task.Retry); err != nil {
			return nil, fmt.Errorf("failed to scan task: %v", err)
		}
		tasks = append(tasks, FetchTask{
			TaskId:  task.Id,
			Type:    task.Data["type"].(string),
			Address: task.Data["address"].(string),
			Retry:   task.Retry,
		})
	}
	return tasks, nil
}

func getMetadata(ctx context.Context, tx pgx.Tx, task FetchTask) (map[string]interface{}, error) {
	var metadata_bytes []byte
	var field_name string
	switch task.Type {
	case "nft_collections":
		field_name = "collection_content"
	case "nft_items":
		field_name = "content"
	case "jetton_masters":
		field_name = "jetton_content"
	}
	query := fmt.Sprintf("SELECT %s as metadata FROM %s WHERE address = $1", field_name, task.Type)
	err := tx.QueryRow(ctx, query, task.Address).Scan(&metadata_bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %v", err)
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(metadata_bytes, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
	}
	return metadata, nil
}

// extractURL extracts the 'url' or 'uri' from the metadata.
func extractURL(metadata map[string]interface{}) (string, error) {
	if url, ok := metadata["url"].(string); ok {
		return url, nil
	}
	if uri, ok := metadata["uri"].(string); ok {
		return uri, nil
	}
	return "", fmt.Errorf("no 'url' or 'uri' found in metadata")
}

// completeTask removes the task from the tasks table.
func completeTask(ctx context.Context, tx pgx.Tx, task FetchTask) error {
	query := "DELETE FROM background_tasks WHERE id = $1"
	_, err := tx.Exec(ctx, query, task.TaskId)
	if err != nil {
		return fmt.Errorf("failed to delete task: %v", err)
	}
	return nil
}

func getMetadataFromJson(metadata map[string]interface{}) AddressMetadata {
	var result AddressMetadata
	for key := range metadata {
		if value, ok := metadata[key].(string); ok {
			switch key {
			case "name":
				if result.Name == nil {
					result.Name = new(string)
				}
				*result.Name = value
			case "description":
				if result.Description == nil {
					result.Description = new(string)
				}
				*result.Description = value
			case "image":
				if result.Image == nil {
					result.Image = new(string)
				}
				*result.Image = value
			case "symbol":
				if result.Symbol == nil {
					result.Symbol = new(string)
				}
				*result.Symbol = value
			default:
				if result.Extra == nil {
					result.Extra = make(map[string]interface{})
				}
				result.Extra[key] = value
			}
		}
	}

	return result
}

func fetchContent(metadata map[string]interface{}) (AddressMetadata, error) {
	url, err := extractURL(metadata)
	if err != nil {
		metadata_from_db := getMetadataFromJson(metadata)
		if metadata_from_db.hasAnyData() {
			return metadata_from_db, nil
		} else {
			return AddressMetadata{}, fmt.Errorf("failed to extract URL or required data: %v", err)
		}
	}

	resp, err := client.Get(url)
	if err != nil {
		return AddressMetadata{}, fmt.Errorf("failed to fetch content from URL: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return AddressMetadata{}, fmt.Errorf("non-OK HTTP status: %s", resp.Status)
	}

	body_bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return AddressMetadata{}, fmt.Errorf("failed to read response body: %v", err)
	}

	var content map[string]interface{}
	if err := json.Unmarshal(body_bytes, &content); err != nil {
		return AddressMetadata{}, fmt.Errorf("failed to unmarshal response body: %v", err)
	}
	return getMetadataFromJson(content), nil
}

func processTask(ctx context.Context, pool *pgxpool.Pool, task FetchTask) (taskError error) {
	defer gate.Release(1)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %v", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer func() {
		if taskError != nil {
			_ = tx.Rollback(ctx)
		} else {
			_ = tx.Commit(ctx)
		}
	}()

	// Process the task within the transaction
	metadata, err := getMetadata(ctx, tx, task)
	if err != nil {
		log.Printf("Error getting metadata for task %v: %v", task, err)
		return handleTaskFailure(ctx, tx, task, err)
	}

	content, err := fetchContent(metadata)
	if err != nil {
		log.Printf("Error fetching content for task %v: %v", task, err)
		return handleTaskFailure(ctx, tx, task, err)
	}

	_, err = tx.Exec(ctx, `INSERT INTO address_metadata (address, type, valid, name, description, image, symbol, extra, updated_at)
    							VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (address, type) DO UPDATE SET 
    							valid = $3, name = $4, description = $5, image = $6, symbol = $7, extra = $8, updated_at = $9`,
		task.Address, task.Type, true, content.Name, content.Description, content.Image, content.Symbol, content.Extra, time.Now().Unix())
	if err != nil {
		log.Printf("Error inserting metadata for task %v: %v", task, err)
		return handleTaskFailure(ctx, tx, task, err)
	}
	if err := completeTask(ctx, tx, task); err != nil {
		log.Printf("Error completing task %v: %v", task, err)
		return handleTaskFailure(ctx, tx, task, err)
	}
	return nil
}

func handleTaskFailure(ctx context.Context, tx pgx.Tx, task FetchTask, taskErr error) error {
	delay := calculateBackoffDelay(task.Retry)

	if task.Retry < max_retries {
		_, err := tx.Exec(ctx, `
        UPDATE background_tasks
        SET status = 'ready',
            retry_at = EXTRACT(EPOCH FROM NOW())::bigint + $1,
        	retries = retries + 1,
            error = $3
        WHERE id = $2
    `, int64(delay.Seconds()), task.TaskId, taskErr.Error())
		if err != nil {
			log.Printf("Error updating retry_at for task %v: %v", task, err)
			return err
		}
	} else {
		_, err := tx.Exec(ctx, `
		UPDATE background_tasks
		SET status = 'failed', error = $2, retries = retries + 1
		WHERE id = $1`, task.TaskId, taskErr.Error())
		if err != nil {
			log.Printf("Error updating status to failed for task %v: %v", task, err)
			return err
		}
	}
	extra := map[string]interface{}{
		"error": taskErr.Error(),
	}
	extra_json, _ := json.Marshal(extra)

	_, err := tx.Exec(ctx, `INSERT INTO address_metadata (address, type, valid, name, description, image, symbol, extra, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (address, type) DO UPDATE SET valid = $3`,
		task.Address, task.Type, false, nil, nil, nil, nil, extra_json, time.Now().Unix())
	if err != nil {
		log.Printf("Error inserting metadata for failed task %v: %v", task, err)
		return err
	}
	return nil
}

func calculateBackoffDelay(retry int) time.Duration {
	delay := initial_backoff * time.Duration(math.Pow(backoff_multiplier, float64(retry-1)))
	if delay > max_backoff {
		delay = max_backoff
	}
	return delay
}

func initializeDb(ctx context.Context, pgDsn string, processes int) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(pgDsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %v", err)
	}
	// Set maximum connections in the pool
	config.MaxConns = max(int32(processes)*2, 4)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %v", err)
	}
	return pool, nil
}

func updateStalledTasks(ctx context.Context, pool *pgxpool.Pool) {
	for {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			log.Printf("failed to acquire connection: %v", err)
		}

		_, err = conn.Exec(ctx, `
            UPDATE background_tasks
            SET status = 'ready',
                retry_at =  EXTRACT(EPOCH FROM NOW())::bigint + LEAST(
                    $1 * POWER($2, retries - 1),
                    $3
                )
            WHERE type = 'fetch_metadata' AND status = 'in_progress' AND started_at < EXTRACT(EPOCH FROM NOW())::bigint - $4`,
			int64(initial_backoff.Seconds()), backoff_multiplier, int64(max_backoff.Seconds()), stalled_task_interval)
		if err != nil {
			log.Print("failed to update stalled tasks: ", err)
		}
		conn.Release()
		time.Sleep(time.Minute)
	}
}

func main() {
	var pg_dsn string
	var processes int
	flag.StringVar(&pg_dsn, "pg", "postgresql://localhost:5432", "PostgreSQL connection string")
	flag.IntVar(&processes, "processes", 32, "Set number of parallel queries")
	flag.DurationVar(&initial_backoff, "initial-backoff", 5*time.Second, "Initial backoff duration")
	flag.Float64Var(&backoff_multiplier, "backoff-multiplier", 2, "Backoff multiplier")
	flag.DurationVar(&max_backoff, "max-backoff", 5*time.Minute, "Maximum backoff duration")
	flag.IntVar(&max_retries, "max-retries", 5, "Maximum number of retries")
	flag.DurationVar(&stalled_task_interval, "stalled-task-interval", 5*time.Minute,
		"Interval to update stalled tasks")
	flag.Parse()

	gate = semaphore.NewWeighted(int64(processes))
	client = &http.Client{
		Timeout: 30 * time.Second,
	}
	ctx := context.Background()
	pool, err := initializeDb(ctx, pg_dsn, processes)

	if err != nil {
		log.Fatal("Error initializing database connection: ", err)
	}
	defer pool.Close()
	go updateStalledTasks(ctx, pool)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatal("failed to acquire connection: ", err)
	}
	defer conn.Release()
	for {
		tasks, err := fetchTasks(ctx, pool)
		if err != nil {
			log.Println("Error fetching tasks: ", err)
			time.Sleep(time.Second)
			continue
		}

		for _, task := range tasks {
			err = gate.Acquire(ctx, 1)
			if err != nil {
				log.Printf("failed to acquire worker: %s\n", err.Error())
				continue
			}
			_, err := conn.Exec(ctx, `UPDATE background_tasks
					SET status = 'in_progress',
						started_at = EXTRACT(EPOCH FROM NOW())::bigint
					WHERE id = $1`,
				task.TaskId)
			if err != nil {
				log.Println("Error updating task status: ", err)
				gate.Release(1)
				continue
			}

			go processTask(ctx, pool, task)
		}
		time.Sleep(time.Second)
	}

}