package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/guregu/dynamo"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/errgroup"
)

var (
	optTable          = flag.String("table", "", "table name to load to")
	optPartitionKey   = flag.String("partition-key", "", "prefix of partition key")
	optSplitPartition = flag.Int64("split-partition", 10, "number of partitions to split")
	optSequence       = flag.Int64("sequence", 0, "sequence number to start from")
	optPutWorkers     = flag.Int("put-workers", 10, "number of workers to put items")
	optVersion        = flag.Bool("version", false, "show version")
)

var version string

type Record struct {
	Code      string `dynamo:"code"`
	Content   string `dynamo:"content"`
	Seq       int64  `dynamo:"seq"`
	CreatedAt int64  `dynamo:"created_at"`
	UpdatedAt int64  `dynamo:"updated_at"`
}

func main() {
	flag.Parse()

	if *optVersion {
		fmt.Println(version)
		return
	}

	if err := run(); err != nil {
		log.Fatalf("*** %v", err)
	}
}

func run() error {
	if *optTable == "" {
		return errors.New("--table must be specified")
	}
	if *optPartitionKey == "" {
		return errors.New("--partition-key must be specified")
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	cli := dynamo.NewFromIface(dynamodb.New(sess))

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	from := time.Now()
	seq := *optSequence

	chPut := make(chan Record, *optPutWorkers*2)
	egPut, ctxPut := errgroup.WithContext(ctx)
	for i := 0; i < *optPutWorkers; i++ {
		i := i
		ctx := ctxPut
		egPut.Go(func() (retErr error) {
			defer func() {
				if retErr != nil {
					cancel()
				}
				log.Printf("[%d]put: done: err=%v", i, retErr)
			}()

			// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
			// up to 25 operations per request
			recs := make([]interface{}, 0, 25)
			flush := func() error {
				if len(recs) == 0 {
					return nil
				}
				_, err := cli.Table(*optTable).Batch().Write().Put(recs...).RunWithContext(ctx)
				if err != nil {
					return err
				}
				recs = recs[:0]
				return nil
			}

		loop:
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case e, ok := <-chPut:
					if !ok {
						break loop
					}
					recs = append(recs, e)
					if len(recs) == cap(recs) {
						if err := flush(); err != nil {
							return err
						}
					}
				}
			}
			return flush()
		})
	}

	var retErr error
	for _, fn := range flag.Args() {
		if err := func() error {
			log.Printf("loading %s", fn)
			fp, err := os.Open(fn)
			if err != nil {
				return fmt.Errorf("os.Open: %w", err)
			}
			defer fp.Close()

			r := csv.NewReader(fp)
			r.Comma = '\t'

			for i := 0; ; i++ {
				record, err := r.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("Read: %w", err)
				}

				if i == 0 {
					// skip header
					continue
				}

				content := record[1]

				nextSeq := atomic.AddInt64(&seq, 1)
				suffix := strconv.FormatInt(nextSeq%*optSplitPartition, 10)
				pk := *optPartitionKey + ":" + suffix
				now := time.Now().Unix()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case chPut <- Record{
					Code:      pk,
					Seq:       nextSeq,
					Content:   content,
					CreatedAt: now,
					UpdatedAt: now,
				}:
				}

				if nextSeq%1000 == 0 {
					log.Printf("%s: Posted %d recs", fn, nextSeq)
				}
			}

			return nil
		}(); err != nil {
			retErr = multierror.Append(retErr, err)
			// eg.Goしているgoroutineを回収したいのでreturnしないで継続
			break
		}
	}

	close(chPut)
	log.Printf("waiting for put workers done")
	if err := egPut.Wait(); err != nil {
		retErr = multierror.Append(retErr, fmt.Errorf("egPut.Wait: %w", err))
	}

	if retErr != nil {
		return retErr
	}

	log.Printf("dur=%s", time.Since(from))

	return nil
}
