package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	// **Important** This imports must be found within the project somewhere, this pulls
	// chain specific global configuration for bstream for proper decoding of project. If
	// this import is missing, you will hit quite a few weird errors
	_ "github.com/streamingfast/firehose-ethereum/types"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/dstore"
	pbeth "github.com/streamingfast/firehose-ethereum/types/pb/sf/ethereum/type/v2"
)

func main() {
	cli.Ensure(len(os.Args) > 1, usage())

	store, err := dstore.NewDBinStore(os.Args[1])
	cli.NoError(err, "Unable to create merged blocks store")

	fmt.Fprintf(os.Stderr, "Walking all files in %s\n", store.BaseURL().String())
	store.Walk(context.Background(), "", func(filename string) (err error) {
		fmt.Fprintf(os.Stderr, "Processing file %s\n", filename)
		err = printBlocksFromFile(context.Background(), store, filename)
		cli.NoError(err, "Reand and print blocks failed")

		return nil
	})
	fmt.Fprintln(os.Stderr, "Done")
}

func printBlocksFromFile(ctx context.Context, store dstore.Store, path string) error {
	rawReader, err := store.OpenObject(ctx, path)
	if err != nil {
		return fmt.Errorf("open object %q: %w", path, err)
	}
	defer rawReader.Close()

	blockReader, err := bstream.GetBlockReaderFactory.New(rawReader)
	if err != nil {
		return fmt.Errorf("new block reader: %w", err)
	}

	for {
		rawBlock, err := blockReader.Read()
		if rawBlock != nil {
			block := rawBlock.ToProtocol().(*pbeth.Block)
			asJson, err := json.MarshalIndent(block, "", " ")
			if err != nil {
				return fmt.Errorf("marshal block: %w", err)
			}

			fmt.Fprintf(os.Stderr, "Printing block %s\n", block.AsRef())
			fmt.Println(string(asJson))
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

func usage() string {
	return "usage: go run . <merged_blocks_store>"
}
