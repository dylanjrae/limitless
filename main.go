package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"cloud.google.com/go/bigtable"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

type jsonrpcRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type jsonrpcErrorResponse struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      *int          `json:"id,omitempty"`
	Error   *jsonrpcError `json:"error,omitempty"`
}

type jsonrpcResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      *int        `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
}

func base10ToHex(base10Str string) (string, error) {
	base10Int, err := strconv.ParseInt(base10Str, 10, 64)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(base10Int, 16), nil
}

func handleChainIdResponse(c *gin.Context, req *jsonrpcRequest) (jsonrpcResponse, error) {
	chainID := c.Param("chain_id")
	hexChainID, err := base10ToHex(chainID)
	if err != nil {
		return jsonrpcResponse{}, err
	}

	response := jsonrpcResponse{
		Jsonrpc: "2.0",
		ID:      &req.ID,
		Result:  "0x" + hexChainID,
	}

	return response, nil
}

func handleBlockNumberResponse(c *gin.Context, req *jsonrpcRequest, bigTableClient *bigtable.Client) (jsonrpcResponse, error) {
	chainID := c.Param("chain_id")

	blockNumber, err := fetchLatestBlockNumber(bigTableClient, chainID)
	if err != nil {
		return jsonrpcResponse{}, err
	}

	blockNumberHex, err := base10ToHex(blockNumber)
	if err != nil {
		return jsonrpcResponse{}, err
	}

	response := jsonrpcResponse{
		Jsonrpc: "2.0",
		ID:      &req.ID,
		Result:  "0x" + blockNumberHex,
	}

	return response, nil
}

func handleGetLogsResponse(c *gin.Context, req *jsonrpcRequest, bigTableClient *bigtable.Client) (jsonrpcResponse, error) {
	// chainID := c.Param("chain_id")
	//get params from request
	//fetch logs from bigtable
	//format logs
	//return json
	return jsonrpcResponse{}, nil
}

func fetchLatestBlockNumber(client *bigtable.Client, chainID string) (string, error) {
	bigTableContext := context.Background()

	tbl := client.Open("blockbynumber")

	// Create a filter that only accepts the latest version of each cell.
	filter := bigtable.LatestNFilter(1)

	// Read all rows.
	var latestBlockNumber uint64
	err := tbl.ReadRows(bigTableContext, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		// The row key is the block number in big-endian format.
		blockNumberBytes := []byte(row.Key())

		// Convert the big-endian byte slice back to an integer.
		blockNumber := binary.BigEndian.Uint64(blockNumberBytes)

		// Update the latest block number.
		if blockNumber > latestBlockNumber {
			latestBlockNumber = blockNumber
		}

		// Continue to the next row.
		return true
	}, bigtable.RowFilter(filter))
	if err != nil {
		return "", err
	}

	return strconv.FormatUint(latestBlockNumber, 10), nil
}

func setupBigTable() {
	ctx := context.Background()

	adminClient, err := bigtable.NewAdminClient(ctx, "project-id", "instance-id")
	if err != nil {
		log.Fatalf("Could not create admin client: %v", err)
	}

	tableNames := []string{"blockbyhash", "blockbynumber"}

	for _, tableName := range tableNames {
		// Check if the table already exists.
		tables, err := adminClient.Tables(ctx)
		if err != nil {
			log.Fatalf("Could not fetch table list: %v", err)
		}

		exists := false
		for _, table := range tables {
			if table == tableName {
				exists = true
				break
			}
		}

		// If the table doesn't exist, create it.
		if !exists {
			if err := adminClient.CreateTable(ctx, tableName); err != nil {
				log.Fatalf("Could not create table %s: %v", tableName, err)
			}

			columnFamilyName := "cf"
			if err := adminClient.CreateColumnFamily(ctx, tableName, columnFamilyName); err != nil {
				log.Fatalf("Could not create column family %s: %v", columnFamilyName, err)
			}

			log.Printf("Table %s and column family %s created.", tableName, columnFamilyName)
		}
	}

	tables, err := adminClient.Tables(ctx)
	if err != nil {
		log.Fatalf("Could not fetch table list: %v", err)
	}

	fmt.Println("Current tables:")
	for _, table := range tables {
		log.Println(table)
	}
}

func addSampleData() {
	ctx := context.Background()

	client, err := bigtable.NewClient(ctx, "project-id", "instance-id")
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
	}

	blockData := []struct {
		blockHash   string
		blockNumber int
	}{
		{"0x28211d40bbe41fcc0db49d1349d34e491f0a0368d4d599314b658b238ed0c5d8", 1234},
		{"0x38211d40bbe41fcc0db49d1349d34e491f0a0368d4d599314b658b238ed0c5d9", 1235},
		{"0x48211d40bbe41fcc0db49d1349d34e491f0a0368d4d599314b658b238ed0c5da", 1236},
		{"0x58211d40bbe41fcc0db49d1349d34e491f0a0368d4d599314b658b238ed0c5db", 1237},
		{"0x68211d40bbe41fcc0db49d1349d34e491f0a0368d4d599314b658b238ed0c5dc", 1238},
	}

	for _, bd := range blockData {
		// Add a row to the "blockbyhash" table.
		blockHashTable := client.Open("blockbyhash")
		blockHashMut := bigtable.NewMutation()
		blockHashMut.Set("cf", "blockNumber", bigtable.Now(), []byte(strconv.Itoa(bd.blockNumber)))
		if err := blockHashTable.Apply(ctx, bd.blockHash, blockHashMut); err != nil {
			log.Fatalf("Could not apply mutation to blockbyhash table: %v", err)
		}
		log.Println("Successfully added blockHash: " + bd.blockHash + " to table: blockbyhash")

		// Add a row to the "blockbynumber" table.
		blockNumberTable := client.Open("blockbynumber")
		blockNumberMut := bigtable.NewMutation()
		blockNumberMut.Set("cf", "blockHash", bigtable.Now(), []byte(bd.blockHash))
		// Convert the block number to a big-endian byte slice.
		blockNumberBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(blockNumberBytes, uint64(bd.blockNumber))

		if err := blockNumberTable.Apply(ctx, string(blockNumberBytes), blockNumberMut); err != nil {
			log.Fatalf("Could not apply mutation to blockbynumber table: %v", err)
		}
		log.Println("Successfully added blockNumber: " + strconv.Itoa(bd.blockNumber) + " to table: blockbynumber")
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// setupBigTable()
	// addSampleData()

	ctx := context.Background()
	bigTableClient, err := bigtable.NewClient(ctx, "project-id", "instance-id")
	if err != nil {
		log.Fatalf("Could not create data operations client: %v", err)
	}

	r := gin.Default()

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "Hello, World!",
		})
	})

	r.POST("/:chain_id/jsonrpc", func(c *gin.Context) {
		var req jsonrpcRequest

		if err := c.BindJSON(&req); err != nil {
			response := jsonrpcErrorResponse{
				Jsonrpc: "2.0",
				Error: &jsonrpcError{
					Code:    -32600,
					Message: "invalid json request",
				},
			}
			c.Header("Content-Type", "application/json") //unsure why this is needed but it isn't getting set without
			c.JSON(http.StatusBadRequest, response)
			return
		}

		switch req.Method {
		case "eth_blockNumber":
			response, _ := handleBlockNumberResponse(c, &req, bigTableClient)
			c.JSON(http.StatusOK, response)

		case "eth_chainId":
			response, _ := handleChainIdResponse(c, &req)
			c.JSON(http.StatusOK, response)

		case "eth_getLogs":
			response, _ := handleGetLogsResponse(c, &req, bigTableClient)
			c.JSON(http.StatusOK, response)

		default:
			response := jsonrpcErrorResponse{
				Jsonrpc: "2.0",
				ID:      &req.ID,
				Error: &jsonrpcError{
					Code:    -32601,
					Message: fmt.Sprintf("The method %s does not exist/is not available", req.Method),
				},
			}
			c.JSON(http.StatusBadRequest, response)
		}
	})

	r.Run()
}