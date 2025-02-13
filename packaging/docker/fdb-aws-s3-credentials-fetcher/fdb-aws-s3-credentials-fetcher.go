package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
)

type BlobCredentials struct {
    Accounts map[string]Account `json:"accounts"`
}

type Account struct {
    Secret  string `json:"secret"`
    APIKey  string `json:"api_key"`
    Token   string `json:"token"`
}

func writeCredentialsFile(region, credFile string, accessKey, secretKey, token string) error {
    s3Endpoint := fmt.Sprintf("s3.%s.amazonaws.com", region)
    
    blobCreds := BlobCredentials{
        Accounts: map[string]Account{
            "@" + s3Endpoint: {
                Secret:  secretKey,
                APIKey:  accessKey,
                Token:   token,
            },
            "@" + s3Endpoint + ":443": {
                Secret:  secretKey,
                APIKey:  accessKey,
                Token:   token,
            },
        },
    }

    data, err := json.Marshal(blobCreds)
    if err != nil {
        return fmt.Errorf("failed to marshal credentials: %v", err)
    }

    if err := os.WriteFile(credFile, data, 0644); err != nil {
        return fmt.Errorf("failed to write credentials file: %v", err)
    }

    return nil
}

func refreshCredentials(region, credFile string) error {
    ctx := context.Background()
    
    // Load the AWS SDK configuration - this will automatically use IRSA when running in EKS
    cfg, err := config.LoadDefaultConfig(ctx, 
        config.WithRegion(region),
    )
    if err != nil {
        return fmt.Errorf("unable to load SDK config: %v", err)
    }

    // Get current credentials
    creds, err := cfg.Credentials.Retrieve(ctx)
    if err != nil {
        return fmt.Errorf("failed to get credentials: %v", err)
    }

    // Write credentials to file
    if err := writeCredentialsFile(region, credFile, creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken); err != nil {
        return fmt.Errorf("failed to write credentials: %v", err)
    }

    return nil
}

func main() {
    // Add usage message
    flag.Usage = func() {
        fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "A credential fetcher for AWS S3 that continuously refreshes credentials for FDB blob storage.\n\n")
        fmt.Fprintf(os.Stderr, "Options:\n")
        flag.PrintDefaults()
    }

    region := flag.String("region", "us-west-2", "AWS region for S3 endpoint")
    dir := flag.String("dir", "", "Directory path where credentials file will be stored")
    flag.Parse()

    if *dir == "" {
        log.Fatal("--dir is required")
    }

    // Ensure config directory exists
    if err := os.MkdirAll(*dir, 0755); err != nil {
        log.Fatalf("Failed to create config directory: %v", err)
    }

    credFile := filepath.Join(*dir, "s3_blob_credentials.json")

    // Main credential refresh loop
    for {
        if err := refreshCredentials(*region, credFile); err != nil {
            log.Printf("Failed to refresh credentials: %v", err)
            time.Sleep(time.Minute)
            continue
        }

        // Sleep for a random duration between 3-5 minutes
        sleepTime := time.Duration(180+rand.Intn(121)) * time.Second
        log.Printf("Credentials refreshed successfully, sleeping for %v", sleepTime)
        time.Sleep(sleepTime)
    }
}
