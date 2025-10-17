// fdb-aws-s3-credentials-fetcher.go
//   Copyright 2024 Apple Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/pflag"
)

type BlobCredentials struct {
    Accounts map[string]Account `json:"accounts"`
}

type Account struct {
    Secret  string `json:"secret"`
    APIKey  string `json:"api_key"`
    Token   string `json:"token"`
}

func writeCredentialsFile(bucket, region, credFile string, accessKey, secretKey, token string) error {
    s3Endpoint := fmt.Sprintf("s3.%s.amazonaws.com", region)
    
    // Bulkload hostname is like this backup-112664522426-us-west-2.s3-us-west-2.amazonaws.com
    // i.e. bucket and then s3.region.amazonaws.com.
    // So add records for this format and for s3.REGION.amazonaws.com too.
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
            "@" + bucket + "." + s3Endpoint: {
                Secret:  secretKey,
                APIKey:  accessKey,
                Token:   token,
            },
            "@" + bucket + "." + s3Endpoint + ":443": {
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
        return fmt.Errorf("failed to write credentials file: %w", err)
    }

    return nil
}

func refreshCredentials(ctx context.Context, bucket, region, credFile string, expiryThreshold time.Duration) error {
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

    // Just write the credentials file each time. I was checking the expiry time, but
    // looking at it and then the blob_credentials.json file, comparing, and then
    // updating made the script more complicated than it needed to be.
    return writeCredentialsFile(bucket, region, credFile, creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken)
}

func main() {
    // Create a context for the lifetime of the program
    ctx := context.Background()

    // Add usage message
    pflag.Usage = func() {
        fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "A credential fetcher for AWS S3 that continuously refreshes credentials for FDB blob storage.\n")
        fmt.Fprintf(os.Stderr, "Requires AWS credentials to be configured in ~/.aws/credentials file.\n\n")
        fmt.Fprintf(os.Stderr, "Options:\n")
        pflag.PrintDefaults()
    }

    help := pflag.BoolP("help", "h", false, "Print this help message")
    
    defaultRegion := "us-west-2"
    if envRegion := os.Getenv("AWS_REGION"); envRegion != "" {
        defaultRegion = envRegion
    }
    
    region := pflag.String("region", defaultRegion, "AWS region for S3 endpoint; default from AWS_REGION env var")
    dir := pflag.String("dir", "", "Directory path where credentials file will be stored (required)")
    defaultBucket := fmt.Sprintf("backup-112664522426-%s", defaultRegion)
    bucket := pflag.String("bucket", defaultBucket, "S3 bucket name")
    runOnce := pflag.Bool("run-once", false, "Generate credentials once and exit")
    expiryThreshold := pflag.Duration("expiry-threshold", 5*time.Minute, "Refresh credentials when they expire within this duration")
    pflag.Parse()

    // If help requested or no dir specified, print usage and exit
    if *help || *dir == "" {
        pflag.Usage()
        os.Exit(1)
    }

    // Ensure config directory exists
    if err := os.MkdirAll(*dir, 0755); err != nil {
        log.Fatalf("Failed to create config directory: %v", err)
    }

    credFile := filepath.Join(*dir, "s3_blob_credentials.json")

    // If run-once is true, just generate credentials and exit
    if *runOnce {
        if err := refreshCredentials(ctx, *bucket, *region, credFile, *expiryThreshold); err != nil {
            log.Fatalf("Failed to refresh credentials: %v", err)
        }
        log.Printf("Credentials written successfully to %s", credFile)
        return
    }

    // Main credential refresh loop
    log.Printf("Starting credential refresh loop")
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()

    // Do first refresh immediately
    if err := refreshCredentials(ctx, *bucket, *region, credFile, *expiryThreshold); err != nil {
        log.Printf("Failed to refresh credentials: %v", err)
    }

    for {
        select {
        case <-ticker.C:
            if err := refreshCredentials(ctx, *bucket, *region, credFile, *expiryThreshold); err != nil {
                log.Printf("Failed to refresh credentials: %v", err)
                continue
            }
            log.Printf("Credentials refreshed successfully")
        case <-ctx.Done():
            return
        }
    }
}
