//iterate over local folder contents and upload to contabo bucket
package main

import (
	"flag"
	"fmt"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// SyncFolderIterator is used to upload a given folder
// to Amazon S3.
type SyncFolderIterator struct {
	bucket    string
	fileInfos []fileInfo
	err       error
}

type fileInfo struct {
	key      string
	fullpath string
}

// NewSyncFolderIterator will walk the path, and store the key and full path
// of the object to be uploaded. This will return a new SyncFolderIterator
// with the data provided from walking the path.
func NewSyncFolderIterator(path, bucket string) *SyncFolderIterator {
	metadata := []fileInfo{}
	filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			key := strings.TrimPrefix(p, path)
			metadata = append(metadata, fileInfo{key, p})
		}

		return nil
	})

	return &SyncFolderIterator{
		bucket,
		metadata,
		nil,
	}
}

// Next will determine whether or not there is any remaining files to
// be uploaded.
func (iter *SyncFolderIterator) Next() bool {
	return len(iter.fileInfos) > 0
}

// Err returns any error when os.Open is called.
func (iter *SyncFolderIterator) Err() error {
	return iter.err
}

// UploadObject will prep the new upload object by open that file and constructing a new
// s3manager.UploadInput.
func (iter *SyncFolderIterator) UploadObject() s3manager.BatchUploadObject {
	fi := iter.fileInfos[0]
	iter.fileInfos = iter.fileInfos[1:]
	body, err := os.Open(fi.fullpath)
	if err != nil {
		iter.err = err
	}

	extension := filepath.Ext(fi.key)
	mimeType := mime.TypeByExtension(extension)

	if mimeType == "" {
		mimeType = "binary/octet-stream"
	}

	input := s3manager.UploadInput{
		Bucket:      &iter.bucket,
		Key:         &fi.key,
		Body:        body,
		ContentType: &mimeType,
	}

	return s3manager.BatchUploadObject{
		Object: &input,
	}
}

// Upload a directory to a given bucket

func myCustomResolver(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
	if service == endpoints.S3ServiceID {
		return endpoints.ResolvedEndpoint{
			URL: "",
			//SigningRegion: region,
		}, nil
	}

	return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
}
func main() {
	bucketPtr := flag.String("bucket", "", "bucket to upload to")
	regionPtr := flag.String("region", "", "region to be used when making requests")
	pathPtr := flag.String("path", "", "path of directory to be synced")
	flag.Parse()

	sess, err := session.NewSessionWithOptions(session.Options{
		Profile: "eu2",
		Config: aws.Config{
			Region:           aws.String(*regionPtr),
			S3ForcePathStyle: aws.Bool(true),
			EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
		},
		AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
	})
	if err != nil {
		log.Fatal(err)
	}
	uploader := s3manager.NewUploader(sess)

	iter := NewSyncFolderIterator(*pathPtr, *bucketPtr)
	if err := uploader.UploadWithIterator(aws.BackgroundContext(), iter); err != nil {
		fmt.Fprintf(os.Stderr, "unexpected error has occurred: %v", err)
	}

	if err := iter.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "unexpected error occurred during file walking: %v", err)
	}

	fmt.Println("Success")
}
