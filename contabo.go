//use aws sdk as it works with contabo but a few tweaks are needed
package storage

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"log"
	"os"
)

type S3Service struct {
	Storage
}

// AWS_S3_REGION = "European Union"
// session
func createSession() (*session.Session, error) {
	region := os.Getenv("AWS_S3_REGION")
	accessKey := os.Getenv("S3_STORAGE_ACCESS_KEY")
	secretKey := os.Getenv("S3_STORAGE_SECRET_ACCESS_KEY")

	sess, err := session.NewSessionWithOptions(session.Options{
		Profile: "eu2",
		Config: aws.Config{
			Region:           aws.String(region),
			S3ForcePathStyle: aws.Bool(true),
			EndpointResolver: endpoints.ResolverFunc(myCustomResolver),
			Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		},
	})
	if err != nil {
		log.Printf("Error creating session: %v", err)
	}

	return sess, err
}

// UploadObject uploads a file to the specified S3 bucket with the given key.
// It returns the uploaded URI on success, or an error if any occurred.
func (con S3Service) UploadObject(localPath string, remoteBucket string, remotePath string) (uploadedURI string, err error) {
	sess, _ := createSession()
	uploader := s3manager.NewUploader(sess)
	// Open the local file
	file, err := os.Open(localPath)
	if err != nil {
		log.Printf("Error uploading file from local:%v", err)
	}
	defer file.Close()
	// Upload the file to S3
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(remoteBucket),
		Key:    aws.String(remotePath),
		Body:   file,
		ACL:    aws.String("public-read"),
	})
	if err != nil {
		log.Printf("Error uploading to bucket: %v", err)
	}
	// Return the uploaded URI
	uploadedURI = result.Location
	return uploadedURI, err
}

// DeleteObject deletes the file at the specified S3 bucket and path.
// It returns an error if any occurred.
func (con S3Service) DeleteObject(remoteBucket string, remotePath string) error {
	// Create an AWS session
	sess, _ := createSession()
	// Create an S3 service client
	svc := s3.New(sess)
	// Create the input parameters for the S3 delete
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(remoteBucket),
		Key:    aws.String(remotePath),
	}
	// Delete the file from S3
	_, err := svc.DeleteObject(input)
	if err != nil {
		log.Printf("Error deleting object:%v", err)
	}

	return nil
}

// MoveFile moves a file from the source S3 bucket and path to the destination S3 bucket and path.
// It returns the uploaded URI on success, or an error if any occurred.
func (con S3Service) MoveFile(srcBucket, srcPath, dstBucket, dstPath string) (uploadedURI string, err error) {
	// Copy the file from the source to the destination
	s3_service := S3Service{}
	result, err := s3_service.CopyFile(srcBucket, srcPath, dstBucket, dstPath)
	if err != nil {
		return "", err
	}

	// Delete the source file
	err = s3_service.DeleteObject(srcBucket, srcPath)
	if err != nil {
		return "", err
	}

	// Return the uploaded URI
	uploadedURI = result
	return uploadedURI, err
}

// CopyFile copies a file from the source S3 bucket and path to the destination S3 bucket and path.
// It returns the uploaded URI on success, or an error if any occurred.
func (con S3Service) CopyFile(srcBucket, srcPath, dstBucket, dstPath string) (uploadedURI string, err error) {
	// Create an AWS session
	sess, _ := createSession()
	// Create an S3 service client
	svc := s3.New(sess)
	// Create the input parameters for the S3 copy
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", srcBucket, srcPath)),
		Key:        aws.String(dstPath),
	}
	// Copy the file from the source to the destination
	_, err = svc.CopyObject(input)
	if err != nil {
		log.Printf("Error copying object:%v", err)
	}
	// Return the uploaded URI
	uploadedURI = fmt.Sprintf("https://eu2.contabostorage.com/%s/%s", dstBucket, dstPath)
	return uploadedURI, err
}

// DownloadFile downloads a file from the specified S3 bucket and path to the local path.
// It returns an error if any occurred.
func (con S3Service) DownloadFile(remoteBucket, remotePath, localPath string) (err error) {
	// Create an AWS session
	sess, _ := createSession()
	// Create an S3 downloader
	downloader := s3manager.NewDownloader(sess)
	// Create the output file
	file, err := os.Create(localPath)
	if err != nil {
		log.Printf("Error creating path:%v", err)
	}
	// Download the file from S3
	_, err = downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(remoteBucket),
		Key:    aws.String(remotePath),
	})
	if err != nil {
		log.Printf("Error Downloading File:%v", err)
	}
	return err
}

func myCustomResolver(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
	region = os.Getenv("AWS_S3_REGION")
	if service == endpoints.S3ServiceID {
		return endpoints.ResolvedEndpoint{
			URL:           os.Getenv("S3_STORAGE_ENDPOINT"),
			SigningRegion: region,
		}, nil
	}
	return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
}
