// s3-benchmark.go
// Copyright (c) 2017 Wasabi Technology, Inc.

package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Global variables
var accessKey, secretKey, urlHost, bucket, copyBucket string
var durationSecs, threads, loops int
var objectSize uint64
var objectData []byte
var runningThreads, uploadCount, downloadCount, deleteCount, copyCount, listCount int32
var endtime, uploadFinish, downloadFinish, deleteFinish, listFinish, copyFinish time.Time

func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
		logfile.Close()
	}
}

// HTTPTransport used for the roundtripper below
var HTTPTransport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 0,
	// Allow an unlimited number of idle connections
	MaxIdleConnsPerHost: 4096,
	MaxIdleConns:        0,
	// But limit their idle time
	IdleConnTimeout: time.Minute,
	// Ignore TLS errors
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

var httpClient = &http.Client{Transport: HTTPTransport}

func getS3Client() *s3.S3 {
	// Build our config
	creds := credentials.NewStaticCredentials(accessKey, secretKey, "")
	loglevel := aws.LogOff
	// Build the rest of the configuration
	awsConfig := &aws.Config{
		Region:               aws.String("us-east-1"),
		Endpoint:             aws.String(urlHost),
		Credentials:          creds,
		LogLevel:             &loglevel,
		S3ForcePathStyle:     aws.Bool(true),
		S3Disable100Continue: aws.Bool(true),
		// Comment following to use default transport
		HTTPClient: &http.Client{Transport: HTTPTransport},
	}
	session := session.New(awsConfig)
	client := s3.New(session)
	if client == nil {
		log.Fatalf("FATAL: Unable to create new client.")
	}
	// Return success
	return client
}

func createBucket(buckets ...string) {
	// Get a client
	client := getS3Client()
	// Create our bucket (may already exist without error)
	for _, newBucketName := range buckets {
		in := &s3.CreateBucketInput{Bucket: aws.String(newBucketName)}
		if _, err := client.CreateBucket(in); err != nil {
			log.Fatalf("FATAL: Unable to create bucket %s (is your access and secret correct?): %v", newBucketName, err)
		}
	}

}

func deleteAllBuckets(buckets ...string) {
	// Get a client
	client := getS3Client()
	// Use multiple routines to do the actual delete
	for _, bucketName := range buckets {
		_, err := client.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: &bucketName,
		})
		if err != nil {
			log.Fatalf("FATAL: Unable to delete bucket: %v, the error was %v", bucketName, err)
		}
	}
}

func deleteAllObjectsVersioned(buckets ...string) {
	// Get a client
	client := getS3Client()
	// Use multiple routines to do the actual delete
	for _, bucketName := range buckets {
		var doneDeletes sync.WaitGroup
		fmt.Println("Deleting contents of: ", bucketName)
		// Loop deleting our versions reading as big a list as we can
		var keyMarker, versionID *string
		var err error
		for loop := 1; ; loop++ {
			// All of this code only works on versioned buckets... so we need to see if the bucket is versioned
			// versioning, _ := client.GetBucketVersioning(&s3.GetBucketVersioningInput{Bucket: aws.String(bucketName)})

			// Delete all the existing objects and versions in the bucket
			in := &s3.ListObjectVersionsInput{Bucket: aws.String(bucketName), KeyMarker: keyMarker, VersionIdMarker: versionID, MaxKeys: aws.Int64(1000)}
			if listVersions, listErr := client.ListObjectVersions(in); listErr == nil {
				delete := &s3.Delete{Quiet: aws.Bool(true)}
				for _, version := range listVersions.Versions {
					delete.Objects = append(delete.Objects, &s3.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
				}
				for _, marker := range listVersions.DeleteMarkers {
					delete.Objects = append(delete.Objects, &s3.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
				}
				if len(delete.Objects) > 0 {
					// Start a delete routine
					doDelete := func(bucket string, delete *s3.Delete) {
						if _, e := client.DeleteObjects(&s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: delete}); e != nil {
							err = fmt.Errorf("DeleteObjects unexpected failure: %s", e.Error())
						}
						doneDeletes.Done()
					}
					doneDeletes.Add(1)
					go doDelete(bucketName, delete)
				}
				// Advance to next versions
				if listVersions.IsTruncated == nil || !*listVersions.IsTruncated {
					break
				}
				keyMarker = listVersions.NextKeyMarker
				versionID = listVersions.NextVersionIdMarker
			} else {
				// The bucket may not exist, just ignore in that case
				if strings.HasPrefix(listErr.Error(), "NoSuchBucket") {
					return
				}
				err = fmt.Errorf("ListObjectVersions unexpected failure: %v", listErr)
				break
			}
		}
		// Wait for deletes to finish
		doneDeletes.Wait()
		// If error, it is fatal
		if err != nil {
			log.Fatalf("FATAL: Unable to delete objects from bucket: %v", err)
		}
	}
}

func deleteAllObjects(buckets ...string) {
	// Get a client
	client := getS3Client()
	// Use multiple routines to do the actual delete
	for _, bucketName := range buckets {
		var doneDeletes sync.WaitGroup
		for loop := 1; ; loop++ {
			// All of this code only works on versioned buckets... so we need to see if the bucket is versioned
			// versioning, _ := client.GetBucketVersioning(&s3.GetBucketVersioningInput{Bucket: aws.String(bucketName)})

			// Delete all the existing objects and versions in the bucket
			in := &s3.ListObjectInput{Bucket: aws.String(bucketName), KeyMarker: keyMarker, VersionIdMarker: versionID, MaxKeys: aws.Int64(1000)}
			if listVersions, listErr := client.ListObjectVersions(in); listErr == nil {
				delete := &s3.Delete{Quiet: aws.Bool(true)}
				for _, version := range listVersions.Versions {
					delete.Objects = append(delete.Objects, &s3.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
				}
				for _, marker := range listVersions.DeleteMarkers {
					delete.Objects = append(delete.Objects, &s3.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
				}
				if len(delete.Objects) > 0 {
					// Start a delete routine
					doDelete := func(bucket string, delete *s3.Delete) {
						if _, e := client.DeleteObjects(&s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: delete}); e != nil {
							err = fmt.Errorf("DeleteObjects unexpected failure: %s", e.Error())
						}
						doneDeletes.Done()
					}
					doneDeletes.Add(1)
					go doDelete(bucketName, delete)
				}
				// Advance to next versions
				if listVersions.IsTruncated == nil || !*listVersions.IsTruncated {
					break
				}
				keyMarker = listVersions.NextKeyMarker
				versionID = listVersions.NextVersionIdMarker
			} else {
				// The bucket may not exist, just ignore in that case
				if strings.HasPrefix(listErr.Error(), "NoSuchBucket") {
					return
				}
				err = fmt.Errorf("ListObjectVersions unexpected failure: %v", listErr)
				break
			}
		}
		// Wait for deletes to finish
		doneDeletes.Wait()
		// If error, it is fatal
		if err != nil {
			log.Fatalf("FATAL: Unable to delete objects from bucket: %v", err)
		}
	}
}

func runUpload(threadNum int) {
	client := getS3Client()
	for time.Now().Before(endtime) {
		objnum := atomic.AddInt32(&uploadCount, 1)
		fileobj := bytes.NewReader(objectData)
		prefix := fmt.Sprintf("Object-%d", objnum)
		// using aws library instead of http put and gets
		_, err := client.PutObject(&s3.PutObjectInput{
			Body:   fileobj,
			Bucket: &bucket,
			Key:    &prefix,
		})
		if err != nil {
			log.Fatalln("Failed to upload", err)
		}

	}
	// Remember last done time
	uploadFinish = time.Now()
	// One less thread
	atomic.AddInt32(&runningThreads, -1)
}

func runCopy(threadNum int) {
	client := getS3Client()
	for time.Now().Before(endtime) {
		atomic.AddInt32(&copyCount, 1)
		objnum := rand.Int31n(uploadCount) + 1
		prefix := fmt.Sprintf("Object-%d", objnum)
		//using aws library instead of puts and gets
		sourceObject := aws.String(fmt.Sprintf("/%v/%v", bucket, prefix))
		_, err := client.CopyObject(&s3.CopyObjectInput{
			Bucket:     &copyBucket,
			CopySource: sourceObject,
			Key:        &prefix,
		})

		if err != nil {
			log.Fatal("Failed to copy object", err)
		}
	}
	// Remember last done time
	copyFinish = time.Now()
	// One less thread
	atomic.AddInt32(&runningThreads, -1)
}

func runDownload(threadNum int) {
	client := getS3Client()
	for time.Now().Before(endtime) {
		atomic.AddInt32(&downloadCount, 1)
		objnum := rand.Int31n(uploadCount) + 1
		prefix := fmt.Sprintf("Object-%d", objnum)
		//using aws library instead of puts and gets
		resultFile, err := client.GetObject(&s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &prefix,
		})
		if err != nil {
			log.Fatal("Failed to get object", err)
		}

		if _, err := io.Copy(ioutil.Discard, resultFile.Body); err != nil {
			log.Fatal("Failed to copy object to file", err)
		}
		resultFile.Body.Close()

	}
	// Remember last done time
	downloadFinish = time.Now()
	// One less thread
	atomic.AddInt32(&runningThreads, -1)
}

func runDelete(threadNum int) {
	client := getS3Client()
	for {
		objnum := atomic.AddInt32(&deleteCount, 1)
		if objnum > uploadCount {
			break
		}
		prefix := fmt.Sprintf("Object-%d", objnum)
		// use aws library to do the work

		_, err := client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    &prefix,
		})
		if err != nil {
			log.Fatalf("FATAL: Error deleting object %s: %v", prefix, err)
		}
	}
	// Remember last done time
	deleteFinish = time.Now()
	// One less thread
	atomic.AddInt32(&runningThreads, -1)
}

func runList() {
	// Get a client
	client := getS3Client()
	// just get a complete list loop times of everything in bucket
	var keyMarker, versionID *string
	var err error
	for loop := 1; ; loop++ {
		// Delete all the existing objects and versions in the bucket
		in := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), KeyMarker: keyMarker, VersionIdMarker: versionID, MaxKeys: aws.Int64(1000)}
		if listVersions, listErr := client.ListObjectVersions(in); listErr == nil {
			for _ = range listVersions.Versions {
				atomic.AddInt32(&listCount, 1)
			}
			for _ = range listVersions.DeleteMarkers {
				atomic.AddInt32(&listCount, 1)
			}
			// Advance to next versions
			if listVersions.IsTruncated == nil || !*listVersions.IsTruncated {
				break
			}
			keyMarker = listVersions.NextKeyMarker
			versionID = listVersions.NextVersionIdMarker
		} else {
			// The bucket may not exist, just ignore in that case
			if strings.HasPrefix(listErr.Error(), "NoSuchBucket") {
				return
			}
			err = fmt.Errorf("ListObjectVersions unexpected failure: %v", listErr)
			break
		}
	}
	if err != nil {
		log.Fatalf("FATAL: Unable to list objects from bucket: %v", err)
	}
	listFinish = time.Now()
}

func main() {
	// Hello
	fmt.Println("Wasabi benchmark program v2.1")

	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.StringVar(&accessKey, "a", "", "Access key")
	myflag.StringVar(&secretKey, "s", "", "Secret key")
	myflag.StringVar(&urlHost, "u", "http://s3.wasabisys.com", "URL for host with method prefix")
	myflag.StringVar(&bucket, "b", "wasabi-benchmark-bucket", "Bucket for testing")
	myflag.IntVar(&durationSecs, "d", 60, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	if accessKey == "" {
		log.Fatal("Missing argument -a for access key.")
	}
	if secretKey == "" {
		log.Fatal("Missing argument -s for secret key.")
	}
	var err error
	if objectSize, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}

	// Echo the parameters
	logit(fmt.Sprintf("Parameters: url=%s, bucket=%s, duration=%d, threads=%d, loops=%d, size=%s",
		urlHost, bucket, durationSecs, threads, loops, sizeArg))

	// Initialize data for the bucket
	objectData = make([]byte, objectSize)
	rand.Read(objectData)

	// Create the bucket and delete all the objects
	copyBucket = fmt.Sprintf("%v-copy", bucket)
	createBucket(bucket, copyBucket)
	deleteAllObjects(bucket, copyBucket)

	// Loop running the tests
	for loop := 1; loop <= loops; loop++ {

		// Run the upload case
		runningThreads = int32(threads)
		starttime := time.Now()
		endtime = starttime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			go runUpload(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&runningThreads) > 0 {
			time.Sleep(time.Millisecond)
		}
		uploadTime := uploadFinish.Sub(starttime).Seconds()

		bps := float64(uint64(uploadCount)*objectSize) / uploadTime
		logit(fmt.Sprintf("Loop %d: PUT time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec.",
			loop, uploadTime, uploadCount, bytefmt.ByteSize(uint64(bps)), float64(uploadCount)/uploadTime))

		// Run the download case
		runningThreads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			go runDownload(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&runningThreads) > 0 {
			time.Sleep(time.Millisecond)
		}
		downloadTime := downloadFinish.Sub(starttime).Seconds()

		bps = float64(uint64(downloadCount)*objectSize) / downloadTime
		logit(fmt.Sprintf("Loop %d: GET time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec.",
			loop, downloadTime, downloadCount, bytefmt.ByteSize(uint64(bps)), float64(downloadCount)/downloadTime))

		// Run the copy case
		runningThreads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			go runCopy(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&runningThreads) > 0 {
			time.Sleep(time.Millisecond)
		}
		copyTime := copyFinish.Sub(starttime).Seconds()

		logit(fmt.Sprintf("Loop %d: COPY time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec.",
			loop, copyTime, copyCount, bytefmt.ByteSize(uint64(bps)), float64(copyCount)/copyTime))

		// Run the list objects case
		// note this is single threaded for now
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(durationSecs))
		runList()

		listTime := listFinish.Sub(starttime).Seconds()
		logit(fmt.Sprintf("Loop %d: LIST time %.1f secs, objects listed = %d, speed = %.1f objects/sec.",
			loop, listTime, listCount, float64(listCount)/listTime))

		// Run the delete case
		runningThreads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(durationSecs))
		for n := 1; n <= threads; n++ {
			go runDelete(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&runningThreads) > 0 {
			time.Sleep(time.Millisecond)
		}
		deleteTime := deleteFinish.Sub(starttime).Seconds()

		logit(fmt.Sprintf("Loop %d: DELETE time %.1f secs, %.1f deletes/sec.",
			loop, deleteTime, float64(uploadCount)/deleteTime))

		// Do some cleanup
		logit(fmt.Sprint("Doing some cleanup."))
		deleteAllObjects(copyBucket)
		deleteAllBuckets(bucket, copyBucket)
	}

	// All done
	fmt.Println("Benchmark completed.")
}
