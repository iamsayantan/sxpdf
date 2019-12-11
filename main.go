package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/SebastiaanKlippert/go-wkhtmltopdf"
)

const (
	// WKHTMLPath is the path string for wkhtmltopdf binary
	WKHTMLPath = "/usr/local/bin/wkhtmltopdf"

	// AWSBucketName is the default bucket name in case command line flag is not available
	AWSBucketName = "securnyx360"

	// S3FolderName is the name of the folder where the pdfs would be uploaded
	S3FolderName = "gopdfs"
)

var (
	jobs      = make(chan PdfGenerationJob, 50)
	awsKeyID  = flag.String("aws.key", "", "AWS Access Key ID")
	awsSecret = flag.String("aws.secret", "", "AWS Secret Key")

	awsSession *session.Session
	s3Service  *s3.S3
	s3Uploader *s3manager.Uploader
)

// PdfGenerationJob represents the structure for pdf generation
type PdfGenerationJob struct {
	TemplatePath string
	OutputPath   string
}

func init() {
	flag.Parse()
	var err error

	// verify that aws key and secret are present in the command line argument.
	// and create an aws session with the session arguments.
	if *awsKeyID == "" {
		log.Fatal("aws.key command line flag is required")
	}

	if *awsSecret == "" {
		log.Fatal("aws.secret command line flag is required")
	}

	awsSession, err = session.NewSession(&aws.Config{
		Region:      aws.String("ap-south-1"),
		Credentials: credentials.NewStaticCredentials(*awsKeyID, *awsSecret, ""),
	})

	if err != nil {
		log.Fatal(err)
	}

	s3Service = s3.New(awsSession)
	s3Uploader = s3manager.NewUploaderWithClient(s3Service)
}

func main() {
	start := time.Now()

	go allocateJobs(400)
	createWorkerPool(200)
	elapsed := time.Since(start)

	log.Printf("[Time Taken] %s", elapsed)
}

func allocateJobs(noOfJobs int) {
	for i := 0; i <= noOfJobs; i++ {
		job := PdfGenerationJob{TemplatePath: "./email.html", OutputPath: fmt.Sprintf("./gopdfs/email%d.pdf", i)}
		jobs <- job
	}

	close(jobs)
}

// createWorkerPool creates a pool of worker goroutines.
func createWorkerPool(noOfWorkers int) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg)
	}

	wg.Wait()
}

// worker function creates a worker which reads for the jobs channel.
// this function takes a WaitGroup wg as parameter on which it will call
// Done() when all jobs have been completed
func worker(wg *sync.WaitGroup) {
	for job := range jobs {
		err := GeneratePDF(job.TemplatePath, job.OutputPath)
		if err != nil {
			log.Println(err)
		}
	}
	wg.Done()
}

// GeneratePDF generates and saves the pdf from the html file.
func GeneratePDF(file, pdfFile string) error {
	// read the html file
	html, err := ioutil.ReadFile(file)

	if err != nil {
		return err
	}

	// set the predefined path in the wkhtmltopdf's global state
	wkhtmltopdf.SetPath(WKHTMLPath)

	// create a new page based on the HTML
	page := wkhtmltopdf.NewPageReader(bytes.NewReader(html))
	page.NoBackground.Set(true)
	page.DisableExternalLinks.Set(true)

	pdfGenerator, err := wkhtmltopdf.NewPDFGenerator()
	if err != nil {
		return err
	}

	// add page to the PDF generator
	pdfGenerator.AddPage(page)

	// set dpi of the content
	pdfGenerator.Dpi.Set(350)

	// set margins to zero at all directions
	pdfGenerator.MarginBottom.Set(0)
	pdfGenerator.MarginTop.Set(0)
	pdfGenerator.MarginLeft.Set(0)
	pdfGenerator.MarginRight.Set(0)

	// create the exact pdf
	err = pdfGenerator.Create()
	if err != nil {
		return err
	}

	uploadBody := bytes.NewReader(pdfGenerator.Bytes())
	uploadToS3(AWSBucketName, pdfFile, uploadBody)

	// write it into a file
	//err = pdfGenerator.WriteFile(pdfFile)
	//
	//if err != nil {
	//	return err
	//}

	return nil
}

func uploadToS3(bucketName, keyName string, body io.Reader) {
	uploadParams := &s3manager.UploadInput{
		Bucket: &bucketName,
		Key: &keyName,
		Body: body,
	}

	result, err := s3Uploader.Upload(uploadParams)
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("Successfully uploaded to s3", result.Location)
}