package gcsmpu

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-resty/resty/v2"
	"golang.org/x/sync/errgroup"
)

const (
	MPUInitiateQuery   = "uploads"
	MPUPartNumberQuery = "partNumber"
	MPUUploadIDQuery   = "uploadId"
)

type UploadBase struct {
	cli             *storage.Client
	bucket          string
	blob            string
	uploadFile      string
	retry           int
	signedURLExpiry time.Duration
	log             *Logger
}

func (u UploadBase) Clone() UploadBase {
	return UploadBase{
		cli:             u.cli,
		bucket:          u.bucket,
		blob:            u.blob,
		uploadFile:      u.uploadFile,
		retry:           u.retry,
		signedURLExpiry: u.signedURLExpiry,
		log:             u.log,
	}
}

type XMLMPU struct {
	// cli        *storage.Client
	// bucket     string
	// blob       string
	// sourceFile string
	UploadBase
	XMLMPUParts []*XMLMPUPart
	chunkSize   int
	workers     int
	uploadID    string
}

type Option func(m *XMLMPU)

const (
	defaultChunkSize       = 5 * 1024 * 1024 // 5 MB
	defaultRetry           = 3               // 建议默认3~5次，不要太大，否则将可能会导致成为僵尸任务。
	defaultSignedURLExpiry = 15 * time.Minute

	MinimumChunkSize = 5 * 1024 * 1024               // 5 MB
	MaximumChunkSize = 5 * 1024 * 1024 * 1024        // 5 GB
	MaximumFileSize  = 5 * 1024 * 1024 * 1024 * 1024 // 5 TB
	MaxUploadSession = 7 * time.Hour * 24            // 7 days
	MaximumParts     = 10000
)

// 请查阅google storage 关于XML API的上传限制文档
// https://cloud.google.com/storage/quotas#requests
//
// Upload a single file in chunks, concurrently.
// This function uses the XML MPU API to initialize an upload and upload a
// file in chunks, concurrently with a worker pool.
//
// The XML MPU API is significantly different from other uploads; please review
// the documentation at `https://cloud.google.com/storage/docs/multipart-uploads`
// before using this feature.
//
// The library will attempt to cancel uploads that fail due to an exception.
// If the upload fails in a way that precludes cancellation, such as a
// hardware failure, process termination, or power outage, then the incomplete
// upload may persist indefinitely. To mitigate this, set the
// `AbortIncompleteMultipartUpload` with a nonzero `Age` in bucket lifecycle
// rules, or refer to the XML API documentation linked above to learn more
// about how to list and delete individual downloads.

// bucket: The name of the bucket to upload to.
// blob: The blob to which to upload.
// uploadFile: The path to the file to upload. File-like objects are not supported.
func NewXMLMPU(cli *storage.Client, bucket string, blob string, uploadFile string, options ...Option) (XMLMPU, error) {
	// Check if uploadFile exists
	_, err := os.Stat(uploadFile)
	if err != nil {
		return XMLMPU{}, err
	}

	m := XMLMPU{
		UploadBase: UploadBase{
			cli:             cli,
			bucket:          bucket,
			blob:            blob,
			uploadFile:      uploadFile,
			retry:           defaultRetry,
			signedURLExpiry: defaultSignedURLExpiry,
			log:             NewLogger(nil, false),
		},
		chunkSize: defaultChunkSize,
		workers:   runtime.NumCPU(),
	}

	for _, option := range options {
		option(&m)
	}

	if m.workers < 1 {
		m.workers = 1
	}

	if m.chunkSize < MinimumChunkSize || m.chunkSize > MaximumChunkSize {
		err := fmt.Errorf("Invalid chunk size: %d. Chunk size must be between %d and %d", m.chunkSize, MinimumChunkSize, MaximumChunkSize)
		return XMLMPU{}, err
	}

	return m, nil
}

func WithBucket(bucket string) Option {
	return func(m *XMLMPU) {
		m.bucket = bucket
	}
}

func WithBlob(blob string) Option {
	return func(m *XMLMPU) {
		m.blob = blob
	}
}

func WithChunkSize(chunkSize int) Option {
	return func(m *XMLMPU) {
		m.chunkSize = chunkSize
	}
}

func WithWorkers(workers int) Option {
	return func(m *XMLMPU) {
		m.workers = workers
	}
}
func WithRetry(retry int) Option {
	return func(m *XMLMPU) {
		m.retry = retry
	}
}
func WithLog(log LoggerInterface, debug bool) Option {
	return func(m *XMLMPU) {
		m.log = NewLogger(log, debug)
	}
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Text     string   `xml:",chardata"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type Part struct {
	Text       string `xml:",chardata"`
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUpload struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Text    string   `xml:",chardata"`
	Parts   []Part   `xml:"Part"`
}

type FinalizeXMLMPUResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult" json:"-"`
	Text     string   `xml:",chardata" json:"-"`
	Xmlns    string   `xml:"xmlns,attr" json:"-"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

func (m *XMLMPU) FinalizeXMLMPU() (result FinalizeXMLMPUResult, err error) {
	finalXMLRoot := CompleteMultipartUpload{
		Parts: []Part{},
	}
	for _, part := range m.XMLMPUParts {
		part := Part{
			PartNumber: part.PartNumber,
			ETag:       part.etag,
		}
		finalXMLRoot.Parts = append(finalXMLRoot.Parts, part)
	}

	xmlBytes, err := xml.Marshal(finalXMLRoot)
	if err != nil {
		err = fmt.Errorf("Failed to encode XML: %v\n", err)
		return
	}

	xmlString := string(xmlBytes)
	m.log.Debugf("Final XML: %s", xmlString)

	values := url.Values{}
	values.Add(MPUUploadIDQuery, m.uploadID)

	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "POST",
		Expires: time.Now().Add(m.signedURLExpiry),
		//ContentType:     "application/xml",
		QueryParameters: values,
	}
	u, err := m.cli.Bucket(m.bucket).SignedURL(m.blob, opts)
	if err != nil {
		err = fmt.Errorf("Bucket(%q).SignedURL: %s", m.bucket, err)
		return
	}

	client := resty.New()
	resp, err := client.R().SetBody(xmlBytes).Post(u)
	if err != nil {
		err = fmt.Errorf("POST request failed: %s", err)
		return
	}
	m.log.Debug("Finalize MPU: ", string(resp.Body()))

	if resp.StatusCode() != http.StatusOK {
		err = fmt.Errorf("POST request returned non-OK status: %d", resp.StatusCode())
		return
	}
	body := resp.Body()
	m.log.Debugf("Initiate MPU: %s", string(body))

	err = xml.Unmarshal(body, &result)
	if err != nil {
		err = fmt.Errorf("failed to unmarshal response body: %s", err)
		return
	}

	return
}
func (m *XMLMPU) InitiateXMLMPU() error {
	opts := &storage.SignedURLOptions{
		Scheme:          storage.SigningSchemeV4,
		Method:          "POST",
		Expires:         time.Now().Add(m.signedURLExpiry),
		QueryParameters: url.Values{MPUInitiateQuery: []string{""}},
	}
	u, err := m.cli.Bucket(m.bucket).SignedURL(m.blob, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", m.bucket, err)
	}

	client := resty.New()
	resp, err := client.R().Post(u)
	if err != nil {
		return fmt.Errorf("POST request failed: %s", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("POST request returned non-OK status: %d", resp.StatusCode())
	}
	body := resp.Body()
	m.log.Debugf("Initiate MPU: %s", string(body))

	result := InitiateMultipartUploadResult{}
	err = xml.Unmarshal(body, &result)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response body: %s", err)
	}

	uploadID := result.UploadId
	m.uploadID = uploadID
	return nil
}

func (m *XMLMPU) UploadChunksConcurrently() (result FinalizeXMLMPUResult, err error) {

	err = m.InitiateXMLMPU()
	if err != nil {
		err = fmt.Errorf("Failed to initiate XMLMPU: %s", err)
		return
	}
	m.log.Infof("Initiated XMLMPU: %s", m.uploadID)

	err = m.InitiateXMLMPUParts()
	if err != nil {
		err = fmt.Errorf("Failed to initiate multipart upload: %s", err)
		return
	}
	m.log.Infof("Initiated %d XMLMPU parts", len(m.XMLMPUParts))
	err = m.UploadPartsConcurrently()
	if err != nil {
		m.log.Errorf("Failed to upload parts: %s", err)
		errC := m.Cancel()
		if errC != nil {
			err = fmt.Errorf("Failed to upload parts: %s, Failed to cancel multipart upload: %s", err, errC)
			return
		}
		err = fmt.Errorf("Failed to upload parts: %s", err)
		return
	}

	result, err = m.FinalizeXMLMPU()
	if err != nil {
		m.log.Errorf("Failed to finalize multipart upload: %s", err)
		errC := m.Cancel()
		if errC != nil {
			err = fmt.Errorf("Failed to finalize multipart upload: %s, Failed to cancel multipart upload: %s", err, errC)
			return
		}
		err = fmt.Errorf("Failed to finalize multipart upload: %s", err)
		return
	}
	m.log.Infof("Finalized XMLMPU: %s", m.uploadID)

	return
}

func (m *XMLMPU) Cancel() error {
	opts := &storage.SignedURLOptions{
		Scheme:          storage.SigningSchemeV4,
		Method:          "DELETE",
		Expires:         time.Now().Add(m.signedURLExpiry),
		QueryParameters: url.Values{MPUUploadIDQuery: []string{m.uploadID}},
	}
	u, err := m.cli.Bucket(m.bucket).SignedURL(m.blob, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", m.bucket, err)
	}

	client := resty.New()
	resp, err := client.R().Delete(u)
	if err != nil {
		return fmt.Errorf("POST request failed: %s", err)
	}
	body := resp.Body()
	m.log.Debugf("Cancel MPU: %s", string(body))

	if resp.StatusCode() != http.StatusOK {
		return fmt.Errorf("POST request returned non-OK status: %d", resp.StatusCode())
	}

	return nil
}

func (m *XMLMPU) UploadPartsConcurrently() error {
	var eg errgroup.Group
	sem := make(chan int, m.workers)
	for i, part := range m.XMLMPUParts {
		current := i
		sem <- 1
		eg.Go(func() error {
			err := m.XMLMPUParts[current].Upload()
			if err != nil {
				<-sem
				return fmt.Errorf("Failed to upload part %d: %s", part.PartNumber, err)
			}
			<-sem
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return fmt.Errorf("Failed to upload parts: %s", err)
	}
	return nil
}

func (m *XMLMPU) InitiateXMLMPUParts() error {
	fileInfo, err := os.Stat(m.uploadFile)
	if err != nil {
		return fmt.Errorf("Failed to get file size: %s", err)
	}
	fileSize := fileInfo.Size()
	numParts := (fileSize + int64(m.chunkSize) - 1) / int64(m.chunkSize)
	if numParts > MaximumParts {
		return fmt.Errorf("File size %d is too large to upload in %d parts", fileSize, numParts)
	}

	m.XMLMPUParts = make([]*XMLMPUPart, numParts)
	for partNumber := 1; partNumber <= int(numParts); partNumber++ {
		// Calculate the start and end positions for each part
		start := int64((partNumber - 1) * m.chunkSize)
		end := start + int64(m.chunkSize)
		if end > fileSize {
			end = fileSize
		}
		//checksum := calculateChecksum(m.uploadFile, start, end)

		part := &XMLMPUPart{
			UploadBase: m.UploadBase.Clone(),
			UploadID:   m.uploadID,
			Start:      start,
			End:        end,
			PartNumber: partNumber,
			Checksum:   "",
		}

		m.XMLMPUParts[partNumber-1] = part
	}
	m.log.Infof("Initiated '%s' upload file, size '%d', chunk size '%d', total parts '%d'",
		m.uploadFile, fileSize, m.chunkSize, numParts)
	return nil
}

type XMLMPUPart struct {
	UploadBase
	UploadID   string
	Start      int64
	End        int64
	PartNumber int
	Checksum   string
	etag       string
	finished   bool
}

func (p *XMLMPUPart) Clone() *XMLMPUPart {
	return &XMLMPUPart{
		UploadBase: p.UploadBase.Clone(),
		UploadID:   p.UploadID,
		Start:      p.Start,
		End:        p.End,
		PartNumber: p.PartNumber,
		Checksum:   p.Checksum,
	}
}

func readPayload(filename string, start int64, end int64) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(start, io.SeekStart)
	if err != nil {
		return nil, err
	}

	payload := make([]byte, end-start)
	_, err = file.Read(payload)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

func (p *XMLMPUPart) Upload() error {

	err := p.upload()
	if err == nil {
		return nil
	}

	for i := 0; i < p.retry; i++ {
		err := p.upload()
		if err == nil {
			return nil
		}
		p.log.Errorf("Failed to upload part %d, retry %d: %s", p.PartNumber, i+1, err)
	}

	return fmt.Errorf("Failed to upload part %d", p.PartNumber)
}

func (p *XMLMPUPart) upload() error {
	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "PUT",
		Expires: time.Now().Add(p.signedURLExpiry),
		QueryParameters: url.Values{
			MPUUploadIDQuery:   []string{p.UploadID},
			MPUPartNumberQuery: []string{strconv.Itoa(p.PartNumber)},
		},
	}

	u, err := p.cli.Bucket(p.bucket).SignedURL(p.blob, opts)
	if err != nil {
		return fmt.Errorf("Bucket(%q).SignedURL: %s", p.bucket, err)
	}

	p.log.Debugf("Upload PartNumber '%d' start", p.PartNumber)

	f, err := os.Open(p.uploadFile)
	if err != nil {
		return fmt.Errorf("open file failed: %s", err)
	}
	reader := io.NewSectionReader(f, p.Start, p.End-p.Start)

	client := resty.New()
	resp, err := client.R().SetBody(reader).Put(u) // Set payload as request body
	if err != nil {
		return fmt.Errorf("PUT request failed: %s", err)
	}
	p.log.Debugf("Upload PartNumber '%d' end", p.PartNumber)

	p.etag = resp.Header().Get("ETag")

	if resp.StatusCode() != http.StatusOK {
		body := resp.Body()
		p.log.Errorf("Upload response body: %s", string(body))
		return fmt.Errorf("PUT request returned non-OK status: %d", resp.StatusCode())
	}
	p.finished = true
	p.log.Infof("Uploaded part %d, Etag:%s", p.PartNumber, p.etag)

	return nil
}
