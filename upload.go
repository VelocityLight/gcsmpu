package gcsmpu

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-resty/resty/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
)

const (
	MPUInitiateQuery   = "uploads"
	MPUPartNumberQuery = "partNumber"
	MPUUploadIDQuery   = "uploadId"
)

type UploadBase struct {
	cli             *storage.Client
	ctx             context.Context
	bucket          string
	blob            string
	retry           int
	signedURLExpiry time.Duration
	log             *Logger
}

func (u UploadBase) Clone() UploadBase {
	return UploadBase{
		cli:             u.cli,
		ctx:             u.ctx,
		bucket:          u.bucket,
		blob:            u.blob,
		retry:           u.retry,
		signedURLExpiry: u.signedURLExpiry,
		log:             u.log,
	}
}

type XMLMPU struct {
	UploadBase
	XMLMPUParts XMLMPUParts
	wg          sync.WaitGroup
	mutex       sync.Mutex
	err         error
	chunkSize   int64
	workers     int
	totalSize   int64
	// readerPos 仅仅应用在seeker类型的reader，用于记录当前读取reader截止的位置
	readerPos             int64
	uploadID              string
	disableReaderAtSeeker bool
	reader                io.Reader
	// 提供slicePool的实现，可以是maxSlicePool或者其他实现了byteSlicePool接口的对象
	slicePool byteSlicePool
}
type XMLMPUParts []*XMLMPUPart

func (x XMLMPUParts) Len() int           { return len(x) }
func (x XMLMPUParts) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }
func (x XMLMPUParts) Less(i, j int) bool { return x[i].PartNumber < x[j].PartNumber }

type Option func(m *XMLMPU) error

const (
	defaultChunkSize       = 5 * 1024 * 1024 // 5 MB
	defaultRetry           = 3               // 建议默认3~5次，不要太大，否则将可能会导致成为僵尸任务。
	defaultSignedURLExpiry = 1 * time.Hour

	MinimumChunkSize = 5 * 1024 * 1024               // 5 MB
	MaximumChunkSize = 5 * 1024 * 1024 * 1024        // 5 GB
	MaximumFileSize  = 5 * 1024 * 1024 * 1024 * 1024 // 5 TB
	MaxUploadSession = 7 * time.Hour * 24            // 7 days
	MaximumParts     = 10000

	GOOGLEAPPLICATIONCREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS" // 环境变量中指定的凭证文件路径
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
func NewXMLMPU(ctx context.Context, bucket, blob string, body io.Reader, options ...Option) (m XMLMPU, err error) {
	// Check if uploadFile exists
	if body == nil {
		return XMLMPU{}, fmt.Errorf("upload body is nil")
	}

	m = XMLMPU{
		UploadBase: UploadBase{
			ctx:             ctx,
			bucket:          bucket,
			blob:            blob,
			retry:           defaultRetry,
			signedURLExpiry: defaultSignedURLExpiry,
			log:             NewLogger(nil, false),
		},
		chunkSize: defaultChunkSize,
		workers:   runtime.NumCPU(),
		reader:    body,
	}

	for _, option := range options {
		err = option(&m)
		if err != nil {
			return
		}
	}

	if m.workers < 1 {
		m.workers = 1
	}

	if m.chunkSize < MinimumChunkSize || m.chunkSize > MaximumChunkSize {
		err := fmt.Errorf("Invalid chunk size: %d. Chunk size must be between %d and %d", m.chunkSize, MinimumChunkSize, MaximumChunkSize)
		return XMLMPU{}, err
	}

	// export GOOGLE_APPLICATION_CREDENTIALS=/Users/liqiuqing/work/gcp/cred.json
	if m.cli == nil {
		_, exists := os.LookupEnv(GOOGLEAPPLICATIONCREDENTIALS)
		if !exists {
			return XMLMPU{}, fmt.Errorf("environment variable %s is not set", GOOGLEAPPLICATIONCREDENTIALS)
		}
		m.cli, err = storage.NewClient(ctx)
		if err != nil {
			return XMLMPU{}, fmt.Errorf("storage.NewClient: %s", err)
		}
	}

	return m, nil
}
func WithCredentialsFile(credFile string) Option {
	return func(m *XMLMPU) error {
		cli, err := storage.NewClient(m.ctx, option.WithCredentialsFile(credFile))
		if err != nil {
			return fmt.Errorf("storage.NewClient with %s : %s", credFile, err)
		}
		m.cli = cli
		return nil
	}
}

func WithStorageClient(cli *storage.Client) Option {
	return func(m *XMLMPU) error {
		m.cli = cli
		return nil
	}
}

func WithBucket(bucket string) Option {
	return func(m *XMLMPU) error {
		m.bucket = bucket
		return nil
	}
}

func WithBlob(blob string) Option {
	return func(m *XMLMPU) error {
		m.blob = blob
		return nil
	}
}

func WithChunkSize(chunkSize int64) Option {
	return func(m *XMLMPU) error {
		m.chunkSize = chunkSize
		return nil
	}
}

func WithWorkers(workers int) Option {
	return func(m *XMLMPU) error {
		m.workers = workers
		return nil
	}
}

func WithRetry(retry int) Option {
	return func(m *XMLMPU) error {
		m.retry = retry
		return nil
	}
}

func DisableReaderAtSeeker() Option {
	return func(m *XMLMPU) error {
		m.disableReaderAtSeeker = true
		return nil
	}
}

func WithLog(log LoggerInterface, debug bool) Option {
	return func(m *XMLMPU) error {
		m.log = NewLogger(log, debug)
		return nil
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
	sort.Sort(m.XMLMPUParts)
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

	opts := &storage.SignedURLOptions{
		Scheme:  storage.SigningSchemeV4,
		Method:  "POST",
		Expires: time.Now().Add(m.signedURLExpiry),
		//ContentType:     "application/xml",
		QueryParameters: url.Values{MPUUploadIDQuery: []string{m.uploadID}},
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

func computeSeekerLength(s io.Seeker) (int64, error) {
	curOffset, err := s.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	endOffset, err := s.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	_, err = s.Seek(curOffset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return endOffset - curOffset, nil
}

func readFillBuf(r io.Reader, b []byte) (offset int, err error) {
	for offset < len(b) && err == nil {
		var n int
		n, err = r.Read(b[offset:])
		offset += n
	}

	return offset, err
}

type readerAtSeeker interface {
	io.ReaderAt
	io.ReadSeeker
}

func (m *XMLMPU) nextReader() (io.ReadSeeker, int, func(), error) {

	if r, ok := m.reader.(readerAtSeeker); ok && !m.disableReaderAtSeeker {
		m.log.Debugf("Read chunk by readerAtSeeker")
		var err error

		n := m.chunkSize
		if m.totalSize >= 0 {
			bytesLeft := m.totalSize - m.readerPos

			if bytesLeft <= m.chunkSize {
				err = io.EOF
				n = bytesLeft
			}
		}

		var (
			reader  io.ReadSeeker
			cleanup func()
		)

		reader = io.NewSectionReader(r, m.readerPos, n)

		cleanup = func() {}

		m.readerPos += n

		return reader, int(n), cleanup, err
	} else {
		m.log.Debugf("Read chunk by slicePool")
		part, err := m.slicePool.Get(m.ctx)
		if err != nil {
			return nil, 0, func() {}, err
		}
		m.log.Infof("pool %p", part)

		n, err := readFillBuf(r, *part)
		m.readerPos += int64(n)

		cleanup := func() {
			m.slicePool.Put(part)
		}

		return bytes.NewReader((*part)[0:n]), n, cleanup, err
	}

}

// keeps track of a single chunk of data being sent to S3.
type chunk struct {
	buf     io.ReadSeeker
	num     int
	cleanup func()
}

// geterr is a thread-safe getter for the error object
func (m *XMLMPU) geterr() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.err
}

// seterr is a thread-safe setter for the error object
func (m *XMLMPU) seterr(e error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.err = e
}
func (m *XMLMPU) appendMPUPart(part *XMLMPUPart) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.XMLMPUParts = append(m.XMLMPUParts, part)
}

func (m *XMLMPU) readChunk(ch chan chunk) {
	defer m.wg.Done()
	for {
		data, ok := <-ch

		if !ok {
			break
		}
		m.log.Debugf("Read chunk %d", data.num)

		select {
		case <-m.ctx.Done():
			data.cleanup()
			return
		default:
			part := &XMLMPUPart{
				UploadBase: m.UploadBase.Clone(),
				UploadID:   m.uploadID,
				reader:     data.buf,
				PartNumber: int(data.num),
				Checksum:   "",
			}
			if m.geterr() == nil {
				if err := part.Upload(); err != nil {
					m.seterr(err)
				}
			}
			m.appendMPUPart(part)
			data.cleanup()
		}

	}
}

func (m *XMLMPU) UploadChunksConcurrently() (result FinalizeXMLMPUResult, err error) {

	m.log.Debug("UploadChunksConcurrently start")
	if seeker, ok := m.reader.(io.Seeker); ok {
		m.totalSize, err = computeSeekerLength(seeker)
		if err != nil {
			return
		}
		numParts := (m.totalSize + int64(m.chunkSize) - 1) / int64(m.chunkSize)
		if numParts > MaximumParts {
			return result, fmt.Errorf("File size %d is too large to upload in %d parts", m.totalSize, numParts)
		}

	}

	m.log.Debug("InitiateXMLMPU start")
	err = m.InitiateXMLMPU()
	if err != nil {
		err = fmt.Errorf("Failed to initiate XMLMPU: %s", err)
		return
	}

	m.slicePool = newByteSlicePool(int64(m.chunkSize))
	m.slicePool.ModifyCapacity(m.workers + 1)
	defer m.slicePool.Close()

	ch := make(chan chunk, m.workers)
	for i := 0; i < m.workers; i++ {
		m.wg.Add(1)
		go m.readChunk(ch)
	}
	num := 1

	for m.geterr() == nil && err == nil {
		var (
			reader       io.ReadSeeker
			cleanup      func()
			nextChunkLen int
		)

		if num > MaximumParts {
			m.seterr(fmt.Errorf("File size %d is too large to upload in %d parts", m.totalSize, num))
			return
		}

		reader, nextChunkLen, cleanup, err = m.nextReader()
		if err != nil && err != io.EOF {
			return result, fmt.Errorf("Failed to read next chunk: %s", err)
		}

		if nextChunkLen == 0 {
			cleanup()
			break
		}

		ch <- chunk{
			buf:     reader,
			num:     num,
			cleanup: cleanup,
		}

		num++
	}
	close(ch)
	m.wg.Wait()

	if m.geterr() != nil {
		err = fmt.Errorf("Failed to upload parts: %s", m.geterr())
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

type XMLMPUPart struct {
	UploadBase
	reader     io.ReadSeeker
	UploadID   string
	PartNumber int
	Checksum   string
	etag       string
	finished   bool
}

func (p *XMLMPUPart) Clone() *XMLMPUPart {
	return &XMLMPUPart{
		UploadBase: p.UploadBase.Clone(),
		UploadID:   p.UploadID,
		reader:     p.reader,
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

	req, err := http.NewRequest("PUT", u, p.reader)
	if err != nil {
		return fmt.Errorf("PUT request failed: %s", err)
	}
	req = req.WithContext(p.ctx)

	client := &http.Client{
		Transport: createTransport(nil),
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT request failed: %s", err)
	}
	defer resp.Body.Close()

	p.log.Debugf("Upload PartNumber '%d' end", p.PartNumber)
	p.etag = resp.Header.Get("ETag")

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		p.log.Errorf("Upload response body: %s", string(body))
		return fmt.Errorf("PUT request returned non-OK status: %d", resp.StatusCode)
	}
	p.finished = true
	p.log.Infof("Uploaded part %d, Etag:%s", p.PartNumber, p.etag)

	return nil
}

func createTransport(localAddr net.Addr) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}
	if localAddr != nil {
		dialer.LocalAddr = localAddr
	}
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
	}
}
