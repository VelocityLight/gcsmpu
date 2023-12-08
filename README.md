- # CloudStroage XML Golang 语言API

  ## [Go Versions Supported](#supported-versions)

  Our libraries are compatible with at least the three most recent, major Go
  releases. They are currently compatible with:

  - Go 1.21
  - Go 1.20
  - Go 1.19

  ## Authorization

  `storage.NewClient`默认读取环境变量`GOOGLE_APPLICATION_CREDENTIALS`指定的SA文件

  ```shell
  # 注意，这里需要的是type是service_account的凭证,而不是type是authorized_user的凭证
  # gcloud auth application-default login这个命令生成的凭证是type是authorized_user的凭证
  # 详见：https://cloud.google.com/docs/authentication/production#auth-cloud-implicit-go
  export GOOGLE_APPLICATION_CREDENTIALS=/path/cred.json
  ```
  
  To authorize using a [JSON key file](https://cloud.google.com/iam/docs/managing-service-account-keys),pass[`option.WithCredentialsFile`](https://pkg.go.dev/google.golang.org/api/option#WithCredentialsFile)to the `NewClient` function of the desired package. For example:
  
  ```go
  client, err := storage.NewClient(ctx, option.WithCredentialsFile("keyfile.json"))
  ```

  ## Request Quotas

  > 注意当使用`DisableReaderAtSeeker`禁用seek方法或者没有seek方法时上传数据时的内存消耗
  >
  > 每一个NewXMLMPU占用的最大内存约等于`(并发数+1)*块大小`，注意这并不包括读取文件时的页面缓存
  >
  > 编程时应考虑到实际内存情况，防止内存溢出

  具体要求查看XML API的请求限制 [XML API requests Quotas](https://cloud.google.com/storage/quotas#requests)

  - 块大小默认是5MB，最小限制是5MB，最大是5GB
  - 上传文件的大小最大限制是5TB
  - 上传文件的最大分块是10000块
  - 默认并发数是通过`runtime.NumCPU()`获取，最小是1
  
  ## Commond CLI
  
  - -c 参数用于指定授权的service_account类型的 JSON 密钥文件
  
  - -bucket 参数用于指定存储桶名称
  
  - -file 参数用于指定源文件名
  
  - -blob 参数用于指定目标文件名
  
  - -debug 参数用于启用调试模式
  
  - -log 参数用于输出日志
  
  - -pprofPort 开启pprof监听端口
  
  - -disableReaderAtSeeker 是否禁用ReaderAtSeeker，ReaderAtSeeker底层按照io.NewSectionReader方式读取数据
  
  - -workers 指定最大并发数，默认4
  
  - -chunkSize 指定分块大小，默认5mb
  
  ```
  go run cmd/multipart_upload/main.go -c test/cred.json -bucket gcs_buket_name -blob test/xml_MPU_test_file -file test/xml_MPU_test_file -log -debug -disableReaderAtSeeker -pprofPort 6060 -workers 4
  ```
  
  ## Example Usage
  
  First  `export GOOGLE_APPLICATION_CREDENTIALS=/path/key.json` to use throughout your application:
  
  ```go
    // Define test input values
  	bucket := "polymeric_billing_temp"
  	blob := "/test/xml_MPU_test_file"
  	uploadFile := "test/xml_MPU_test_file"
  
  	logger := logrus.New()
  	logger.SetLevel(logrus.DebugLevel)
  
  	opts := []Option{}
  	opts = append(opts,
  		WithChunkSize(5*1024*1024),
  		WithWorkers(5),
  		WithLog(logger, true),
  		DisableReaderAtSeeker(), // 禁用seek的方法
  	)
  
  	f, err := os.Open(uploadFile)
  	if err != nil {
  		panic(err)
  	}
  	defer f.Close()
  
  	// Call the function being tested
  	// set env GOOGLE_APPLICATION_CREDENTIALS
  	xmlMPU, err := NewXMLMPU(ctx, bucket, blob, f, opts...)
  
  	// Check if an error occurred
  	if err != nil {
  		t.Errorf("NewXMLMPU returned an error: %v", err)
  	}
  
  	result, err := xmlMPU.UploadChunksConcurrently()
  	if err != nil {
  		t.Errorf("UploadChunksConcurrently returned an error: %v", err)
  	}
  
  	fmt.Printf("%+v\n", result)
  ```
  
  
  
  ## Links
  
  - [Go on Google Cloud](https://cloud.google.com/go/home)
  - [Getting started with Go on Google Cloud](https://cloud.google.com/go/getting-started)
  - [XML API multipart uploads](https://cloud.google.com/storage/docs/multipart-uploads)
  - [Initiate a multipart upload](https://cloud.google.com/storage/docs/xml-api/post-object-multipart)
  - [Complete a multipart upload](https://cloud.google.com/storage/docs/xml-api/post-object-complete)
  - [XML API requests Quotas](https://cloud.google.com/storage/quotas#requests)
