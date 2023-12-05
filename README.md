- # CloudStroage XML Golang 语言API

  ## [Go Versions Supported](#supported-versions)

  Our libraries are compatible with at least the three most recent, major Go
  releases. They are currently compatible with:

  - Go 1.21
  - Go 1.20
  - Go 1.19

  ## Authorization

  To authorize using a [JSON key file](https://cloud.google.com/iam/docs/managing-service-account-keys),pass[`option.WithCredentialsFile`](https://pkg.go.dev/google.golang.org/api/option#WithCredentialsFile)to the `NewClient` function of the desired package. For example:

  ```go
  client, err := storage.NewClient(ctx, option.WithCredentialsFile("keyfile.json"))
  ```

  ## Request Quotas

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

  ```
  go run cmd/multipart_upload/main.go -c test/cred.json -bucket polymeric_billing_temp -blob test/xml_MPU_test_file -file test/xml_MPU_test_file -log
  ```

  ## Example Usage

  First create a `storage.Client` to use throughout your application:

  ```go
  ctx := context.Background()
  
  // 注意，这里需要的是type是service_account的凭证
  // 而不是type是authorized_user的凭证
  // gcloud auth application-default login这个命令生成的凭证是type是authorized_user的凭证
  // 详见：https://cloud.google.com/docs/authentication/production#auth-cloud-implicit-go
  client, err := storage.NewClient(ctx, option.WithCredentialsFile("keyfile.json"))
  if err != nil {
      log.Fatalf("Failed to create client: %v", err)
  }
  defer client.Close()
  
  // Define test input values
  bucket := "polymeric_billing_temp"
  blob := "xml_MPU_test_file"
  uploadFile := "xml_MPU_test_file"
  
  logger := logrus.New()
  logger.SetLevel(logrus.DebugLevel)
  
  opts := []Option{}
  opts = append(opts,
      WithChunkSize(5*1024*1024),
      WithWorkers(5),
      WithLog(logger, true),
  )
  
  // Call the function being tested
  xmlMPU, err := NewXMLMPU(client, bucket, blob, uploadFile, opts...)
  
  // Check if an error occurred
  if err != nil {
      log.Fatalf("NewXMLMPU returned an error: %v", err)
  }
  
  result, err := xmlMPU.UploadChunksConcurrently()
  if err != nil {
      log.Fatalf("UploadChunksConcurrently returned an error: %v", err)
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

