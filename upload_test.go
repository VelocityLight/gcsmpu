package gcsmpu

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
)

// TestXMLMPUSinglePart 是对 NewXMLMPU 函数的单元测试。
// 它创建一个模拟的存储客户端，并定义了测试输入值。
// 然后调用被测试的函数 NewXMLMPU，并检查是否返回错误。
// 最后调用 xmlMPU.UploadChunksConcurrently 函数，并打印结果。
//
// TestXMLMPUSinglePart is a unit test for the NewXMLMPU function.
// It creates a mock storage client and defines the test input values.
// Then it calls the function being tested, NewXMLMPU, and checks if it returns an error.
// Finally, it calls the xmlMPU.UploadChunksConcurrently function and prints the result.
func TestXMLMPUSinglePart(t *testing.T) {
	ctx := context.Background()

	// 注意，这里需要的是type是service_account的凭证
	// 而不是type是authorized_user的凭证
	// gcloud auth application-default login这个命令生成的凭证是type是authorized_user的凭证
	// 详见：https://cloud.google.com/docs/authentication/production#auth-cloud-implicit-go
	gcsCli, err := storage.NewClient(ctx)

	if err != nil {
		t.Error(err)
		return
	}
	defer gcsCli.Close()

	// Define test input values
	bucket := "polymeric_billing_temp"
	blob := "test/notes.txt"
	uploadFile := "test/notes.txt"

	opts := []Option{
		WithStorageClient(gcsCli),
	}
	f, err := os.Open(uploadFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Call the function being tested
	xmlMPU, err := NewXMLMPU(ctx, bucket, blob, f, opts...)

	// Check if an error occurred
	if err != nil {
		t.Errorf("NewXMLMPU returned an error: %v", err)
		return
	}

	result, err := xmlMPU.UploadChunksConcurrently()
	if err != nil {
		t.Errorf("UploadChunksConcurrently returned an error: %v", err)
		return
	}

	fmt.Printf("%+v\n", result)
}

func TestXMLMPUMultiPart(t *testing.T) {
	ctx := context.Background()

	// 注意，这里需要的是type是service_account的凭证
	// 而不是type是authorized_user的凭证
	// gcloud auth application-default login这个命令生成的凭证是type是authorized_user的凭证
	// 详见：https://cloud.google.com/docs/authentication/production#auth-cloud-implicit-go
	gcsCli, err := storage.NewClient(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	defer gcsCli.Close()

	// Define test input values
	bucket := "polymeric_billing_temp"
	blob := "/test/xml_MPU_test_file"
	uploadFile := "test/xml_MPU_test_file"

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	opts := []Option{}
	opts = append(opts,
		WithStorageClient(gcsCli),
		WithChunkSize(5*1024*1024),
		WithWorkers(5),
		WithLog(logger, true),
	)

	f, err := os.Open(uploadFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Call the function being tested
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
}

func TestSlicePool(t *testing.T) {
	ctx := context.Background()

	// 注意，这里需要的是type是service_account的凭证
	// 而不是type是authorized_user的凭证
	// gcloud auth application-default login这个命令生成的凭证是type是authorized_user的凭证
	// 详见：https://cloud.google.com/docs/authentication/production#auth-cloud-implicit-go
	gcsCli, err := storage.NewClient(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	defer gcsCli.Close()

	// Define test input values
	bucket := "polymeric_billing_temp"
	blob := "/test/xml_MPU_test_file"
	uploadFile := "test/xml_MPU_test_file"

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	opts := []Option{}
	opts = append(opts,
		WithStorageClient(gcsCli),
		WithChunkSize(5*1024*1024),
		WithWorkers(5),
		WithLog(logger, true),
		DisableReaderAtSeeker(),
	)

	f, err := os.Open(uploadFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Call the function being tested
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
}
