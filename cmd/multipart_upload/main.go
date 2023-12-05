package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	"cloud.google.com/go/storage"

	"google.golang.org/api/option"

	"github.com/liqiuqing/gcsmpu"
	"github.com/sirupsen/logrus"
)

// main函数是程序的入口点。
// 它解析命令行参数并执行分块上传操作。
func main() {

	credFile := flag.String("c", "cred.json", "authorize using a JSON key file")     // -c 参数用于指定授权的 JSON 密钥文件
	bucketName := flag.String("bucket", "polymeric_billing_temp", "bucket name")     // -bucket 参数用于指定存储桶名称
	sourceFilename := flag.String("file", "notes.txt", "source file name")           // -file 参数用于指定源文件名
	destinationBlobName := flag.String("blob", "notes.txt", "destination file name") // -blob 参数用于指定目标文件名
	debug := flag.Bool("debug", false, "debug mode")                                 // -debug 参数用于启用调试模式
	outputLog := flag.Bool("log", false, "output log")                               // -log 参数用于输出日志
	flag.Parse()

	ctx := context.Background()

	cli, err := storage.NewClient(ctx, option.WithCredentialsFile(*credFile))
	if err != nil {
		panic(err)
	}

	opts := []gcsmpu.Option{}
	if *outputLog {
		logger := logrus.New()
		logger.SetLevel(logrus.DebugLevel)
		opts = append(opts, gcsmpu.WithLog(logger, *debug))
	}

	m, err := gcsmpu.NewXMLMPU(cli, *bucketName, *destinationBlobName, *sourceFilename, opts...)
	if err != nil {
		fmt.Println(err)
	}
	result, err := m.UploadChunksConcurrently()
	if err != nil {
		panic(err)
	}

	bs, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(bs))
}
