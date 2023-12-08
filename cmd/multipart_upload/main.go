package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/liqiuqing/gcsmpu"
	"github.com/sirupsen/logrus"
)

// main函数是程序的入口点。
// 它解析命令行参数并执行分块上传操作。
func main() {

	credFile := flag.String("c", "", "authorize using a JSON key file, default $GOOGLE_APPLICATION_CREDENTIALS") // -c 参数用于指定授权的 JSON 密钥文件
	bucketName := flag.String("bucket", "polymeric_billing_temp", "bucket name")                                 // -bucket 参数用于指定存储桶名称
	sourceFilename := flag.String("file", "notes.txt", "source file name")                                       // -file 参数用于指定源文件名
	destinationBlobName := flag.String("blob", "notes.txt", "destination file name")                             // -blob 参数用于指定目标文件名
	debug := flag.Bool("debug", false, "debug mode")                                                             // -debug 参数用于启用调试模式
	outputLog := flag.Bool("log", false, "output log")                                                           // -log 参数用于输出日志
	pprofPort := flag.String("pprofPort", "", "listen pprof port")
	chunkSize := flag.Int64("chunkSize", 5*1024*1024, "chunk size,default 5MB")
	workers := flag.Int("workers", 4, "workers,default 4")
	disableReaderAtSeeker := flag.Bool("disableReaderAtSeeker", false, "disable readerAtSeeker")
	flag.Parse()

	now := time.Now()
	if pprofPort != nil && *pprofPort != "" {
		go func() {
			addr := fmt.Sprintf("localhost:%s", *pprofPort)
			log.Println(http.ListenAndServe(addr, nil))
		}()
	}
	cliOpts := []option.ClientOption{}
	if credFile != nil && *credFile != "" {
		cliOpts = append(cliOpts, option.WithCredentialsFile(*credFile))
	}

	ctx := context.Background()

	opts := []gcsmpu.Option{
		gcsmpu.WithWorkers(*workers),
		gcsmpu.WithChunkSize(*chunkSize),
	}
	if *outputLog {
		logger := logrus.New()
		logger.SetLevel(logrus.DebugLevel)
		opts = append(opts, gcsmpu.WithLog(logger, *debug))
	}
	if credFile != nil && *credFile != "" {
		cli, err := storage.NewClient(ctx, option.WithCredentialsFile(*credFile))
		if err != nil {
			panic(err)
		}
		opts = append(opts, gcsmpu.WithStorageClient(cli))
	}

	if *disableReaderAtSeeker {
		opts = append(opts, gcsmpu.DisableReaderAtSeeker())
	}

	var reader io.Reader

	fi, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	}

	if fi.Mode()&os.ModeNamedPipe == 0 {
		if *sourceFilename == "" {
			panic("source file name is required")
		}
		f, err := os.Open(*sourceFilename)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		reader = f
	} else {
		reader = os.Stdin
		reader = bufio.NewReader(reader)
	}

	m, err := gcsmpu.NewXMLMPU(ctx, *bucketName, *destinationBlobName, reader, opts...)
	if err != nil {
		fmt.Println(err)
	}
	result, err := m.UploadChunksConcurrently()
	if err != nil {
		panic(err)
	}

	bs, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println("result:", string(bs))
	fmt.Println("elapsed:", time.Since(now).String())
}
