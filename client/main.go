package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"grcp_exer3/grcp-lesson/pb"
	"io"
	"log"
	"os"
	"time"
)

func main() {
	// ここはセキュリティが暗号化されていない
	// 本番運用時はSSLの運用をする
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	//file servese client
	client := pb.NewFileServiceClient(conn)
	//callListFiles(client)
	//callDownload(client)
	//CallUpload(client)
	CallUploadAndNotifyProgress(client)
}

func callListFiles(client pb.FileServiceClient) {
	res, err := client.ListFiles(context.Background(), &pb.ListFilesRequest{})

	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println(res.GetFilenames())
}

func callDownload(client pb.FileServiceClient) {
	req := &pb.DownloadRequest{Filename: "name.txt"}
	stream, err := client.Download(context.Background(), req)
	if err != nil {
		log.Fatalln(err)
	}

	//responseを受け取る
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("Response from Download(butes): %v", res.GetData())
		log.Printf("Response from Download(string): %v", string(res.GetData()))
	}
}

func CallUpload(client pb.FileServiceClient) {
	filename := "sports.txt"
	path := "/Users/matsudomasato/go/src/grcp_exer3/grcp-lesson/storage/" + filename

	file, err := os.Open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	stream, err := client.Upload(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	//5はデータ格納用のbuffer

	buf := make([]byte, 5)
	for {
		n, err := file.Read(buf)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln(err)
		}

		req := &pb.UploadRequest{Data: buf[:n]}

		fmt.Println(req)
		sendErr := stream.Send(req)
		if sendErr != nil {
			log.Fatalln(sendErr)
		}
		time.Sleep(1 * time.Second)
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("receieved data size: %v", res.GetSize())
}

func CallUploadAndNotifyProgress(client pb.FileServiceClient) {
	filename := "sports.txt"
	path := "/Users/matsudomasato/go/src/grcp_exer3/grcp-lesson/storage/" + filename

	file, err := os.Open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	stream, err := client.UploadAndNotifyProgress(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	//リクエスト側

	buf := make([]byte, 5)
	go func() {
		for {
			n, err := file.Read(buf)
			//	データの読み込みが終わったら
			if n == 0 || err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln(err)
			}

			req := &pb.UploadAndNotifyProgressRequest{Data: buf[:n]}
			sendErr := stream.Send(req)
			if sendErr != nil {
				log.Fatalln(sendErr)
			}
			time.Sleep(1 * time.Second)
		}

		//for文抜けたら、リクエストの終了を通知
		err := stream.CloseSend()
		if err != nil {
			log.Fatalln(err)
		}

	}()

	//レスポンス側
	//チャネル作成
	ch := make(chan struct{})
	go func() {
		for {
			//サーバー側からのレスポンスを受け取る
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln(err)
			}

			log.Printf("receieved message: %v", res.GetMsg())
		}
		close(ch)
	}()

	<-ch

}
