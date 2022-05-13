package main

import (
	"bytes"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"grcp_exer3/grcp-lesson/pb"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"
)

type server struct {
	pb.UnimplementedFileServiceServer
}

func (*server) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	fmt.Println("Listfiles was invoked")

	//ここまでがserverの処理で作るもの
	dir := "/Users/matsudomasato/go/src/grcp_exer3/grcp-lesson/storage"
	paths, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var filenames []string
	for _, path := range paths {
		if !path.IsDir() {
			//パスがディレクトリである場合
			filenames = append(filenames, path.Name())
		}
	}

	//grcpにファイルを格納する
	res := &pb.ListFilesResponse{
		Filenames: filenames,
	}
	return res, nil
}

func (*server) Download(req *pb.DownloadRequest, stream pb.FileService_DownloadServer) error {
	fmt.Println("download was invoked")

	filename := req.GetFilename()
	path := "/Users/matsudomasato/go/src/grcp_exer3/grcp-lesson/storage/" + filename

	file, err := os.Open(path)
	if err != nil {
		return err
	}

	defer file.Close()

	//長さが5のbyte型スライスを用意

	buf := make([]byte, 5)
	for {
		// bufに読み込む
		n, err := file.Read(buf)
		//何も読み込まれなかった場合、またはファイルの終端まで到達した場合
		if n == 0 || err == io.EOF {
			//終了する
			break
		}

		if err != nil {
			return err
		}
		//読み込んだ内容をresponseに詰める
		res := &pb.DownloadResponse{Data: buf[:n]}
		sendErr := stream.Send(res)

		if sendErr != nil {
			return sendErr
		}
		time.Sleep(1 * time.Second)
	}

	//responseが終了する
	return nil
}

func (*server) Upload(stream pb.FileService_UploadServer) error {
	fmt.Println("Upload was invoked")

	//サーバーからのバイトを受け取る
	var buf bytes.Buffer
	for {
		req, err := stream.Recv()
		//	終了信号が到達した場合
		if err == io.EOF {
			res := &pb.UploadResponse{Size: int32(buf.Len())}
			return stream.SendAndClose(res)
		}
		if err != nil {
			break
		}

		data := req.GetData()
		log.Printf("received data(bytes):%v", data)

		log.Printf("received data(string):%v", string(data))
		buf.Write(data)
	}
	return nil
}

func (*server) UploadAndNotifyProgress(stream pb.FileService_UploadAndNotifyProgressServer) error {
	fmt.Println("UploadAndNotifyProgress was invoked")
	size := 0
	for {
		req, err := stream.Recv()

		if err != nil {
			return nil
		}

		if err != nil {
			return err
		}

		data := req.GetData()
		log.Printf("received data(bytes):%v", data)

		size += len(data)

		res := &pb.UploadAndNotifyProgressResponse{
			Msg: fmt.Sprintf("received %vbytes", size),
		}

		err = stream.Send(res)

		if err != nil {
			return err
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", "localhost:50051")
	if err != nil {
		log.Fatal("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	//gcpに登録
	pb.RegisterFileServiceServer(s, &server{})

	fmt.Println("Server is running...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
