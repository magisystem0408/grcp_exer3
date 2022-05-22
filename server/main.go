package main

import (
	"bytes"
	"context"
	"fmt"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return status.Error(codes.NotFound, "file was not found")
	}

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
		time.Sleep(5 * time.Second)
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

//送る前に何か処理を入れる
func myLogging() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		log.Printf("[interceptor] request data: %+v", req)

		resp, err = handler(ctx, req)
		if err != nil {
			return nil, err
		}
		log.Printf("response data: %")

		return resp, nil
	}
}

func authorize(ctx context.Context) (context.Context, error) {
	token, err := grpc_auth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return nil, err
	}
	if token != "test-token" {
		return nil, status.Error(codes.Unauthenticated, "token is invalid")
	}

	return ctx, nil
}

func main() {
	lis, err := net.Listen("tcp", "localhost:50051")
	if err != nil {
		log.Fatal("Failed to listen: %v", err)
	}

	s := grpc.NewServer(grpc.UnaryInterceptor(
		grpc_middleware.ChainUnaryServer(
			myLogging(),
			grpc_auth.UnaryServerInterceptor(authorize),
		)))

	//gcpに登録
	pb.RegisterFileServiceServer(s, &server{})

	fmt.Println("Server is running...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
