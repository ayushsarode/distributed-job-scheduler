package worker

import (
	"context"

	pb "github.com/ayushsarode/distributed-job-scheduler/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ControlClient struct {
	client   pb.WorkerControlServiceClient
	WorkerID uuid.UUID
	log      zerolog.Logger
}

func NewControlClient(addr string, log zerolog.Logger) (*ControlClient, error) {
    conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    return &ControlClient{
        client: pb.NewWorkerControlServiceClient(conn),
        log:    log,
    }, nil
}

func (c *ControlClient) Register(ctx context.Context, host string) error {
    resp, err := c.client.RegisterWorker(ctx, &pb.RegisterWorkerRequest{Host: host})
    if err != nil {
        return err
    }
    id, err := uuid.Parse(resp.WorkerId)
    if err != nil {
        return err
    }
    c.WorkerID = id
    return nil
}

