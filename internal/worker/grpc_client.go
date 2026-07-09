package worker

import (
	"context"
	"io"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	pb "github.com/ayushsarode/distributed-job-scheduler/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCClient struct {
	conn     *grpc.ClientConn
	client   pb.SchedulerServiceClient
	WorkerID uuid.UUID
	log      zerolog.Logger
}

func NewGRPCClient(schedulerAddr string, log zerolog.Logger) (*GRPCClient, error) {
	conn, err := grpc.NewClient(schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, err
	}

	return &GRPCClient{
		conn:   conn,
		client: pb.NewSchedulerServiceClient(),
		log:    log,
	}, nil
}

func (g *GRPCClient) Register(ctx context.Context, host string) error {
	resp, err := g.client.RegisterWorker(ctx, &pb.RegisterWorkerRequest{Host: host})

	if err != nil {
		return err
	}

	id, err := uuid.Parse(resp.WorkerId)
	if err != nil {
		return err
	}

	g.WorkerID = id

	g.log.Info().Str("worker_id", id.String()).Msg("registered via gRPC")

	return nil
}

func (g *GRPCClient) SubscribeJobs(ctx context.Context, jobChan chan<- *models.Job) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		stream, err := g.client.PushJobs(ctx, &pb.PushJobsRequest{WorkerId: g.WorkerID.String()})

		if err != nil {
			g.log.Error().Err(err).Msg("failed to open job stream, retrying...")
			time.Sleep(2 * time.Second)
			continue
		}
		g.log.Info().Msg("connected to job stream")

		for {
			pbJob, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				g.log.Error().Err(err).Msg("job stream error, reconnecting...")
				break
			}
			jobID, _ := uuid.Parse(pbJob.Id)
			job := &models.Job{
				ID:       jobID,
				Type:     pbJob.Type,
				Payload:  pbJob.Payload,
				Priority: int16(pbJob.Priority),
				Attempts: int(pbJob.Attempts),
			}

			select {
			case jobChan <- job:
				g.log.Debug().Str("job_id", job.ID.String()).Msg("job received via gRPC")

			case <-ctx.Done():
				return
			}
		}
	}
}

func (g *GRPCClient) SendHeartbeat(ctx context.Context, cpu, memory float64, runningJobs int) error {
	_, err := g.client.SendHeartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId:    g.WorkerID.String(),
		Cpu:         cpu,
		Memory:      memory,
		RunningJobs: int32(runningJobs),
	})
	return err
}

func (g *GRPCClient) ReportResult(ctx context.Context, jobID uuid.UUID, success bool, errMsg string) (string, error) {
	resp, err := g.client.ReportResult(ctx, &pb.ResultRequest{
		WorkerId: g.WorkerID.String(),
		JobId:    jobID.String(),
		Success:  success,
		Error:    errMsg,
	})
	if err != nil {
		return "", err
	}
	return resp.NewStatus, nil
}

func (g *GRPCClient) Close() error {
	return g.conn.Close()
}
