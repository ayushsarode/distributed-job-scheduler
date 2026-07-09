package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	pb "github.com/ayushsarode/distributed-job-scheduler/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SchedulerServer struct {
	pb.UnimplementedSchedulerServiceServer
	Jobs    repository.JobsRepository
	Workers repository.WorkersRepository
	Log     zerolog.Logger
	mu      sync.Mutex
	streams map[string]pb.SchedulerService_PushJobsServer // workerID -> stream
}

func NewSchedulerServer(jobs repository.JobsRepository, workers repository.WorkersRepository, log zerolog.Logger) *SchedulerServer {
	return &SchedulerServer{
		Jobs:    jobs,
		Workers: workers,
		Log:     log,
		streams: make(map[string]pb.SchedulerService_PushJobsServer),
	}
}

func (s *SchedulerServer) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	w, err := s.Workers.Register(ctx, req.Host)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "register worker: %v", err)
	}

	s.Log.Info().Str("worker_id", w.ID.String()).Str("host", req.Host).Msg("worker registered via gRPC")

	return &pb.RegisterWorkerResponse{WorkerId: w.ID.String()}, nil
}

func (s *SchedulerServer) PushJobs(req *pb.PushJobsRequest, stream pb.SchedulerService_PushJobsServer) error {
	workerID := req.WorkerId
	s.Log.Info().Str("worker_id", workerID).Msg("worker subscribed to job stream")

	s.mu.Lock()
	s.streams[workerID] = stream
	s.mu.Unlock()

	<-stream.Context().Done()

	s.mu.Lock()
	delete(s.streams, workerID)
	s.mu.Unlock()

	s.Log.Info().Str("worker_id", workerID).Msg("worker disconnected from job stream")
	return nil
}

func (s *SchedulerServer) PushToWorker(workerID string, job *models.Job) error {
	s.mu.Lock()
	stream, ok := s.streams[workerID]
	s.mu.Unlock()

	if !ok {
		return fmt.Errorf("worker %s not connected", workerID)
	}

	return stream.Send(&pb.Job{
		Id: job.ID.String(),
		Type: job.Type,
		Payload: job.Payload,
		Priority: int32(job.Priority),
		Attempts: int32(job.Attempts),
	})
}

func (s *SchedulerServer) SendHeartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	id, err := uuid.Parse(req.WorkerId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid worker_id: %v", err)
	}

	if err := s.Workers.Heartbeat(ctx, id, req.Cpu, req.Memory, int(req.RunningJobs)); err != nil {
		return nil, status.Errorf(codes.Internal, "heartbeat: %v", err)
	}

	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func(s *SchedulerServer) ReportResult(ctx context.Context, req *pb.ResultRequest)(*pb.ResultResponse, error) {
	jobID, err := uuid.Parse(req.JobId)

	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid job_id: %v", err)
	}

	var newStatus string

	if req.Success {
		if err := s.Jobs.MarkCompleted(ctx, jobID); err != nil {
			return nil, status.Errorf(codes.Internal, "mark completed; %v")
		}
		newStatus = string(models.JobStatusCompleted)
	} else {
		st, err := s.Jobs.MarkFailed(ctx, jobID, 3)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "mark failed: %v", err)
		}
		newStatus = string(st)
	}

	s.Log.Info().Str("job_id", req.JobId).Str("status", newStatus).Msg("result reported")
	return &pb.ResultResponse{NewStatus: newStatus}, nil
}