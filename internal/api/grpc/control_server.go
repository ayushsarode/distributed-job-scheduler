package grpc

import (
	"context"
	"sync"

	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	pb "github.com/ayushsarode/distributed-job-scheduler/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ControlServer struct {
	pb.UnimplementedWorkerControlServiceServer
	Workers repository.WorkersRepository
	Log     zerolog.Logger

	mu      sync.Mutex
	drains  map[string]chan struct{}
	cancels map[string]chan struct{}
}

func NewControlServer(workers repository.WorkersRepository, log zerolog.Logger) *ControlServer {
	return &ControlServer{
		Workers: workers,
		Log:     log,
		drains:  make(map[string]chan struct{}),
		cancels: make(map[string]chan struct{}),
	}
}

func (s *ControlServer) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	w, err := s.Workers.Register(ctx, req.Host)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "register: %v", err)
	}
	s.Log.Info().Str("worker_id", w.ID.String()).Msg("worker registered (control plane)")
	return &pb.RegisterWorkerResponse{WorkerId: w.ID.String()}, nil
}

func (s *ControlServer) DrainWorker(ctx context.Context, req *pb.DrainWorkerRequest) (*pb.DrainWorkerResponse, error) {
	s.mu.Lock()
	ch := s.drainSignalLocked(req.WorkerId)
	closeOnce(ch)
	s.mu.Unlock()

	s.Log.Info().Str("worker_id", req.WorkerId).Msg("drain signalled")
	return &pb.DrainWorkerResponse{Acknowledged: true}, nil
}

func (s *ControlServer) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	jobID, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad job_id")
	}

	s.mu.Lock()
	ch := s.cancelSignalLocked(jobID.String())
	closeOnce(ch)
	s.mu.Unlock()

	s.Log.Info().Str("job_id", jobID.String()).Msg("cancel signalled")
	return &pb.CancelJobResponse{Cancelled: true}, nil
}

func (s *ControlServer) DrainSignal(workerID string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drainSignalLocked(workerID)
}

func (s *ControlServer) CancelSignal(jobID string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelSignalLocked(jobID)
}

func (s *ControlServer) drainSignalLocked(workerID string) chan struct{} {
	ch, ok := s.drains[workerID]
	if !ok {
		ch = make(chan struct{})
		s.drains[workerID] = ch
	}
	return ch
}

func (s *ControlServer) cancelSignalLocked(jobID string) chan struct{} {
	ch, ok := s.cancels[jobID]
	if !ok {
		ch = make(chan struct{})
		s.cancels[jobID] = ch
	}
	return ch
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
