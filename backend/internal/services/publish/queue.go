package publish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	dbrouter "github.com/kurodakayn/mpp-backend/internal/db"
	"github.com/kurodakayn/mpp-backend/internal/models"
	"github.com/kurodakayn/mpp-backend/internal/pkg/rediskey"
	"github.com/kurodakayn/mpp-backend/internal/publisher"
	platformaccount "github.com/kurodakayn/mpp-backend/internal/services/platform_account"
)

const (
	publishTaskType         = "publish:project"
	publishQueueName        = "publish"
	publishTaskMaxRetry     = 3
	publishTaskTimeout      = 30 * time.Minute
	publishTaskRetention    = 24 * time.Hour
	publishWorkerConcurrent = 2
	publishLockKeyPrefix    = "mpp:publish:lock:"
	publishLockTTL          = 30 * time.Minute
	publishLockRefreshEvery = publishLockTTL / 3
	publishStaleAfter       = 2 * publishLockTTL
	publishReplayWait       = 2 * time.Second
	publishReplayPoll       = 25 * time.Millisecond
	publishCleanupTimeout   = 2 * time.Second
)

var (
	ErrPublicationAlreadyPublishing = errors.New("publication is already publishing")
	ErrPublishQueueEmpty            = errors.New("publish queue empty")
	errPublishLockOwnershipLost     = errors.New("publish lock ownership lost")
	errPublishJobWorkspaceMismatch  = errors.New("publish job workspace does not match project")
	publishLockRefreshInterval      = publishLockRefreshEvery
)

type PublishJob struct {
	JobID          uuid.UUID `json:"job_id"`
	ProjectID      uuid.UUID `json:"project_id"`
	WorkspaceID    uuid.UUID `json:"workspace_id"`
	UserID         uuid.UUID `json:"user_id"`
	Platform       string    `json:"platform"`
	PublicationID  uuid.UUID `json:"publication_id,omitempty"`
	ScheduleID     uuid.UUID `json:"schedule_id,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	// Kept only so old Redis payloads still unmarshal; publishing never reuses live browser sessions.
	BrowserSessionID uuid.UUID `json:"browser_session_id,omitempty"`
	EnqueuedAt       time.Time `json:"enqueued_at"`
}

type PublishRequest struct {
	IdempotencyKey string
}

type PublishJobHandler func(context.Context, PublishJob) error

type PublishQueue interface {
	Enqueue(ctx context.Context, job PublishJob) error
	Start(ctx context.Context, handler PublishJobHandler)
	AcquireLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	LockValue(ctx context.Context, key string) (string, error)
	RefreshLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, key, value string) error
}

type RedisPublishQueue struct {
	redisClient redis.UniversalClient
	asynqClient *asynq.Client
}

func NewRedisPublishQueue(client redis.UniversalClient) *RedisPublishQueue {
	return &RedisPublishQueue{
		redisClient: client,
		asynqClient: asynq.NewClientFromRedisClient(client),
	}
}

func (q *RedisPublishQueue) Enqueue(ctx context.Context, job PublishJob) error {
	task, err := newPublishTask(job)
	if err != nil {
		return err
	}
	_, err = q.asynqClient.EnqueueContext(
		ctx,
		task,
		asynq.Queue(publishQueueName),
		asynq.MaxRetry(publishTaskMaxRetry),
		asynq.Timeout(publishTaskTimeout),
		asynq.Retention(publishTaskRetention),
		asynq.TaskID(job.JobID.String()),
	)
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		return nil
	}
	return err
}

func (q *RedisPublishQueue) Start(ctx context.Context, handler PublishJobHandler) {
	if err := q.Run(ctx, handler); err != nil && ctx.Err() == nil {
		log.Printf("publish worker stopped with error: %v", err)
	}
}

func (q *RedisPublishQueue) Run(ctx context.Context, handler PublishJobHandler) error {
	server := asynq.NewServerFromRedisClient(q.redisClient, asynq.Config{
		Concurrency: publishWorkerConcurrent,
		Queues: map[string]int{
			publishQueueName: 1,
		},
	})
	mux := asynq.NewServeMux()
	mux.HandleFunc(publishTaskType, func(taskCtx context.Context, task *asynq.Task) error {
		job, err := publishJobFromTask(task)
		if err != nil {
			return fmt.Errorf("invalid publish task payload: %w: %w", err, asynq.SkipRetry)
		}
		return handler(taskCtx, job)
	})

	go func() {
		<-ctx.Done()
		server.Shutdown()
	}()

	if err := server.Run(mux); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}

func newPublishTask(job PublishJob) (*asynq.Task, error) {
	payload, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(publishTaskType, payload), nil
}

func publishJobFromTask(task *asynq.Task) (PublishJob, error) {
	var job PublishJob
	if err := json.Unmarshal(task.Payload(), &job); err != nil {
		return PublishJob{}, err
	}
	return job, nil
}

func (q *RedisPublishQueue) AcquireLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return q.redisClient.SetNX(ctx, key, value, ttl).Result()
}

func (q *RedisPublishQueue) LockValue(ctx context.Context, key string) (string, error) {
	value, err := q.redisClient.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return value, err
}

func (q *RedisPublishQueue) RefreshLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	const refreshLockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`
	result, err := q.redisClient.Eval(ctx, refreshLockScript, []string{key}, value, ttl.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (q *RedisPublishQueue) ReleaseLock(ctx context.Context, key, value string) error {
	const releaseLockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`
	return q.redisClient.Eval(ctx, releaseLockScript, []string{key}, value).Err()
}

func (s *Service) EnqueuePublishProject(ctx context.Context, projectID uuid.UUID, platform string, scopeUserID *uuid.UUID, req PublishRequest) (PublishResponse, error) {
	if scopeUserID == nil {
		return PublishResponse{}, ErrForbidden
	}
	req.IdempotencyKey = normalizeIdempotencyKey(req.IdempotencyKey)
	project, projectErr := s.projectForPublish(ctx, projectID, *scopeUserID)
	if projectErr != nil {
		return PublishResponse{}, projectErr
	}
	workspaceID := models.ProjectWorkspaceID(project)
	if req.IdempotencyKey != "" {
		if resp, ok, err := s.findIdempotentPublishResponseForWorkspace(workspaceID, project.ID, platform, *scopeUserID, req.IdempotencyKey); err != nil {
			return PublishResponse{}, err
		} else if ok {
			return resp, nil
		}
	}
	project, pub, err := s.preparePublishJobForProject(ctx, project, platform, *scopeUserID)
	if err != nil {
		if req.IdempotencyKey != "" {
			if resp, ok, lookupErr := s.findIdempotentPublishResponseForWorkspace(workspaceID, project.ID, platform, *scopeUserID, req.IdempotencyKey); lookupErr != nil {
				return PublishResponse{}, lookupErr
			} else if ok {
				return resp, nil
			}
		}
		return PublishResponse{}, err
	}
	scheduledAt := time.Now().UTC()

	if s.queue == nil {
		schedule, err := s.createScheduledPublication(ctx, project, pub, *scopeUserID, req.IdempotencyKey, scheduledAt, models.ScheduledPublicationStatusScheduled)
		if err != nil {
			return PublishResponse{}, err
		}
		resp, err := s.PublishProjectInWorkspaceWithContext(ctx, workspaceID, project.ID, platform, scopeUserID, schedule.ID)
		if err != nil {
			return PublishResponse{}, err
		}
		if schedule.ID != uuid.Nil {
			resp.ScheduledPublicationID = schedule.ID.String()
			resp.ScheduledAt = &schedule.ScheduledAt
		}
		return resp, nil
	}

	job := PublishJob{
		JobID:          uuid.New(),
		ProjectID:      project.ID,
		WorkspaceID:    workspaceID,
		UserID:         *scopeUserID,
		Platform:       platform,
		PublicationID:  pub.ID,
		IdempotencyKey: req.IdempotencyKey,
		EnqueuedAt:     scheduledAt,
	}
	lockKey := publishLockKey(project.ID, platform)
	coordinationQueue := s.coordinationQueueOrDefault()
	if coordinationQueue == nil {
		return PublishResponse{}, ErrPublishQueueEmpty
	}
	acquired, err := coordinationQueue.AcquireLock(ctx, lockKey, job.JobID.String(), publishLockTTL)
	if err != nil {
		return PublishResponse{}, err
	}
	if !acquired {
		if req.IdempotencyKey != "" {
			if resp, ok, err := s.waitForIdempotentPublishResponseForWorkspace(ctx, workspaceID, project.ID, platform, *scopeUserID, req.IdempotencyKey); err != nil {
				return PublishResponse{}, err
			} else if ok {
				return resp, nil
			}
		}
		return PublishResponse{}, ErrPublicationAlreadyPublishing
	}

	var schedule models.ScheduledPublication
	outboxEventID := uuid.Nil
	if err := s.writerDB(ctx).Transaction(func(tx *gorm.DB) error {
		txService := *s
		txService.db = tx
		txService.router = dbrouter.NewRouter(tx)
		createdSchedule, err := txService.createScheduledPublication(ctx, project, pub, *scopeUserID, req.IdempotencyKey, scheduledAt, models.ScheduledPublicationStatusScheduled)
		if err != nil {
			return err
		}
		schedule = createdSchedule
		job.ScheduleID = schedule.ID
		if err := txService.recordPublishEvent(models.PublishEvent{
			PublicationID:  pub.ID,
			WorkspaceID:    job.WorkspaceID,
			ProjectID:      project.ID,
			UserID:         *scopeUserID,
			Platform:       platform,
			JobID:          job.JobID,
			IdempotencyKey: req.IdempotencyKey,
			EventType:      "requested",
			Status:         pub.Status,
		}); err != nil {
			return err
		}
		if err := txService.recordProjectPublishActivityForWorkspace(job.WorkspaceID, project.ID, *scopeUserID, models.ProjectActivityPublishRequested, map[string]any{
			"platform": platform,
			"job_id":   job.JobID.String(),
		}); err != nil {
			return err
		}
		if err := txService.lifecycle().MarkQueued(&pub, job.EnqueuedAt); err != nil {
			return err
		}
		if err := txService.recordPublishEvent(models.PublishEvent{
			PublicationID:  pub.ID,
			WorkspaceID:    job.WorkspaceID,
			ProjectID:      project.ID,
			UserID:         *scopeUserID,
			Platform:       platform,
			JobID:          job.JobID,
			IdempotencyKey: req.IdempotencyKey,
			EventType:      "queued",
			Status:         models.PublicationStatusQueued,
		}); err != nil {
			return err
		}
		if err := txService.recordProjectPublishActivityForWorkspace(job.WorkspaceID, project.ID, *scopeUserID, models.ProjectActivityPublishQueued, map[string]any{
			"platform": platform,
			"job_id":   job.JobID.String(),
		}); err != nil {
			return err
		}
		outboxEvent, err := txService.recordPublishJobOutbox(job)
		if err != nil {
			return err
		}
		outboxEventID = outboxEvent.ID
		return nil
	}); err != nil {
		_ = coordinationQueue.ReleaseLock(ctx, lockKey, job.JobID.String())
		return PublishResponse{}, err
	}
	s.invalidateDashboardCaches(ctx)
	s.refreshProjectReadModel(ctx, project.ID)
	if err := s.dispatchOutboxEvent(ctx, outboxEventID); err != nil {
		log.Printf("failed to dispatch publish outbox event %s for job %s: %v", outboxEventID, job.JobID, err)
	}

	resp := PublishResponse{
		Status:         models.PublicationStatusQueued,
		JobID:          job.JobID.String(),
		IdempotencyKey: req.IdempotencyKey,
		Platform:       platform,
		QueuedAt:       &job.EnqueuedAt,
		PublishURL:     pub.PublishURL,
	}
	if schedule.ID != uuid.Nil {
		resp.ScheduledPublicationID = schedule.ID.String()
		resp.ScheduledAt = &schedule.ScheduledAt
	}
	return resp, nil
}

func (s *Service) BatchEnqueuePublishProject(ctx context.Context, projectID uuid.UUID, platforms []string, scopeUserID *uuid.UUID, req PublishRequest) (map[string]PublishResponse, error) {
	results := make(map[string]PublishResponse)
	for _, platform := range platforms {
		platformReq := req
		if platformReq.IdempotencyKey != "" {
			platformReq.IdempotencyKey = platformReq.IdempotencyKey + ":" + platform
		}
		resp, err := s.EnqueuePublishProject(ctx, projectID, platform, scopeUserID, platformReq)
		if err != nil {
			results[platform] = publishErrorResponse(err)
			continue
		}
		results[platform] = resp
	}
	return results, nil
}

func (s *Service) StartPublishWorker(ctx context.Context) {
	if s.queue == nil {
		return
	}

	s.StartPublishOutboxDispatcher(ctx)
	s.StartScheduledPublicationDispatcher(ctx)
	go s.queue.Start(ctx, s.processPublishJob)
}

func (s *Service) StartPublishWorkerWithErrors(ctx context.Context) <-chan error {
	if s.queue == nil {
		return nil
	}

	runner, ok := s.queue.(interface {
		Run(context.Context, PublishJobHandler) error
	})
	if !ok {
		s.StartPublishWorker(ctx)
		return nil
	}

	s.StartPublishOutboxDispatcher(ctx)
	s.StartScheduledPublicationDispatcher(ctx)
	workerErrors := make(chan error, 1)
	go func() {
		if err := runner.Run(ctx, s.processPublishJob); err != nil && ctx.Err() == nil {
			workerErrors <- err
		}
	}()
	return workerErrors
}

func (s *Service) processPublishJob(ctx context.Context, job PublishJob) error {
	if err := s.ensurePublishJobWorkspaceID(ctx, &job); err != nil {
		if errors.Is(err, errPublishJobWorkspaceMismatch) {
			log.Printf("discarding publish job %s because workspace does not match project", job.JobID)
			if coordinationQueue := s.coordinationQueueOrDefault(); coordinationQueue != nil && job.JobID != uuid.Nil && job.ProjectID != uuid.Nil && strings.TrimSpace(job.Platform) != "" {
				_ = coordinationQueue.ReleaseLock(context.Background(), publishLockKey(job.ProjectID, job.Platform), job.JobID.String())
			}
			return nil
		}
		log.Printf("failed to resolve workspace for publish job %s: %v", job.JobID, err)
		return err
	}
	if job.JobID == uuid.Nil || job.ProjectID == uuid.Nil || job.WorkspaceID == uuid.Nil || job.UserID == uuid.Nil || strings.TrimSpace(job.Platform) == "" {
		log.Printf("discarding invalid publish job: %+v", job)
		return nil
	}
	coordinationQueue := s.coordinationQueueOrDefault()
	if coordinationQueue == nil {
		return ErrPublishQueueEmpty
	}
	observeJob := func(result string) {
		s.observePublishJob(job.Platform, result)
	}

	lockKey := publishLockKey(job.ProjectID, job.Platform)
	locked, err := s.ensurePublishJobLock(ctx, job, lockKey)
	if err != nil {
		log.Printf("publish lock check failed for job %s: %v", job.JobID, err)
		observeJob(publishJobResultError)
		return err
	}
	if !locked {
		log.Printf("skipping publish job %s because lock is not owned by this job", job.JobID)
		return nil
	}

	publishCtx, cancelPublish := context.WithCancelCause(ctx)
	defer cancelPublish(nil)

	stopRefreshing := s.startPublishLockRefresh(publishCtx, lockKey, job.JobID.String(), func() {
		cancelPublish(errPublishLockOwnershipLost)
	})
	defer stopRefreshing()

	if err := s.recordPublishEvent(models.PublishEvent{
		PublicationID:  job.PublicationID,
		WorkspaceID:    job.WorkspaceID,
		ProjectID:      job.ProjectID,
		UserID:         job.UserID,
		Platform:       job.Platform,
		JobID:          job.JobID,
		IdempotencyKey: job.IdempotencyKey,
		EventType:      "started",
		Status:         models.PublicationStatusPublishing,
	}); err != nil {
		log.Printf("failed to record publish job %s start event: %v", job.JobID, err)
	}

	resp, err := s.PublishProjectInWorkspaceWithContext(publishCtx, job.WorkspaceID, job.ProjectID, job.Platform, &job.UserID, job.ScheduleID)
	if err != nil {
		if errors.Is(context.Cause(publishCtx), errPublishLockOwnershipLost) {
			log.Printf("publish job %s stopped because lock ownership was lost", job.JobID)
			return nil
		}
		log.Printf("publish job %s failed: %v", job.JobID, err)
		observeJob(publishJobResultError)
		cleanupCtx, cancelCleanup := publishCleanupContext(ctx)
		if markErr := s.lifecycle().MarkFailedForWorkspace(cleanupCtx, job.WorkspaceID, job.ProjectID, job.Platform, err.Error()); markErr != nil {
			log.Printf("failed to mark publish job %s as failed: %v", job.JobID, markErr)
		} else {
			s.invalidateDashboardCaches(cleanupCtx)
			s.refreshProjectReadModel(cleanupCtx, job.ProjectID)
		}
		cancelCleanup()
		_ = s.recordPublishEvent(models.PublishEvent{
			PublicationID:  job.PublicationID,
			WorkspaceID:    job.WorkspaceID,
			ProjectID:      job.ProjectID,
			UserID:         job.UserID,
			Platform:       job.Platform,
			JobID:          job.JobID,
			IdempotencyKey: job.IdempotencyKey,
			EventType:      "failed",
			Status:         models.PublicationStatusFailed,
			ErrorMessage:   SanitizeUserFacingErrorMessage(err.Error()),
		})
		if releaseErr := coordinationQueue.ReleaseLock(context.Background(), lockKey, job.JobID.String()); releaseErr != nil {
			log.Printf("publish lock release failed for failed job %s: %v", job.JobID, releaseErr)
		}
		return err
	}
	if resp.Status == models.PublicationStatusFailed {
		message := resp.ErrorMessage
		err := fmt.Errorf("publish job %s failed: %s", job.JobID, message)
		observeJob(publishJobResultError)
		_ = s.recordPublishEvent(models.PublishEvent{
			PublicationID:  job.PublicationID,
			WorkspaceID:    job.WorkspaceID,
			ProjectID:      job.ProjectID,
			UserID:         job.UserID,
			Platform:       job.Platform,
			JobID:          job.JobID,
			IdempotencyKey: job.IdempotencyKey,
			EventType:      "failed",
			Status:         models.PublicationStatusFailed,
			ErrorMessage:   message,
		})
		if releaseErr := coordinationQueue.ReleaseLock(context.Background(), lockKey, job.JobID.String()); releaseErr != nil {
			log.Printf("publish lock release failed for failed job %s: %v", job.JobID, releaseErr)
		}
		return err
	}
	_ = s.recordPublishEvent(models.PublishEvent{
		PublicationID:  job.PublicationID,
		WorkspaceID:    job.WorkspaceID,
		ProjectID:      job.ProjectID,
		UserID:         job.UserID,
		Platform:       job.Platform,
		JobID:          job.JobID,
		IdempotencyKey: job.IdempotencyKey,
		EventType:      "succeeded",
		Status:         models.PublicationStatusSucceeded,
		RemoteID:       resp.RemoteID,
		PublishURL:     resp.PublishURL,
	})

	if err := coordinationQueue.ReleaseLock(ctx, lockKey, job.JobID.String()); err != nil {
		log.Printf("publish lock release failed for job %s: %v", job.JobID, err)
	}
	observeJob(publishJobResultSuccess)
	return nil
}

func (s *Service) ensurePublishJobWorkspaceID(ctx context.Context, job *PublishJob) error {
	if job == nil {
		return nil
	}
	if job.ProjectID == uuid.Nil {
		if job.UserID != uuid.Nil {
			job.WorkspaceID = models.PersonalWorkspaceID(job.UserID)
		}
		return nil
	}

	queryCtx := ctx
	if queryCtx == nil || queryCtx.Err() != nil {
		queryCtx = s.requestContext()
	}
	var project models.Project
	err := s.writerDB(queryCtx).Select("id", "user_id", "workspace_id").First(&project, "id = ?", job.ProjectID).Error
	if err == nil {
		projectWorkspaceID := models.ProjectWorkspaceID(project)
		if job.WorkspaceID != uuid.Nil && job.WorkspaceID != projectWorkspaceID {
			return errPublishJobWorkspaceMismatch
		}
		job.WorkspaceID = projectWorkspaceID
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if job.UserID != uuid.Nil {
			job.WorkspaceID = models.PersonalWorkspaceID(job.UserID)
		}
		return nil
	}
	return err
}

func (s *Service) ensurePublishJobLock(ctx context.Context, job PublishJob, lockKey string) (bool, error) {
	coordinationQueue := s.coordinationQueueOrDefault()
	if coordinationQueue == nil {
		return false, ErrPublishQueueEmpty
	}
	lockValue, err := coordinationQueue.LockValue(ctx, lockKey)
	if err != nil {
		return false, err
	}
	if lockValue == job.JobID.String() {
		return true, nil
	}
	if lockValue != "" {
		return false, nil
	}

	retriable, err := s.publicationRetriableForJob(job.WorkspaceID, job.ProjectID, job.Platform)
	if err != nil {
		return false, err
	}
	if !retriable {
		return false, nil
	}

	return coordinationQueue.AcquireLock(ctx, lockKey, job.JobID.String(), publishLockTTL)
}

func (s *Service) startPublishLockRefresh(ctx context.Context, lockKey, lockValue string, onLostOwnership func()) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(publishLockRefreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				coordinationQueue := s.coordinationQueueOrDefault()
				if coordinationQueue == nil {
					log.Printf("publish lock refresh skipped for %s: coordination queue unavailable", lockKey)
					continue
				}
				refreshed, err := coordinationQueue.RefreshLock(ctx, lockKey, lockValue, publishLockTTL)
				if err != nil {
					log.Printf("publish lock refresh failed for %s: %v", lockKey, err)
					continue
				}
				if !refreshed {
					log.Printf("publish lock refresh skipped for %s because ownership changed", lockKey)
					if onLostOwnership != nil {
						onLostOwnership()
					}
					return
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}

func (s *Service) preparePublishJob(ctx context.Context, projectID uuid.UUID, platform string, userID uuid.UUID) (models.Project, models.ProjectPlatformPublication, error) {
	project, err := s.projectForPublish(ctx, projectID, userID)
	if err != nil {
		return models.Project{}, models.ProjectPlatformPublication{}, ErrForbidden
	}
	return s.preparePublishJobForProject(ctx, project, platform, userID)
}

func (s *Service) preparePublishJobForProject(ctx context.Context, project models.Project, platform string, userID uuid.UUID) (models.Project, models.ProjectPlatformPublication, error) {
	var pub models.ProjectPlatformPublication
	workspaceID := models.ProjectWorkspaceID(project)
	if err := s.strongReadDB(ctx).Where("workspace_id = ? AND project_id = ? AND platform = ?", workspaceID, project.ID, platform).First(&pub).Error; err != nil {
		return models.Project{}, models.ProjectPlatformPublication{}, fmt.Errorf("publication record not found for platform: %s", platform)
	}
	if !pub.Enabled || pub.Status == models.PublicationStatusCancelled {
		return models.Project{}, models.ProjectPlatformPublication{}, ErrPublicationDisabled
	}
	if (pub.Status == models.PublicationStatusQueued || pub.Status == models.PublicationStatusPublishing) && !publicationPublishingStale(pub) {
		return models.Project{}, models.ProjectPlatformPublication{}, ErrPublicationAlreadyPublishing
	}
	if _, err := publisher.Factory.GetPublisher(platform); err != nil {
		return models.Project{}, models.ProjectPlatformPublication{}, err
	}
	if pub.Status == models.PublicationStatusSyncing || (!publicationHasSyncedDraft(pub) && pub.Status != models.PublicationStatusPublishing) {
		return models.Project{}, models.ProjectPlatformPublication{}, ErrPublicationRequiresSync
	}
	if s.accounts != nil {
		if _, err := s.accounts.ResolvePublicationAccount(userID, &pub); err != nil {
			if errors.Is(err, platformaccount.ErrPlatformAccountForbidden) {
				return models.Project{}, models.ProjectPlatformPublication{}, ErrForbidden
			}
			return models.Project{}, models.ProjectPlatformPublication{}, err
		}
	}

	return project, pub, nil
}

func (s *Service) publicationRetriableForJob(workspaceID uuid.UUID, projectID uuid.UUID, platform string) (bool, error) {
	var pub models.ProjectPlatformPublication
	if err := s.writerDB(s.requestContext()).Select("enabled", "status").Where("workspace_id = ? AND project_id = ? AND platform = ?", workspaceID, projectID, platform).First(&pub).Error; err != nil {
		return false, err
	}
	if !pub.Enabled || pub.Status == models.PublicationStatusCancelled {
		return false, nil
	}
	return pub.Status == models.PublicationStatusQueued || pub.Status == models.PublicationStatusPublishing || pub.Status == models.PublicationStatusFailed, nil
}

func publicationPublishingStale(pub models.ProjectPlatformPublication) bool {
	if (pub.Status != models.PublicationStatusQueued && pub.Status != models.PublicationStatusPublishing) || pub.LastAttemptAt == nil {
		return false
	}
	return time.Since(*pub.LastAttemptAt) > publishStaleAfter
}

func publishLockKey(projectID uuid.UUID, platform string) string {
	return publishLockKeyPrefix + rediskey.Tag("project", projectID.String()) + ":" + rediskey.Part(platform)
}

func publishCleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), publishCleanupTimeout)
}
