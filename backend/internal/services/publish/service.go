package publish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	dbrouter "github.com/kurodakayn/mpp-backend/internal/db"
	"github.com/kurodakayn/mpp-backend/internal/models"
	"github.com/kurodakayn/mpp-backend/internal/pkg/objectstorage"
	"github.com/kurodakayn/mpp-backend/internal/pkg/resilience"
	platformcapabilities "github.com/kurodakayn/mpp-backend/internal/platformcapabilities"
	"github.com/kurodakayn/mpp-backend/internal/publisher"
	"github.com/kurodakayn/mpp-backend/internal/services/accesspolicy"
	browsersession "github.com/kurodakayn/mpp-backend/internal/services/browser_session"
	platformaccount "github.com/kurodakayn/mpp-backend/internal/services/platform_account"
)

var ErrForbidden = accesspolicy.ErrForbidden
var ErrPublicationDisabled = errors.New("publication is disabled")
var ErrPublicationRequiresSync = errors.New("publication requires prepublish sync")
var ErrManualPublishUnsupported = errors.New("manual publish is only supported for x")

var sensitiveErrorQueryParamPattern = regexp.MustCompile(`(?i)(secret|access_token|x-amz-credential|x-amz-signature|x-amz-security-token)=([^&"\s]+)`)

type Service struct {
	db                    *gorm.DB
	router                *dbrouter.Router
	accounts              *platformaccount.Service
	queue                 PublishQueue
	coordinationQueue     PublishQueue
	publishJobObserver    PublishJobObserver
	browserWorkerClient   publisher.BrowserWorkerClient
	browserSessionService *browsersession.BrowserSessionService
	objectStorage         objectstorage.Client
	storageConfig         objectstorage.Config
	dashboardCache        DashboardCacheInvalidator
	readModels            DashboardReadModelUpdater
}

type PublishJobObserver interface {
	ObservePublishJob(platform string, result string)
}

type DashboardCacheInvalidator interface {
	InvalidateDashboardProjectListCache(ctx context.Context)
	InvalidateDashboardStatsCache(ctx context.Context)
}

type DashboardReadModelUpdater interface {
	RefreshProjectAsync(ctx context.Context, projectID uuid.UUID)
	RefreshWorkspaceAsync(ctx context.Context, workspaceID uuid.UUID)
}

const (
	publishJobResultSuccess = "success"
	publishJobResultError   = "error"
)

func NewService(db *gorm.DB, accounts *platformaccount.Service) *Service {
	return NewServiceWithRouter(db, accounts, nil)
}

func NewServiceWithRouter(db *gorm.DB, accounts *platformaccount.Service, router *dbrouter.Router) *Service {
	if router == nil {
		router = dbrouter.NewRouter(db)
	}
	if accounts == nil {
		accounts = platformaccount.NewServiceWithRouter(db, router)
	}
	return &Service{
		db:       db,
		router:   router,
		accounts: accounts,
	}
}

func (s *Service) WithContext(ctx context.Context) *Service {
	if ctx == nil {
		return s
	}
	scoped := *s
	scoped.db = s.db.WithContext(ctx)
	if s.accounts != nil {
		scoped.accounts = s.accounts.WithContext(ctx)
	}
	if s.browserSessionService != nil {
		scoped.browserSessionService = s.browserSessionService.WithContext(ctx)
	}
	return &scoped
}

func (s *Service) requestContext() context.Context {
	if s.db != nil && s.db.Statement != nil && s.db.Statement.Context != nil {
		return s.db.Statement.Context
	}
	return context.Background()
}

func (s *Service) writerDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		ctx = s.requestContext()
	}
	if s.router == nil {
		return publishDBWithContext(s.db, ctx)
	}
	return s.router.Writer(ctx)
}

func (s *Service) strongReadDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		ctx = s.requestContext()
	}
	if s.router == nil {
		return publishDBWithContext(s.db, ctx)
	}
	return s.router.Reader(ctx, dbrouter.StrongRead)
}

func publishDBWithContext(database *gorm.DB, ctx context.Context) *gorm.DB {
	if database == nil || ctx == nil {
		return database
	}
	return database.WithContext(ctx)
}

func (s *Service) SetQueue(queue PublishQueue) {
	s.queue = queue
	if s.coordinationQueue == nil {
		s.coordinationQueue = queue
	}
}

func (s *Service) SetCoordinationQueue(queue PublishQueue) {
	s.coordinationQueue = queue
	if s.queue == nil {
		s.queue = queue
	}
}

func (s *Service) coordinationQueueOrDefault() PublishQueue {
	if s == nil {
		return nil
	}
	if s.coordinationQueue != nil {
		return s.coordinationQueue
	}
	return s.queue
}

func (s *Service) SetPublishJobObserver(observer PublishJobObserver) {
	s.publishJobObserver = observer
}

func (s *Service) SetBrowserWorkerClient(client publisher.BrowserWorkerClient) {
	s.browserWorkerClient = client
}

func (s *Service) SetBrowserSessionService(service *browsersession.BrowserSessionService) {
	s.browserSessionService = service
}

func (s *Service) UseObjectStorage(client objectstorage.Client, config objectstorage.Config) {
	s.objectStorage = client
	s.storageConfig = config
}

func (s *Service) SetDashboardCacheInvalidator(invalidator DashboardCacheInvalidator) {
	s.dashboardCache = invalidator
}

func (s *Service) SetDashboardReadModelUpdater(updater DashboardReadModelUpdater) {
	s.readModels = updater
}

func (s *Service) UseRedis(client redis.UniversalClient) {
	s.UseRedisCoordination(client)
	s.UseRedisQueue(client)
}

func (s *Service) UseRedisCoordination(client redis.UniversalClient) {
	if client == nil {
		return
	}
	s.coordinationQueue = NewRedisPublishQueue(client)
	if s.queue == nil {
		s.queue = s.coordinationQueue
	}
}

func (s *Service) UseRedisQueue(client redis.UniversalClient) {
	if client == nil {
		return
	}
	s.queue = NewRedisPublishQueue(client)
	if s.coordinationQueue == nil {
		s.coordinationQueue = s.queue
	}
}

func (s *Service) observePublishJob(platform string, result string) {
	if s.publishJobObserver != nil {
		s.publishJobObserver.ObservePublishJob(platform, result)
	}
}

func (s *Service) invalidateDashboardCaches(ctx context.Context) {
	if s.dashboardCache == nil {
		return
	}
	s.dashboardCache.InvalidateDashboardProjectListCache(ctx)
	s.dashboardCache.InvalidateDashboardStatsCache(ctx)
}

func (s *Service) invalidateDashboardProjectListCache(ctx context.Context) {
	if s.dashboardCache == nil {
		return
	}
	s.dashboardCache.InvalidateDashboardProjectListCache(ctx)
}

func (s *Service) refreshProjectReadModel(ctx context.Context, projectID uuid.UUID) {
	if s.readModels == nil || projectID == uuid.Nil {
		return
	}
	s.readModels.RefreshProjectAsync(ctx, projectID)
}

func SanitizeUserFacingErrorMessage(message string) string {
	return sensitiveErrorQueryParamPattern.ReplaceAllString(message, "$1=<redacted>")
}

func (s *Service) BatchPublishProject(projectID uuid.UUID, platforms []string, scopeUserID *uuid.UUID) (map[string]PublishResponse, error) {
	results := make(map[string]PublishResponse)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, platform := range platforms {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			resp, err := s.PublishProject(projectID, p, scopeUserID, uuid.Nil)
			mu.Lock()
			if err != nil {
				results[p] = publishErrorResponse(err)
			} else {
				results[p] = resp
			}
			mu.Unlock()
		}(platform)
	}

	wg.Wait()
	return results, nil
}

func (s *Service) PublishProject(projectID uuid.UUID, platform string, scopeUserID *uuid.UUID, scheduleID uuid.UUID) (PublishResponse, error) {
	return s.PublishProjectWithContext(context.Background(), projectID, platform, scopeUserID, scheduleID)
}

func (s *Service) PublishProjectWithContext(ctx context.Context, projectID uuid.UUID, platform string, scopeUserID *uuid.UUID, scheduleID uuid.UUID) (PublishResponse, error) {
	return s.PublishProjectInWorkspaceWithContext(ctx, uuid.Nil, projectID, platform, scopeUserID, scheduleID)
}

func (s *Service) PublishProjectInWorkspaceWithContext(ctx context.Context, workspaceID uuid.UUID, projectID uuid.UUID, platform string, scopeUserID *uuid.UUID, scheduleID uuid.UUID) (PublishResponse, error) {
	// Remote browser sessions are only for account connection and cookie capture.
	// Publish jobs must be durable across Redis workers, so they load saved credentials instead.
	if ctx == nil {
		ctx = context.Background()
	}
	s = s.WithContext(ctx)

	if scopeUserID == nil {
		return PublishResponse{}, ErrForbidden
	}
	proj, err := s.projectForPublish(ctx, projectID, *scopeUserID)
	if err != nil {
		return PublishResponse{}, err
	}
	projectWorkspaceID := models.ProjectWorkspaceID(proj)
	if workspaceID == uuid.Nil {
		workspaceID = projectWorkspaceID
	} else if workspaceID != projectWorkspaceID {
		return PublishResponse{}, ErrForbidden
	}

	var pub models.ProjectPlatformPublication
	if err := s.strongReadDB(ctx).Where("workspace_id = ? AND project_id = ? AND platform = ?", workspaceID, projectID, platform).First(&pub).Error; err != nil {
		return PublishResponse{}, fmt.Errorf("publication record not found for platform: %s", platform)
	}
	if !pub.Enabled || pub.Status == models.PublicationStatusCancelled {
		return PublishResponse{}, ErrPublicationDisabled
	}

	startedAt := time.Now().UTC()
	lifecycle := s.lifecycle()
	attempt, hasAttempt, err := lifecycle.StartPublishAttemptForWorkspace(workspaceID, scheduleID, startedAt)
	if err != nil {
		return PublishResponse{}, err
	}
	failAttempt := func(err error) error {
		if hasAttempt {
			_ = lifecycle.FinishPublishAttemptForWorkspace(workspaceID, &attempt, publishAttemptCompletion{
				Status:       models.PublishAttemptStatusFailed,
				ErrorMessage: SanitizeUserFacingErrorMessage(err.Error()),
			})
		}
		return err
	}

	p, err := publisher.Factory.GetPublisher(platform)
	if err != nil {
		return PublishResponse{}, failAttempt(err)
	}
	if pub.Status == models.PublicationStatusSyncing || (!publicationHasSyncedDraft(pub) && pub.Status != models.PublicationStatusQueued && pub.Status != models.PublicationStatusPublishing) {
		return PublishResponse{}, failAttempt(ErrPublicationRequiresSync)
	}

	if err := s.accounts.ApplySavedCredentialsToPublication(*scopeUserID, &pub); err != nil {
		if errors.Is(err, platformaccount.ErrPlatformAccountForbidden) {
			return PublishResponse{}, failAttempt(ErrForbidden)
		}
		return PublishResponse{}, failAttempt(err)
	}
	pub, err = s.preparePublicationMediaRefs(ctx, proj, pub)
	if err != nil {
		return PublishResponse{}, failAttempt(err)
	}

	var account models.PlatformAccount
	if pub.PlatformAccountID != nil && *pub.PlatformAccountID != uuid.Nil {
		if err := s.strongReadDB(ctx).Where("id = ? AND workspace_id = ?", *pub.PlatformAccountID, workspaceID).First(&account).Error; err != nil {
			return PublishResponse{}, err
		}
	}
	if usesStoredBrowserCookies(platform) {
		if account.ID == uuid.Nil {
			return PublishResponse{}, failAttempt(fmt.Errorf("%w: %s account is not connected", platformaccount.ErrInvalidPlatformAccount, platform))
		}
		if err := s.applySavedBrowserCookies(ctx, *scopeUserID, platform, &account); err != nil {
			return PublishResponse{}, failAttempt(err)
		}
	}

	if err := lifecycle.MarkPublishing(&pub, startedAt); err != nil {
		return PublishResponse{}, failAttempt(err)
	}
	s.invalidateDashboardCaches(ctx)
	s.refreshProjectReadModel(ctx, projectID)

	var remoteID string
	var publishURL string
	publishPolicy := resilience.DefaultOperationPolicy("publish-" + platform)
	publishPolicy.MaxAttempts = 1
	err = resilience.Run(
		ctx,
		publishPolicy,
		func(ctx context.Context) error {
			var publishErr error
			remoteID, publishURL, publishErr = p.Publish(ctx, &pub, &account)
			return publishErr
		},
	)

	status := models.PublicationStatusSucceeded
	errMsg := ""
	if err != nil {
		status = models.PublicationStatusFailed
		errMsg = SanitizeUserFacingErrorMessage(err.Error())
	}

	response := PublishResponse{
		Status:           status,
		RemoteID:         remoteID,
		PublishURL:       publishURL,
		ErrorMessage:     errMsg,
		BrowserSessionID: uuid.Nil,
	}
	if err := lifecycle.CompletePublication(&pub, publicationCompletion{
		Status:       status,
		RemoteID:     remoteID,
		PublishURL:   publishURL,
		ErrorMessage: errMsg,
	}); err != nil {
		return PublishResponse{}, failAttempt(err)
	}
	s.refreshProjectReadModel(ctx, projectID)
	attemptStatus := models.PublishAttemptStatusSucceeded
	if status == models.PublicationStatusFailed {
		attemptStatus = models.PublishAttemptStatusFailed
	}
	if hasAttempt {
		if err := lifecycle.FinishPublishAttemptForWorkspace(workspaceID, &attempt, publishAttemptCompletion{
			Status:       attemptStatus,
			RemoteID:     remoteID,
			PublishURL:   publishURL,
			ErrorMessage: errMsg,
		}); err != nil {
			return PublishResponse{}, err
		}
	}
	s.invalidateDashboardCaches(ctx)
	if err := s.recordProjectPublishActivityForWorkspace(workspaceID, projectID, *scopeUserID, models.ProjectActivityPublishCompleted, map[string]any{
		"platform":  platform,
		"status":    status,
		"remote_id": remoteID,
	}); err != nil {
		log.Printf("failed to record project publish activity for project %s platform %s: %v", projectID, platform, err)
	}

	return response, nil
}

func (s *Service) CreateXPostIntent(projectID uuid.UUID, scopeUserID *uuid.UUID) (PublishResponse, error) {
	ctx := context.Background()
	if scopeUserID == nil {
		return PublishResponse{}, ErrForbidden
	}
	if _, err := s.projectForPublish(ctx, projectID, *scopeUserID); err != nil {
		return PublishResponse{}, err
	}

	var pub models.ProjectPlatformPublication
	if err := s.strongReadDB(ctx).Where("project_id = ? AND platform = ?", projectID, "x").First(&pub).Error; err != nil {
		return PublishResponse{}, fmt.Errorf("publication record not found for platform: x")
	}
	if !pub.Enabled || pub.Status == models.PublicationStatusCancelled {
		return PublishResponse{}, ErrPublicationDisabled
	}
	if !publicationHasSyncedDraft(pub) {
		return PublishResponse{}, ErrPublicationRequiresSync
	}

	publishURL, err := publisher.BuildXPostIntentURL(pub.AdaptedContent)
	if err != nil {
		return PublishResponse{}, err
	}

	if err := s.writerDB(ctx).Model(&pub).Updates(map[string]any{
		"publish_url":   publishURL,
		"error_message": "",
	}).Error; err != nil {
		return PublishResponse{}, err
	}
	s.invalidateDashboardProjectListCache(ctx)
	s.refreshProjectReadModel(ctx, projectID)

	return PublishResponse{
		Status:     "manual_required",
		Platform:   "x",
		PublishURL: publishURL,
	}, nil
}

func normalizeIdempotencyKey(key string) string {
	return strings.TrimSpace(key)
}

func publicationHasSyncedDraft(pub models.ProjectPlatformPublication) bool {
	content := strings.TrimSpace(string(pub.AdaptedContent))
	return content != "" && content != "{}" && content != "null"
}

func (s *Service) recordPublishEvent(event models.PublishEvent) error {
	if event.ProjectID == uuid.Nil || event.UserID == uuid.Nil || event.Platform == "" || event.JobID == uuid.Nil {
		return nil
	}
	if event.WorkspaceID == uuid.Nil {
		var project models.Project
		if err := s.writerDB(s.requestContext()).Select("id", "user_id", "workspace_id").First(&project, "id = ?", event.ProjectID).Error; err == nil {
			event.WorkspaceID = models.ProjectWorkspaceID(project)
		}
	}
	if event.Metadata == nil {
		event.Metadata = datatypes.JSON(`{}`)
	}
	if event.Status == "" {
		event.Status = models.PublicationStatusDraft
	}
	return s.writerDB(s.requestContext()).Create(&event).Error
}

func (s *Service) recordProjectPublishActivity(projectID uuid.UUID, userID uuid.UUID, eventType string, metadata map[string]any) error {
	return s.recordProjectPublishActivityForWorkspace(uuid.Nil, projectID, userID, eventType, metadata)
}

func (s *Service) recordProjectPublishActivityForWorkspace(workspaceID uuid.UUID, projectID uuid.UUID, userID uuid.UUID, eventType string, metadata map[string]any) error {
	if projectID == uuid.Nil || userID == uuid.Nil || strings.TrimSpace(eventType) == "" {
		return nil
	}
	var project models.Project
	if err := s.writerDB(s.requestContext()).Select("id", "user_id", "workspace_id").First(&project, "id = ?", projectID).Error; err != nil {
		return err
	}
	projectWorkspaceID := models.ProjectWorkspaceID(project)
	if workspaceID == uuid.Nil {
		workspaceID = projectWorkspaceID
	} else if workspaceID != projectWorkspaceID {
		return ErrForbidden
	}
	payload := datatypes.JSON([]byte(`{}`))
	if metadata != nil {
		encoded, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		payload = datatypes.JSON(encoded)
	}
	return s.writerDB(s.requestContext()).Create(&models.ProjectActivity{
		WorkspaceID: workspaceID,
		ProjectID:   projectID,
		ActorUserID: userID,
		EventType:   eventType,
		Metadata:    payload,
	}).Error
}

func (s *Service) findIdempotentPublishResponse(projectID uuid.UUID, platform string, userID uuid.UUID, key string) (PublishResponse, bool, error) {
	return s.findIdempotentPublishResponseForWorkspace(uuid.Nil, projectID, platform, userID, key)
}

func (s *Service) findIdempotentPublishResponseForWorkspace(workspaceID uuid.UUID, projectID uuid.UUID, platform string, userID uuid.UUID, key string) (PublishResponse, bool, error) {
	if strings.TrimSpace(key) == "" {
		return PublishResponse{}, false, nil
	}
	db := s.writerDB(s.requestContext())
	if !db.Migrator().HasTable(&models.PublishEvent{}) {
		return PublishResponse{}, false, nil
	}

	var queued models.PublishEvent
	query := db.
		Scopes(workspaceScope(workspaceID)).
		Where("project_id = ? AND platform = ? AND user_id = ? AND idempotency_key = ? AND event_type = ?", projectID, platform, userID, key, "queued").
		Order("created_at DESC")
	err := query.
		First(&queued).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return PublishResponse{}, false, nil
	}
	if err != nil {
		return PublishResponse{}, false, err
	}

	event := queued
	var events []models.PublishEvent
	err = db.
		Scopes(workspaceScope(workspaceID)).
		Where("project_id = ? AND platform = ? AND user_id = ? AND job_id = ?", projectID, platform, userID, queued.JobID).
		Order("created_at ASC").
		Find(&events).Error
	if err != nil {
		return PublishResponse{}, false, err
	}
	for _, candidate := range events {
		if publishEventNewerForReplay(candidate, event) {
			event = candidate
		}
	}

	resp := PublishResponse{
		Status:         event.Status,
		JobID:          queued.JobID.String(),
		IdempotencyKey: key,
		Platform:       platform,
		QueuedAt:       &queued.CreatedAt,
		RemoteID:       event.RemoteID,
		PublishURL:     event.PublishURL,
		ErrorMessage:   event.ErrorMessage,
	}
	return resp, true, nil
}

func publishEventNewerForReplay(candidate, current models.PublishEvent) bool {
	if candidate.CreatedAt.After(current.CreatedAt) {
		return true
	}
	if current.CreatedAt.After(candidate.CreatedAt) {
		return false
	}
	return publishEventReplayRank(candidate.EventType) > publishEventReplayRank(current.EventType)
}

func publishEventReplayRank(eventType string) int {
	switch eventType {
	case "succeeded", "failed":
		return 4
	case "started":
		return 3
	case "queued":
		return 2
	case "requested":
		return 1
	default:
		return 0
	}
}

func (s *Service) waitForIdempotentPublishResponse(ctx context.Context, projectID uuid.UUID, platform string, userID uuid.UUID, key string) (PublishResponse, bool, error) {
	return s.waitForIdempotentPublishResponseForWorkspace(ctx, uuid.Nil, projectID, platform, userID, key)
}

func (s *Service) waitForIdempotentPublishResponseForWorkspace(ctx context.Context, workspaceID uuid.UUID, projectID uuid.UUID, platform string, userID uuid.UUID, key string) (PublishResponse, bool, error) {
	deadline := time.NewTimer(publishReplayWait)
	defer deadline.Stop()

	ticker := time.NewTicker(publishReplayPoll)
	defer ticker.Stop()

	for {
		resp, ok, err := s.findIdempotentPublishResponseForWorkspace(workspaceID, projectID, platform, userID, key)
		if err != nil || ok {
			return resp, ok, err
		}

		select {
		case <-ctx.Done():
			return PublishResponse{}, false, ctx.Err()
		case <-deadline.C:
			return PublishResponse{}, false, nil
		case <-ticker.C:
		}
	}
}

func (s *Service) applySavedBrowserCookies(ctx context.Context, userID uuid.UUID, platform string, account *models.PlatformAccount) error {
	if account == nil || !usesStoredBrowserCookies(platform) || account.UserID == uuid.Nil {
		return nil
	}

	cookies, err := publisher.NewCookieStore(s.strongReadDB(ctx)).LoadForAccount(ctx, userID, account.ID, platform)
	if err != nil {
		return fmt.Errorf("%w: %s cookies are unavailable: %w", platformaccount.ErrInvalidPlatformAccount, platform, err)
	}

	cookiesJSON, err := json.Marshal(cookies)
	if err != nil {
		return fmt.Errorf("failed to prepare %s cookies: %w", platform, err)
	}
	account.Cookies = datatypes.JSON(cookiesJSON)
	return nil
}

func usesStoredBrowserCookies(platform string) bool {
	return platformcapabilities.UsesStoredBrowserCookies(platform)
}
