package publish

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/glebarez/sqlite"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/kurodakayn/mpp-backend/internal/models"
	"github.com/kurodakayn/mpp-backend/internal/pkg/rediskey"
	"github.com/kurodakayn/mpp-backend/internal/publisher"
	platformaccount "github.com/kurodakayn/mpp-backend/internal/services/platform_account"
)

type queueTestPublisher struct{}

func TestPublishLockKeyUsesProjectHashTag(t *testing.T) {
	projectID := uuid.New()

	key := publishLockKey(projectID, "Wechat")

	tag, ok := rediskey.ExtractTag(key)
	require.True(t, ok)
	require.Equal(t, "project:"+projectID.String(), tag)
	require.Equal(t, publishLockKeyPrefix+rediskey.Tag("project", projectID.String())+":wechat", key)
}

func (p queueTestPublisher) ValidateConfig(_ []byte) error {
	return nil
}

func (p queueTestPublisher) Publish(_ context.Context, _ *models.ProjectPlatformPublication, _ *models.PlatformAccount) (string, string, error) {
	return "remote-id", "https://example.com/published", nil
}

type failingQueueTestPublisher struct{}

func (p failingQueueTestPublisher) ValidateConfig(_ []byte) error {
	return nil
}

func (p failingQueueTestPublisher) Publish(_ context.Context, _ *models.ProjectPlatformPublication, _ *models.PlatformAccount) (string, string, error) {
	return "", "", errors.New("platform unavailable")
}

type contextAwareQueueTestPublisher struct {
	valueSeen atomic.Bool
}

func (p *contextAwareQueueTestPublisher) ValidateConfig(_ []byte) error {
	return nil
}

func (p *contextAwareQueueTestPublisher) Publish(ctx context.Context, _ *models.ProjectPlatformPublication, _ *models.PlatformAccount) (string, string, error) {
	if ctx.Value(publishContextTestKey{}) == "job-context" {
		p.valueSeen.Store(true)
	}
	return "remote-id", "https://example.com/published", nil
}

type publishContextTestKey struct{}

type blockingQueueTestPublisher struct {
	entered chan struct{}
	release chan struct{}
}

func (p *blockingQueueTestPublisher) ValidateConfig(_ []byte) error {
	return nil
}

func (p *blockingQueueTestPublisher) Publish(ctx context.Context, _ *models.ProjectPlatformPublication, _ *models.PlatformAccount) (string, string, error) {
	close(p.entered)
	select {
	case <-ctx.Done():
		return "", "", ctx.Err()
	case <-p.release:
		return "remote-id", "https://example.com/published", nil
	}
}

type testPublishQueue struct {
	jobs            []PublishJob
	locks           map[string]string
	refreshes       int
	onAcquire       func(key, _ string)
	enqueueErr      error
	onAcquireLocked func(key, _ string)
}

func newTestPublishQueue() *testPublishQueue {
	return &testPublishQueue{locks: map[string]string{}}
}

func (q *testPublishQueue) Enqueue(_ context.Context, job PublishJob) error {
	if q.enqueueErr != nil {
		return q.enqueueErr
	}
	q.jobs = append(q.jobs, job)
	return nil
}

func (q *testPublishQueue) Start(ctx context.Context, handler PublishJobHandler) {
	for len(q.jobs) > 0 {
		job := q.jobs[0]
		q.jobs = q.jobs[1:]
		_ = handler(ctx, job)
	}
}

func (q *testPublishQueue) AcquireLock(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	if _, exists := q.locks[key]; exists {
		if q.onAcquireLocked != nil {
			q.onAcquireLocked(key, value)
		}
		return false, nil
	}
	q.locks[key] = value
	if q.onAcquire != nil {
		q.onAcquire(key, value)
	}
	return true, nil
}

func (q *testPublishQueue) LockValue(_ context.Context, key string) (string, error) {
	return q.locks[key], nil
}

func (q *testPublishQueue) RefreshLock(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	if q.locks[key] != value {
		return false, nil
	}
	q.refreshes++
	return true, nil
}

func (q *testPublishQueue) ReleaseLock(_ context.Context, key, value string) error {
	if q.locks[key] == value {
		delete(q.locks, key)
	}
	return nil
}

type errorReportingPublishQueue struct {
	*testPublishQueue
	err error
}

func (q *errorReportingPublishQueue) Run(context.Context, PublishJobHandler) error {
	return q.err
}

type testPublishJobObserver struct {
	observations []publishJobObservation
}

type publishJobObservation struct {
	platform string
	result   string
}

func (o *testPublishJobObserver) ObservePublishJob(platform string, result string) {
	o.observations = append(o.observations, publishJobObservation{
		platform: platform,
		result:   result,
	})
}

type testDashboardCacheInvalidator struct {
	projectListInvalidations int
	statsInvalidations       int
}

func (i *testDashboardCacheInvalidator) InvalidateDashboardProjectListCache(context.Context) {
	i.projectListInvalidations++
}

func (i *testDashboardCacheInvalidator) InvalidateDashboardStatsCache(context.Context) {
	i.statsInvalidations++
}

func setupPublishQueueTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)

	require.NoError(t, db.Exec(`CREATE TABLE users (
		id TEXT PRIMARY KEY,
		username TEXT NOT NULL,
		email TEXT NOT NULL,
		is_email_verified BOOLEAN NOT NULL DEFAULT 0,
		password_hash TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT 'user',
		created_at DATETIME,
		updated_at DATETIME
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE projects (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		workspace_id TEXT,
		collab_document_id TEXT UNIQUE,
		template_id TEXT,
		brand_profile_id TEXT,
		title TEXT NOT NULL,
		source_content TEXT NOT NULL,
		status TEXT NOT NULL,
		created_at DATETIME,
		updated_at DATETIME
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE project_collaborators (
		project_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		role TEXT NOT NULL,
		created_by TEXT NOT NULL,
		created_at DATETIME,
		PRIMARY KEY (project_id, user_id)
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE project_activities (
		id TEXT PRIMARY KEY,
		workspace_id TEXT NOT NULL,
		project_id TEXT NOT NULL,
		actor_user_id TEXT NOT NULL,
		target_user_id TEXT,
		event_type TEXT NOT NULL,
		metadata TEXT NOT NULL DEFAULT '{}',
		created_at DATETIME NOT NULL
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE platform_accounts (
		id TEXT PRIMARY KEY,
		user_id TEXT NOT NULL,
		workspace_id TEXT,
		owner_user_id TEXT,
		connected_by_user_id TEXT,
		platform TEXT NOT NULL,
		username TEXT NOT NULL,
		display_name TEXT NOT NULL DEFAULT '',
		platform_user_id TEXT NOT NULL DEFAULT '',
		share_scope TEXT NOT NULL DEFAULT 'private',
		status TEXT NOT NULL DEFAULT 'untested',
		health_status TEXT NOT NULL DEFAULT 'unknown',
		credential_secret_ref TEXT NOT NULL DEFAULT '',
		credentials TEXT NOT NULL DEFAULT '{}',
		metadata TEXT NOT NULL DEFAULT '{}',
		cookies TEXT NOT NULL DEFAULT '[]',
		config TEXT NOT NULL DEFAULT '{}',
		avatar_url TEXT,
		last_connected_at DATETIME,
		last_verified_at DATETIME,
		last_tested_at DATETIME,
		last_test_error TEXT,
		expires_at DATETIME,
		created_at DATETIME,
		updated_at DATETIME
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE platform_account_grants (
		id TEXT PRIMARY KEY,
		platform_account_id TEXT NOT NULL,
		workspace_id TEXT NOT NULL,
		grantee_user_id TEXT,
		project_id TEXT,
		role TEXT NOT NULL,
		created_by TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE project_platform_publications (
		id TEXT PRIMARY KEY,
		workspace_id TEXT NOT NULL,
		project_id TEXT NOT NULL,
		platform TEXT NOT NULL,
		platform_account_id TEXT,
		enabled BOOLEAN NOT NULL DEFAULT 1,
		status TEXT NOT NULL,
		draft_status TEXT NOT NULL DEFAULT 'unsynced',
		review_status TEXT NOT NULL DEFAULT 'draft',
		sync_required BOOLEAN NOT NULL DEFAULT 0,
		config TEXT NOT NULL DEFAULT '{}',
		adapted_content TEXT NOT NULL DEFAULT '{}',
		remote_id TEXT,
		publish_url TEXT,
		error_message TEXT,
		retry_count INTEGER NOT NULL DEFAULT 0,
		last_attempt_at DATETIME,
		published_at DATETIME,
		created_at DATETIME,
		updated_at DATETIME
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE media_asset_usages (
		id TEXT PRIMARY KEY,
		media_asset_id TEXT NOT NULL,
		workspace_id TEXT NOT NULL,
		project_id TEXT,
		publication_id TEXT,
		template_id TEXT,
		resource_type TEXT NOT NULL,
		resource_id TEXT NOT NULL,
		usage_kind TEXT NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL,
		UNIQUE(media_asset_id, resource_type, resource_id)
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE publish_events (
		id TEXT PRIMARY KEY,
		publication_id TEXT NOT NULL,
		workspace_id TEXT NOT NULL,
		project_id TEXT NOT NULL,
		user_id TEXT NOT NULL,
		platform TEXT NOT NULL,
		job_id TEXT NOT NULL,
		idempotency_key TEXT NOT NULL,
		event_type TEXT NOT NULL,
		status TEXT NOT NULL,
		message TEXT,
		remote_id TEXT,
		publish_url TEXT,
		error_message TEXT,
		metadata TEXT NOT NULL DEFAULT '{}',
		created_at DATETIME
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE outbox_events (
		id TEXT PRIMARY KEY,
		aggregate_type TEXT NOT NULL,
		aggregate_id TEXT NOT NULL,
		event_type TEXT NOT NULL,
		payload TEXT NOT NULL DEFAULT '{}',
		status TEXT NOT NULL DEFAULT 'pending',
		attempts INTEGER NOT NULL DEFAULT 0,
		next_attempt_at DATETIME,
		processed_at DATETIME,
		error_message TEXT NOT NULL DEFAULT '',
		created_at DATETIME,
		updated_at DATETIME
	)`).Error)

	return db
}

func newPublishTestService(db *gorm.DB) *Service {
	return NewService(db, platformaccount.NewService(db))
}

func createConnectedQueueAccount(t *testing.T, db *gorm.DB, userID uuid.UUID, platform string) models.PlatformAccount {
	t.Helper()

	workspaceID := models.PersonalWorkspaceID(userID)
	account := models.PlatformAccount{
		UserID:       userID,
		WorkspaceID:  &workspaceID,
		Platform:     platform,
		Username:     platform,
		DisplayName:  platform,
		ShareScope:   models.PlatformAccountSharePrivate,
		Status:       models.PlatformAccountStatusConnected,
		HealthStatus: models.PlatformAccountHealthHealthy,
		Credentials:  datatypes.JSON(`{"app_id":"wx","app_secret":"secret"}`),
		Metadata:     datatypes.JSON(`{}`),
		Cookies:      datatypes.JSON(`[]`),
		Config:       datatypes.JSON(`{}`),
	}
	ownerID := userID
	account.OwnerUserID = &ownerID
	account.ConnectedByUserID = &ownerID
	require.NoError(t, db.Create(&account).Error)
	return account
}

func TestEnqueuePublishProjectQueuesAndLocksPublication(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusDraft,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{IdempotencyKey: "click-1"})
	require.NoError(t, err)
	require.Equal(t, models.PublicationStatusQueued, resp.Status)
	require.Len(t, queue.jobs, 1)
	require.Equal(t, models.PersonalWorkspaceID(user.ID), queue.jobs[0].WorkspaceID)
	require.Equal(t, uuid.Nil, queue.jobs[0].BrowserSessionID)

	lockKey := publishLockKey(project.ID, "wechat")
	require.Equal(t, queue.jobs[0].JobID.String(), queue.locks[lockKey])

	var outbox models.OutboxEvent
	require.NoError(t, db.First(&outbox, "aggregate_id = ?", queue.jobs[0].JobID).Error)
	require.Equal(t, models.OutboxAggregatePublishJob, outbox.AggregateType)
	require.Equal(t, models.OutboxEventPublishJobRequested, outbox.EventType)
	require.Equal(t, models.OutboxStatusDispatched, outbox.Status)
	require.Equal(t, 1, outbox.Attempts)
	require.NotNil(t, outbox.ProcessedAt)
	var outboxJob PublishJob
	require.NoError(t, json.Unmarshal(outbox.Payload, &outboxJob))
	require.Equal(t, models.PersonalWorkspaceID(user.ID), outboxJob.WorkspaceID)

	var saved models.ProjectPlatformPublication
	require.NoError(t, db.First(&saved, "project_id = ? AND platform = ?", project.ID, "wechat").Error)
	require.Equal(t, models.PublicationStatusQueued, saved.Status)
	require.NotNil(t, saved.LastAttemptAt)

	duplicate, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{IdempotencyKey: "click-1"})
	require.NoError(t, err)
	require.Equal(t, resp.JobID, duplicate.JobID)
}

func TestEnqueuePublishProjectInvalidatesDashboardCaches(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue
	invalidator := &testDashboardCacheInvalidator{}
	service.SetDashboardCacheInvalidator(invalidator)

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "cache-owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued cache post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusDraft,
		Config:         datatypes.JSON(`{"title":"Queued cache post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{IdempotencyKey: "cache-click"})

	require.NoError(t, err)
	require.Equal(t, models.PublicationStatusQueued, resp.Status)
	require.Equal(t, 1, invalidator.projectListInvalidations)
	require.Equal(t, 1, invalidator.statsInvalidations)
}

func TestEnqueuePublishProjectRejectsProjectEditor(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	owner := models.User{Username: "owner", Email: "owner@example.com"}
	editor := models.User{Username: "editor", Email: "editor@example.com"}
	require.NoError(t, db.Create(&owner).Error)
	require.NoError(t, db.Create(&editor).Error)
	project := models.Project{
		UserID:        owner.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	account := createConnectedQueueAccount(t, db, owner.ID, "wechat")
	require.NoError(t, db.Create(&models.PlatformAccountGrant{
		PlatformAccountID: account.ID,
		WorkspaceID:       *account.WorkspaceID,
		GranteeUserID:     &editor.ID,
		Role:              models.PlatformAccountGrantRolePublisher,
		CreatedBy:         owner.ID,
	}).Error)
	require.NoError(t, db.Create(&models.ProjectCollaborator{
		ProjectID: project.ID,
		UserID:    editor.ID,
		Role:      models.ProjectRoleEditor,
		CreatedBy: owner.ID,
	}).Error)
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusDraft,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &editor.ID, PublishRequest{IdempotencyKey: "click-editor"})

	require.ErrorIs(t, err, ErrForbidden)
	require.Empty(t, resp.Status)
	require.Empty(t, queue.jobs)
}

func TestEnqueuePublishProjectRejectsProjectViewer(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	owner := models.User{Username: "owner", Email: "owner@example.com"}
	viewer := models.User{Username: "viewer", Email: "viewer@example.com"}
	require.NoError(t, db.Create(&owner).Error)
	require.NoError(t, db.Create(&viewer).Error)
	project := models.Project{
		UserID:        owner.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, owner.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectCollaborator{
		ProjectID: project.ID,
		UserID:    viewer.ID,
		Role:      models.ProjectRoleViewer,
		CreatedBy: owner.ID,
	}).Error)
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusDraft,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &viewer.ID, PublishRequest{IdempotencyKey: "click-viewer"})

	require.ErrorIs(t, err, ErrForbidden)
	require.Empty(t, resp.Status)
	require.Empty(t, queue.jobs)
}

func TestEnqueuePublishProjectReplaysDuplicateWhenLockWinsBeforeQueuedEvent(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	publication := models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusDraft,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}
	require.NoError(t, db.Create(&publication).Error)

	originalJobID := uuid.New()
	lockKey := publishLockKey(project.ID, "wechat")
	queue.locks[lockKey] = originalJobID.String()
	queue.onAcquireLocked = func(key, _ string) {
		if key != lockKey {
			return
		}
		require.NoError(t, db.Create(&models.PublishEvent{
			PublicationID:  publication.ID,
			ProjectID:      project.ID,
			UserID:         user.ID,
			Platform:       "wechat",
			JobID:          originalJobID,
			IdempotencyKey: "click-race",
			EventType:      "queued",
			Status:         models.PublicationStatusQueued,
		}).Error)
	}

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{IdempotencyKey: "click-race"})

	require.NoError(t, err)
	require.Equal(t, models.PublicationStatusQueued, resp.Status)
	require.Equal(t, originalJobID.String(), resp.JobID)
	require.Empty(t, queue.jobs)
}

func TestEnqueuePublishProjectDoesNotPersistScheduleWhenLockIsHeld(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	require.NoError(t, db.AutoMigrate(&models.ScheduledPublication{}, &models.PublishAttempt{}, &models.ProjectVersion{}))
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	publication := models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusDraft,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}
	require.NoError(t, db.Create(&publication).Error)

	lockKey := publishLockKey(project.ID, "wechat")
	queue.locks[lockKey] = uuid.New().String()

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{})

	require.ErrorIs(t, err, ErrPublicationAlreadyPublishing)
	require.Empty(t, resp.Status)

	var schedules int64
	require.NoError(t, db.Model(&models.ScheduledPublication{}).
		Where("project_id = ? AND publication_id = ?", project.ID, publication.ID).
		Count(&schedules).Error)
	require.Zero(t, schedules)
}

func TestFlushScheduledPublicationsPublishesDueSchedules(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	require.NoError(t, db.AutoMigrate(&models.ScheduledPublication{}, &models.PublishAttempt{}, &models.ProjectVersion{}))
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Scheduled post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	account := createConnectedQueueAccount(t, db, user.ID, "wechat")
	publication := models.ProjectPlatformPublication{
		ProjectID:         project.ID,
		Platform:          "wechat",
		PlatformAccountID: &account.ID,
		Enabled:           true,
		Status:            models.PublicationStatusDraft,
		Config:            datatypes.JSON(`{"title":"Scheduled post"}`),
		AdaptedContent:    datatypes.JSON(`{"format":"html","html":"ready"}`),
	}
	require.NoError(t, db.Create(&publication).Error)
	schedule := models.ScheduledPublication{
		WorkspaceID:       models.PersonalWorkspaceID(user.ID),
		ProjectID:         project.ID,
		PublicationID:     publication.ID,
		PlatformAccountID: publication.PlatformAccountID,
		ScheduledAt:       time.Now().UTC().Add(-time.Minute),
		Status:            models.ScheduledPublicationStatusScheduled,
		IdempotencyKey:    "due-schedule",
		CreatedBy:         user.ID,
	}
	require.NoError(t, db.Create(&schedule).Error)

	require.NoError(t, service.FlushScheduledPublications(context.Background(), 10))

	var savedSchedule models.ScheduledPublication
	require.NoError(t, db.First(&savedSchedule, "id = ?", schedule.ID).Error)
	require.Equal(t, models.ScheduledPublicationStatusPublished, savedSchedule.Status)

	var attempts []models.PublishAttempt
	require.NoError(t, db.Order("attempt_no asc").Find(&attempts, "scheduled_publication_id = ?", schedule.ID).Error)
	require.Len(t, attempts, 1)
	require.Equal(t, models.PublishAttemptStatusSucceeded, attempts[0].Status)

	lockKey := publishLockKey(project.ID, "wechat")
	require.Empty(t, queue.locks[lockKey])
}
func TestEnqueuePublishProjectReplaysOriginalJobEventsAfterPublicationChanges(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	service.queue = newTestPublishQueue()

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	publication := models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusSucceeded,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
		RemoteID:       "newer-remote",
		PublishURL:     "https://example.com/newer",
	}
	require.NoError(t, db.Create(&publication).Error)

	jobID := uuid.New()
	queuedAt := time.Now().UTC().Add(-time.Minute)
	succeededAt := queuedAt.Add(time.Second)
	require.NoError(t, db.Create(&models.PublishEvent{
		PublicationID:  publication.ID,
		ProjectID:      project.ID,
		UserID:         user.ID,
		Platform:       "wechat",
		JobID:          jobID,
		IdempotencyKey: "click-original",
		EventType:      "queued",
		Status:         models.PublicationStatusQueued,
		CreatedAt:      queuedAt,
	}).Error)
	require.NoError(t, db.Create(&models.PublishEvent{
		PublicationID:  publication.ID,
		ProjectID:      project.ID,
		UserID:         user.ID,
		Platform:       "wechat",
		JobID:          jobID,
		IdempotencyKey: "click-original",
		EventType:      "succeeded",
		Status:         models.PublicationStatusSucceeded,
		RemoteID:       "event-remote",
		PublishURL:     "https://example.com/original",
		CreatedAt:      succeededAt,
	}).Error)
	require.NoError(t, db.Model(&publication).Updates(map[string]any{
		"enabled":       false,
		"error_message": "publication was later cancelled",
		"publish_url":   "https://example.com/changed",
		"remote_id":     "changed-remote",
		"status":        models.PublicationStatusCancelled,
	}).Error)

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{IdempotencyKey: "click-original"})

	require.NoError(t, err)
	require.Equal(t, models.PublicationStatusSucceeded, resp.Status)
	require.Equal(t, jobID.String(), resp.JobID)
	require.Equal(t, "event-remote", resp.RemoteID)
	require.Equal(t, "https://example.com/original", resp.PublishURL)
	require.Empty(t, resp.ErrorMessage)
}

func TestEnqueuePublishProjectLeavesFailedDispatchInOutboxForRetry(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	queue.enqueueErr = errors.New("redis unavailable")
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusDraft,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{IdempotencyKey: "click-3"})
	require.NoError(t, err)
	require.Equal(t, models.PublicationStatusQueued, resp.Status)
	require.Empty(t, queue.jobs)

	var queuedEvents int64
	require.NoError(t, db.Model(&models.PublishEvent{}).
		Where("idempotency_key = ? AND event_type = ?", "click-3", "queued").
		Count(&queuedEvents).Error)
	require.EqualValues(t, 1, queuedEvents)

	var outbox models.OutboxEvent
	require.NoError(t, db.First(&outbox, "aggregate_id = ?", uuid.MustParse(resp.JobID)).Error)
	require.Equal(t, models.OutboxStatusFailed, outbox.Status)
	require.Equal(t, 1, outbox.Attempts)
	require.NotNil(t, outbox.NextAttemptAt)
	require.Contains(t, outbox.ErrorMessage, "redis unavailable")

	queue.enqueueErr = nil
	require.NoError(t, db.Model(&models.OutboxEvent{}).
		Where("id = ?", outbox.ID).
		Update("next_attempt_at", time.Now().UTC().Add(-time.Second)).Error)
	require.NoError(t, service.FlushPublishOutbox(context.Background(), 10))
	require.Len(t, queue.jobs, 1)
	require.Equal(t, resp.JobID, queue.jobs[0].JobID.String())
	require.Equal(t, models.PersonalWorkspaceID(user.ID), queue.jobs[0].WorkspaceID)

	require.NoError(t, db.First(&outbox, "id = ?", outbox.ID).Error)
	require.Equal(t, models.OutboxStatusDispatched, outbox.Status)
	require.Equal(t, 2, outbox.Attempts)
	require.NotNil(t, outbox.ProcessedAt)
}

func TestFlushPublishOutboxRetriesStaleProcessingEvent(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	user := models.User{Username: "legacy-outbox-owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Legacy queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)

	job := PublishJob{
		JobID:      uuid.New(),
		ProjectID:  project.ID,
		UserID:     user.ID,
		Platform:   "wechat",
		EnqueuedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(struct {
		JobID      uuid.UUID `json:"job_id"`
		ProjectID  uuid.UUID `json:"project_id"`
		UserID     uuid.UUID `json:"user_id"`
		Platform   string    `json:"platform"`
		EnqueuedAt time.Time `json:"enqueued_at"`
	}{
		JobID:      job.JobID,
		ProjectID:  job.ProjectID,
		UserID:     job.UserID,
		Platform:   job.Platform,
		EnqueuedAt: job.EnqueuedAt,
	})
	require.NoError(t, err)
	staleUpdatedAt := time.Now().UTC().Add(-publishOutboxClaimTimeout - time.Second)
	outbox := models.OutboxEvent{
		AggregateType: models.OutboxAggregatePublishJob,
		AggregateID:   job.JobID,
		EventType:     models.OutboxEventPublishJobRequested,
		Payload:       datatypes.JSON(payload),
		Status:        models.OutboxStatusProcessing,
		Attempts:      3,
		CreatedAt:     staleUpdatedAt,
		UpdatedAt:     staleUpdatedAt,
	}
	require.NoError(t, db.Create(&outbox).Error)

	require.NoError(t, service.FlushPublishOutbox(context.Background(), 10))

	require.Len(t, queue.jobs, 1)
	job.WorkspaceID = models.PersonalWorkspaceID(user.ID)
	require.Equal(t, job, queue.jobs[0])
	require.NoError(t, db.First(&outbox, "id = ?", outbox.ID).Error)
	require.Equal(t, models.OutboxStatusDispatched, outbox.Status)
	require.Equal(t, 4, outbox.Attempts)
	require.NotNil(t, outbox.ProcessedAt)
}

func TestClaimOutboxEventDoesNotDoubleClaimPendingRace(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:outbox-claim-race?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.OutboxEvent{}))
	service := newPublishTestService(db)
	payload, err := json.Marshal(PublishJob{
		JobID:      uuid.New(),
		ProjectID:  uuid.New(),
		UserID:     uuid.New(),
		Platform:   "wechat",
		EnqueuedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	outbox := models.OutboxEvent{
		AggregateType: models.OutboxAggregatePublishJob,
		AggregateID:   uuid.New(),
		EventType:     models.OutboxEventPublishJobRequested,
		Payload:       datatypes.JSON(payload),
		Status:        models.OutboxStatusPending,
	}
	require.NoError(t, db.Create(&outbox).Error)

	const callbackName = "mpp:test:block_outbox_claim_update"
	var blocked atomic.Int32
	bothBlocked := make(chan struct{})
	release := make(chan struct{})
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table != "outbox_events" {
			return
		}
		if blocked.Add(1) == 2 {
			close(bothBlocked)
		}
		<-release
	}))
	defer func() {
		_ = db.Callback().Update().Remove(callbackName)
	}()

	type claimResult struct {
		claimed bool
		err     error
	}
	results := make(chan claimResult, 2)
	for range 2 {
		go func() {
			_, claimed, err := service.claimOutboxEvent(context.Background(), outbox.ID)
			results <- claimResult{claimed: claimed, err: err}
		}()
	}

	select {
	case <-bothBlocked:
	case <-time.After(time.Second):
		t.Fatal("expected both claim attempts to reach the update gate")
	}
	close(release)

	first := <-results
	second := <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	claimedCount := 0
	if first.claimed {
		claimedCount++
	}
	if second.claimed {
		claimedCount++
	}
	require.Equal(t, 1, claimedCount)

	require.NoError(t, db.First(&outbox, "id = ?", outbox.ID).Error)
	require.Equal(t, models.OutboxStatusProcessing, outbox.Status)
	require.Equal(t, 1, outbox.Attempts)
}

func TestOutboxClaimVersionPreventsLateFailureOverwrite(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:outbox-claim-version?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.OutboxEvent{}))
	service := newPublishTestService(db)
	outbox := models.OutboxEvent{
		AggregateType: models.OutboxAggregatePublishJob,
		AggregateID:   uuid.New(),
		EventType:     models.OutboxEventPublishJobRequested,
		Payload:       datatypes.JSON(`{}`),
		Status:        models.OutboxStatusDispatched,
		Attempts:      2,
	}
	require.NoError(t, db.Create(&outbox).Error)

	lateClaim := outbox
	lateClaim.Status = models.OutboxStatusProcessing
	lateClaim.Attempts = 1
	require.NoError(t, service.markOutboxEventFailed(context.Background(), lateClaim, errors.New("late dispatch failure")))

	require.NoError(t, db.First(&outbox, "id = ?", outbox.ID).Error)
	require.Equal(t, models.OutboxStatusDispatched, outbox.Status)
	require.Equal(t, 2, outbox.Attempts)
	require.Empty(t, outbox.ErrorMessage)
}

func TestEnqueuePublishProjectRejectsActivePublishingWithoutRedisLock(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	service.queue = newTestPublishQueue()

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	lastAttemptAt := time.Now().UTC()
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusPublishing,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
		LastAttemptAt:  &lastAttemptAt,
	}).Error)

	_, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{IdempotencyKey: "click-2"})

	require.ErrorIs(t, err, ErrPublicationAlreadyPublishing)
}

func TestEnqueuePublishProjectRequiresPrepublishSyncForSyncingPublication(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	publication := models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusSyncing,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}
	require.NoError(t, db.Create(&publication).Error)

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{IdempotencyKey: "click-syncing"})

	require.ErrorIs(t, err, ErrPublicationRequiresSync)
	require.Empty(t, resp.Status)
	require.Empty(t, queue.jobs)

	var saved models.ProjectPlatformPublication
	require.NoError(t, db.First(&saved, "id = ?", publication.ID).Error)
	require.Equal(t, models.PublicationStatusSyncing, saved.Status)
	require.Nil(t, saved.LastAttemptAt)
}

func TestEnqueuePublishProjectRejectsPublicationChangedToSyncingAfterLock(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	publication := models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusDraft,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}
	require.NoError(t, db.Create(&publication).Error)
	queue.onAcquire = func(_, _ string) {
		require.NoError(t, db.Model(&models.ProjectPlatformPublication{}).
			Where("id = ?", publication.ID).
			Update("status", models.PublicationStatusSyncing).Error)
	}

	resp, err := service.EnqueuePublishProject(context.Background(), project.ID, "wechat", &user.ID, PublishRequest{IdempotencyKey: "click-race"})

	require.ErrorIs(t, err, ErrPublicationRequiresSync)
	require.Empty(t, resp.Status)
	require.Empty(t, queue.jobs)
	require.Empty(t, queue.locks)

	var saved models.ProjectPlatformPublication
	require.NoError(t, db.First(&saved, "id = ?", publication.ID).Error)
	require.Equal(t, models.PublicationStatusSyncing, saved.Status)
	require.Nil(t, saved.LastAttemptAt)
}

func TestProcessPublishJobPublishesAndReleasesLock(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue
	observer := &testPublishJobObserver{}
	service.SetPublishJobObserver(observer)

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusPublishing,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	job := PublishJob{
		JobID:      uuid.New(),
		ProjectID:  project.ID,
		UserID:     user.ID,
		Platform:   "wechat",
		EnqueuedAt: time.Now().UTC(),
	}
	lockKey := publishLockKey(project.ID, "wechat")
	queue.locks[lockKey] = job.JobID.String()

	require.NoError(t, service.processPublishJob(context.Background(), job))

	var saved models.ProjectPlatformPublication
	require.NoError(t, db.First(&saved, "project_id = ? AND platform = ?", project.ID, "wechat").Error)
	require.Equal(t, models.PublicationStatusSucceeded, saved.Status)
	require.Equal(t, "remote-id", saved.RemoteID)
	require.Equal(t, "https://example.com/published", saved.PublishURL)
	require.Empty(t, queue.locks[lockKey])
	require.Equal(t, []publishJobObservation{
		{platform: "wechat", result: publishJobResultSuccess},
	}, observer.observations)
}

func TestProcessPublishJobRejectsWorkspaceMismatch(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusPublishing,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	wrongWorkspaceID := uuid.New()
	job := PublishJob{
		JobID:       uuid.New(),
		ProjectID:   project.ID,
		WorkspaceID: wrongWorkspaceID,
		UserID:      user.ID,
		Platform:    "wechat",
		EnqueuedAt:  time.Now().UTC(),
	}
	lockKey := publishLockKey(project.ID, "wechat")
	queue.locks[lockKey] = job.JobID.String()

	require.NoError(t, service.processPublishJob(context.Background(), job))

	var saved models.ProjectPlatformPublication
	require.NoError(t, db.First(&saved, "project_id = ? AND platform = ?", project.ID, "wechat").Error)
	require.Equal(t, models.PublicationStatusPublishing, saved.Status)
	require.Empty(t, queue.locks[lockKey])

	var wrongWorkspaceEvents int64
	require.NoError(t, db.Model(&models.PublishEvent{}).Where("workspace_id = ?", wrongWorkspaceID).Count(&wrongWorkspaceEvents).Error)
	require.Zero(t, wrongWorkspaceEvents)
}

func TestProcessPublishJobReacquiresExpiredLock(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusPublishing,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	job := PublishJob{
		JobID:      uuid.New(),
		ProjectID:  project.ID,
		UserID:     user.ID,
		Platform:   "wechat",
		EnqueuedAt: time.Now().UTC(),
	}

	require.NoError(t, service.processPublishJob(context.Background(), job))

	var saved models.ProjectPlatformPublication
	require.NoError(t, db.First(&saved, "project_id = ? AND platform = ?", project.ID, "wechat").Error)
	require.Equal(t, models.PublicationStatusSucceeded, saved.Status)
	require.Empty(t, queue.locks[publishLockKey(project.ID, "wechat")])
}

func TestProcessPublishJobReturnsErrorForFailedPublication(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue
	observer := &testPublishJobObserver{}
	service.SetPublishJobObserver(observer)

	publisher.Factory.Register("wechat", failingQueueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusPublishing,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	job := PublishJob{
		JobID:      uuid.New(),
		ProjectID:  project.ID,
		UserID:     user.ID,
		Platform:   "wechat",
		EnqueuedAt: time.Now().UTC(),
	}
	lockKey := publishLockKey(project.ID, "wechat")
	queue.locks[lockKey] = job.JobID.String()

	err := service.processPublishJob(context.Background(), job)

	require.Error(t, err)
	require.Empty(t, queue.locks[lockKey])

	var saved models.ProjectPlatformPublication
	require.NoError(t, db.First(&saved, "project_id = ? AND platform = ?", project.ID, "wechat").Error)
	require.Equal(t, models.PublicationStatusFailed, saved.Status)
	require.Equal(t, 1, saved.RetryCount)
	require.Contains(t, saved.ErrorMessage, "platform unavailable")
	require.Equal(t, []publishJobObservation{
		{platform: "wechat", result: publishJobResultError},
	}, observer.observations)
}

func TestProcessPublishJobMarksFailedWhenWorkerContextCanceled(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue
	invalidator := &testDashboardCacheInvalidator{}
	service.SetDashboardCacheInvalidator(invalidator)

	publisher.Factory.Register("wechat", queueTestPublisher{})
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Canceled queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusPublishing,
		Config:         datatypes.JSON(`{"title":"Canceled queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	job := PublishJob{
		JobID:      uuid.New(),
		ProjectID:  project.ID,
		UserID:     user.ID,
		Platform:   "wechat",
		EnqueuedAt: time.Now().UTC(),
	}
	lockKey := publishLockKey(project.ID, "wechat")
	queue.locks[lockKey] = job.JobID.String()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := service.processPublishJob(ctx, job)

	require.Error(t, err)
	require.Empty(t, queue.locks[lockKey])
	require.Equal(t, 1, invalidator.projectListInvalidations)
	require.Equal(t, 1, invalidator.statsInvalidations)

	var saved models.ProjectPlatformPublication
	require.NoError(t, db.First(&saved, "project_id = ? AND platform = ?", project.ID, "wechat").Error)
	require.Equal(t, models.PublicationStatusFailed, saved.Status)
	require.Equal(t, 1, saved.RetryCount)
	require.NotEmpty(t, saved.ErrorMessage)
}

func TestProcessPublishJobPassesWorkerContextToPublisher(t *testing.T) {
	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue

	publisherSpy := &contextAwareQueueTestPublisher{}
	publisher.Factory.Register("wechat", publisherSpy)
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Canceled queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusPublishing,
		Config:         datatypes.JSON(`{"title":"Canceled queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	job := PublishJob{
		JobID:      uuid.New(),
		ProjectID:  project.ID,
		UserID:     user.ID,
		Platform:   "wechat",
		EnqueuedAt: time.Now().UTC(),
	}
	lockKey := publishLockKey(project.ID, "wechat")
	queue.locks[lockKey] = job.JobID.String()
	ctx := context.WithValue(context.Background(), publishContextTestKey{}, "job-context")

	err := service.processPublishJob(ctx, job)

	require.NoError(t, err)
	require.True(t, publisherSpy.valueSeen.Load())
}

func TestProcessPublishJobCancelsWhenRefreshLosesOwnership(t *testing.T) {
	originalRefreshInterval := publishLockRefreshInterval
	publishLockRefreshInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		publishLockRefreshInterval = originalRefreshInterval
	})

	db := setupPublishQueueTestDB(t)
	service := newPublishTestService(db)
	queue := newTestPublishQueue()
	service.queue = queue
	observer := &testPublishJobObserver{}
	service.SetPublishJobObserver(observer)

	publisherSpy := &blockingQueueTestPublisher{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	publisher.Factory.Register("wechat", publisherSpy)
	defer publisher.Factory.Register("wechat", &publisher.WechatPublisher{})

	user := models.User{Username: "owner"}
	require.NoError(t, db.Create(&user).Error)
	project := models.Project{
		UserID:        user.ID,
		Title:         "Queued post",
		SourceContent: "<p>ready</p>",
		Status:        models.ProjectStatusReady,
	}
	require.NoError(t, db.Create(&project).Error)
	createConnectedQueueAccount(t, db, user.ID, "wechat")
	require.NoError(t, db.Create(&models.ProjectPlatformPublication{
		ProjectID:      project.ID,
		Platform:       "wechat",
		Enabled:        true,
		Status:         models.PublicationStatusPublishing,
		Config:         datatypes.JSON(`{"title":"Queued post"}`),
		AdaptedContent: datatypes.JSON(`{"format":"html","html":"ready"}`),
	}).Error)

	job := PublishJob{
		JobID:      uuid.New(),
		ProjectID:  project.ID,
		UserID:     user.ID,
		Platform:   "wechat",
		EnqueuedAt: time.Now().UTC(),
	}
	lockKey := publishLockKey(project.ID, "wechat")
	queue.locks[lockKey] = job.JobID.String()

	errs := make(chan error, 1)
	go func() {
		errs <- service.processPublishJob(context.Background(), job)
	}()

	select {
	case <-publisherSpy.entered:
	case <-time.After(time.Second):
		t.Fatal("expected publisher to start")
	}
	queue.locks[lockKey] = uuid.New().String()

	var err error
	select {
	case err = <-errs:
	case <-time.After(time.Second):
		close(publisherSpy.release)
		t.Fatal("expected publish job to stop after lock ownership loss")
	}

	require.NoError(t, err)

	var saved models.ProjectPlatformPublication
	require.NoError(t, db.First(&saved, "project_id = ? AND platform = ?", project.ID, "wechat").Error)
	require.Equal(t, models.PublicationStatusPublishing, saved.Status)
	require.Zero(t, saved.RetryCount)
	require.Empty(t, saved.ErrorMessage)
	require.Empty(t, observer.observations)
	require.NotEmpty(t, queue.locks[lockKey])
	require.NotEqual(t, job.JobID.String(), queue.locks[lockKey])
}

func TestRedisPublishQueueEnqueuesAsynqTask(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer func() { _ = client.Close() }()

	queue := NewRedisPublishQueue(client)
	job := PublishJob{
		JobID:      uuid.New(),
		ProjectID:  uuid.New(),
		UserID:     uuid.New(),
		Platform:   "wechat",
		EnqueuedAt: time.Now().UTC(),
	}

	require.NoError(t, queue.Enqueue(context.Background(), job))
	require.NoError(t, queue.Enqueue(context.Background(), job))

	inspector := asynq.NewInspectorFromRedisClient(client)
	tasks, err := inspector.ListPendingTasks(publishQueueName)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, publishTaskType, tasks[0].Type)
	require.Equal(t, publishTaskMaxRetry, tasks[0].MaxRetry)
	require.Equal(t, publishTaskTimeout, tasks[0].Timeout)

	var payload PublishJob
	require.NoError(t, json.Unmarshal(tasks[0].Payload, &payload))
	require.Equal(t, job, payload)
	require.Contains(t, string(tasks[0].Payload), `"workspace_id"`)
}

func TestStartPublishWorkerWithErrorsReportsRunnerFailure(t *testing.T) {
	expectedErr := errors.New("publish worker stopped")
	service := NewService(nil, nil)
	service.queue = &errorReportingPublishQueue{
		testPublishQueue: newTestPublishQueue(),
		err:              expectedErr,
	}

	errs := service.StartPublishWorkerWithErrors(context.Background())
	if errs == nil {
		t.Fatal("expected publish worker error channel")
	}

	select {
	case err := <-errs:
		require.ErrorIs(t, err, expectedErr)
	case <-time.After(time.Second):
		t.Fatal("expected publish worker error")
	}
}
