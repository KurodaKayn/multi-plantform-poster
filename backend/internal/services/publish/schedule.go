package publish

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/kurodakayn/mpp-backend/internal/dto"
	"github.com/kurodakayn/mpp-backend/internal/models"
	"github.com/kurodakayn/mpp-backend/internal/services/accesspolicy"
)

const (
	scheduledPublicationPollInterval = 5 * time.Second
	scheduledPublicationBatchSize    = 25
)

var (
	errScheduledPublicationMissing      = errors.New("scheduled publication does not exist")
	errScheduledPublicationNotStartable = errors.New("scheduled publication is not startable")
)

func (s *Service) createScheduledPublication(ctx context.Context, project models.Project, pub models.ProjectPlatformPublication, userID uuid.UUID, idempotencyKey string, scheduledAt time.Time, status string) (models.ScheduledPublication, error) {
	if !s.writerDB(ctx).Migrator().HasTable(&models.ScheduledPublication{}) {
		return models.ScheduledPublication{}, nil
	}
	workspaceID := models.ProjectWorkspaceID(project)
	schedule := models.ScheduledPublication{
		WorkspaceID:       workspaceID,
		ProjectID:         project.ID,
		PublicationID:     pub.ID,
		PlatformAccountID: pub.PlatformAccountID,
		ProjectVersionID:  nil,
		ScheduledAt:       scheduledAt.UTC(),
		Timezone:          "UTC",
		Status:            status,
		IdempotencyKey:    idempotencyKey,
		CreatedBy:         userID,
	}
	if schedule.Status == "" {
		schedule.Status = models.ScheduledPublicationStatusScheduled
	}
	if latestVersionID, err := s.latestProjectVersionID(ctx, workspaceID, project.ID); err != nil {
		return models.ScheduledPublication{}, err
	} else if latestVersionID != uuid.Nil {
		schedule.ProjectVersionID = &latestVersionID
	}
	if err := s.writerDB(ctx).Create(&schedule).Error; err != nil {
		return models.ScheduledPublication{}, err
	}
	return schedule, nil
}

func (s *Service) ScheduleProjectPublication(ctx context.Context, projectID uuid.UUID, userID uuid.UUID, req dto.SchedulePublicationRequest) (*dto.ScheduledPublication, error) {
	if req.ScheduledAt.IsZero() || strings.TrimSpace(req.Platform) == "" {
		return nil, ErrPublicationRequiresSync
	}
	project, pub, err := s.preparePublishJob(ctx, projectID, req.Platform, userID)
	if err != nil {
		return nil, err
	}
	timezone := strings.TrimSpace(req.Timezone)
	if timezone == "" {
		timezone = "UTC"
	}
	idempotencyKey := normalizeIdempotencyKey(req.IdempotencyKey)
	if idempotencyKey != "" {
		if existing, found, err := s.findIdempotentScheduledPublication(ctx, models.ProjectWorkspaceID(project), project.ID, pub.ID, userID, idempotencyKey); err != nil {
			return nil, err
		} else if found {
			item := scheduledPublicationFromModel(existing, project, pub, nil)
			return &item, nil
		}
	}
	schedule, err := s.createScheduledPublication(ctx, project, pub, userID, idempotencyKey, req.ScheduledAt, models.ScheduledPublicationStatusScheduled)
	if err != nil {
		return nil, err
	}
	if schedule.ID == uuid.Nil {
		return nil, ErrPublicationRequiresSync
	}
	if timezone != schedule.Timezone {
		if err := s.writerDB(ctx).Model(&schedule).Update("timezone", timezone).Error; err != nil {
			return nil, err
		}
		schedule.Timezone = timezone
	}
	item := scheduledPublicationFromModel(schedule, project, pub, nil)
	return &item, nil
}

func (s *Service) findIdempotentScheduledPublication(ctx context.Context, workspaceID uuid.UUID, projectID uuid.UUID, publicationID uuid.UUID, userID uuid.UUID, key string) (models.ScheduledPublication, bool, error) {
	if strings.TrimSpace(key) == "" {
		return models.ScheduledPublication{}, false, nil
	}
	var schedule models.ScheduledPublication
	err := s.writerDB(ctx).
		Where("workspace_id = ? AND project_id = ? AND publication_id = ? AND created_by = ? AND idempotency_key = ?", workspaceID, projectID, publicationID, userID, key).
		Order("created_at DESC").
		First(&schedule).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return models.ScheduledPublication{}, false, nil
	}
	if err != nil {
		return models.ScheduledPublication{}, false, err
	}
	return schedule, true, nil
}

func (s *Service) CancelScheduledPublication(ctx context.Context, projectID uuid.UUID, scheduleID uuid.UUID, userID uuid.UUID) (*dto.ScheduledPublication, error) {
	if projectID == uuid.Nil || scheduleID == uuid.Nil || userID == uuid.Nil {
		return nil, ErrForbidden
	}
	var schedule models.ScheduledPublication
	if err := s.strongReadDB(ctx).
		Preload("Project").
		Preload("Publication").
		First(&schedule, "id = ? AND project_id = ?", scheduleID, projectID).Error; err != nil {
		return nil, err
	}
	if _, err := s.projectForPublish(ctx, schedule.ProjectID, userID); err != nil {
		return nil, err
	}
	if schedule.Status == models.ScheduledPublicationStatusRunning || schedule.Status == models.ScheduledPublicationStatusPublished {
		return nil, ErrPublicationAlreadyPublishing
	}
	now := time.Now().UTC()
	if err := s.writerDB(ctx).Model(&schedule).Updates(map[string]any{
		"status":       models.ScheduledPublicationStatusCancelled,
		"cancelled_by": userID,
		"updated_at":   now,
	}).Error; err != nil {
		return nil, err
	}
	schedule.Status = models.ScheduledPublicationStatusCancelled
	schedule.CancelledBy = &userID
	item := scheduledPublicationFromModel(schedule, schedule.Project, schedule.Publication, nil)
	return &item, nil
}

func (s *Service) RetryScheduledPublication(ctx context.Context, projectID uuid.UUID, scheduleID uuid.UUID, userID uuid.UUID) (*dto.ScheduledPublication, error) {
	if projectID == uuid.Nil || scheduleID == uuid.Nil || userID == uuid.Nil {
		return nil, ErrForbidden
	}
	var schedule models.ScheduledPublication
	if err := s.strongReadDB(ctx).
		Preload("Project").
		Preload("Publication").
		First(&schedule, "id = ? AND project_id = ?", scheduleID, projectID).Error; err != nil {
		return nil, err
	}
	if schedule.Status != models.ScheduledPublicationStatusFailed && schedule.Status != models.ScheduledPublicationStatusNeedsManualAction {
		return nil, ErrPublicationAlreadyPublishing
	}
	if _, err := s.PublishProjectInWorkspaceWithContext(ctx, schedule.WorkspaceID, projectID, schedule.Publication.Platform, &userID, scheduleID); err != nil {
		return nil, err
	}
	return s.scheduledPublicationDetail(ctx, scheduleID)
}

func (s *Service) ListWorkspaceScheduledPublications(ctx context.Context, workspaceID uuid.UUID, userID uuid.UUID, from time.Time, to time.Time) (*dto.ScheduledPublicationsResponse, error) {
	if workspaceID == uuid.Nil || userID == uuid.Nil || from.IsZero() || to.IsZero() || !to.After(from) {
		return nil, ErrForbidden
	}
	if err := s.requireWorkspaceCalendarAccess(ctx, workspaceID, userID); err != nil {
		return nil, err
	}
	var schedules []models.ScheduledPublication
	if err := s.strongReadDB(ctx).
		Preload("Project").
		Preload("Publication").
		Where("workspace_id = ? AND scheduled_at >= ? AND scheduled_at < ?", workspaceID, from, to).
		Order("scheduled_at ASC").
		Find(&schedules).Error; err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(schedules))
	for _, schedule := range schedules {
		ids = append(ids, schedule.ID)
	}
	attemptsBySchedule := map[uuid.UUID][]models.PublishAttempt{}
	if len(ids) > 0 {
		var attempts []models.PublishAttempt
		if err := s.strongReadDB(ctx).
			Where("scheduled_publication_id IN ?", ids).
			Order("attempt_no ASC").
			Find(&attempts).Error; err != nil {
			return nil, err
		}
		for _, attempt := range attempts {
			attemptsBySchedule[attempt.ScheduledPublicationID] = append(attemptsBySchedule[attempt.ScheduledPublicationID], attempt)
		}
	}
	items := make([]dto.ScheduledPublication, 0, len(schedules))
	for _, schedule := range schedules {
		items = append(items, scheduledPublicationFromModel(schedule, schedule.Project, schedule.Publication, attemptsBySchedule[schedule.ID]))
	}
	return &dto.ScheduledPublicationsResponse{Items: items}, nil
}

func (s *Service) scheduledPublicationDetail(ctx context.Context, scheduleID uuid.UUID) (*dto.ScheduledPublication, error) {
	var schedule models.ScheduledPublication
	if err := s.strongReadDB(ctx).
		Preload("Project").
		Preload("Publication").
		First(&schedule, "id = ?", scheduleID).Error; err != nil {
		return nil, err
	}
	var attempts []models.PublishAttempt
	if err := s.strongReadDB(ctx).
		Where("scheduled_publication_id = ?", scheduleID).
		Order("attempt_no ASC").
		Find(&attempts).Error; err != nil {
		return nil, err
	}
	item := scheduledPublicationFromModel(schedule, schedule.Project, schedule.Publication, attempts)
	return &item, nil
}

func (s *Service) requireWorkspaceCalendarAccess(ctx context.Context, workspaceID uuid.UUID, userID uuid.UUID) error {
	return accesspolicy.RequireWorkspaceMemberWithDB(s.strongReadDB(ctx), workspaceID, userID)
}

func (s *Service) StartScheduledPublicationDispatcher(ctx context.Context) {
	if s.queue == nil || s.db == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(scheduledPublicationPollInterval)
		defer ticker.Stop()

		for {
			if err := s.FlushScheduledPublications(ctx, scheduledPublicationBatchSize); err != nil && ctx.Err() == nil {
				log.Printf("scheduled publication flush failed: %v", err)
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *Service) FlushScheduledPublications(ctx context.Context, limit int) error {
	if s.queue == nil || s.db == nil {
		return nil
	}
	if limit <= 0 {
		limit = scheduledPublicationBatchSize
	}

	now := time.Now().UTC()
	var schedules []models.ScheduledPublication
	if err := s.writerDB(ctx).
		Preload("Publication").
		Where("status = ? AND scheduled_at <= ?", models.ScheduledPublicationStatusScheduled, now).
		Order("scheduled_at ASC").
		Limit(limit).
		Find(&schedules).Error; err != nil {
		return err
	}

	var firstErr error
	for _, schedule := range schedules {
		if err := s.dispatchScheduledPublication(ctx, schedule); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Service) dispatchScheduledPublication(ctx context.Context, schedule models.ScheduledPublication) error {
	if schedule.ID == uuid.Nil || schedule.ProjectID == uuid.Nil || schedule.CreatedBy == uuid.Nil || schedule.PublicationID == uuid.Nil {
		return nil
	}
	if schedule.Status != models.ScheduledPublicationStatusScheduled || schedule.ScheduledAt.After(time.Now().UTC()) {
		return nil
	}
	platform := strings.TrimSpace(schedule.Publication.Platform)
	if platform == "" {
		var publication models.ProjectPlatformPublication
		if err := s.writerDB(ctx).Select("platform").First(&publication, "id = ? AND workspace_id = ?", schedule.PublicationID, schedule.WorkspaceID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		platform = publication.Platform
	}
	if platform == "" {
		return nil
	}
	if schedule.WorkspaceID == uuid.Nil {
		return nil
	}

	job := PublishJob{
		JobID:          uuid.New(),
		ProjectID:      schedule.ProjectID,
		WorkspaceID:    schedule.WorkspaceID,
		UserID:         schedule.CreatedBy,
		Platform:       platform,
		PublicationID:  schedule.PublicationID,
		ScheduleID:     schedule.ID,
		IdempotencyKey: schedule.IdempotencyKey,
		EnqueuedAt:     time.Now().UTC(),
	}
	lockKey := publishLockKey(schedule.ProjectID, platform)
	coordinationQueue := s.coordinationQueueOrDefault()
	if coordinationQueue == nil {
		return ErrPublishQueueEmpty
	}
	acquired, err := coordinationQueue.AcquireLock(ctx, lockKey, job.JobID.String(), publishLockTTL)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	return s.processPublishJob(ctx, job)
}

func (s *Service) latestProjectVersionID(ctx context.Context, workspaceID uuid.UUID, projectID uuid.UUID) (uuid.UUID, error) {
	if !s.writerDB(ctx).Migrator().HasTable(&models.ProjectVersion{}) {
		return uuid.Nil, nil
	}
	var version models.ProjectVersion
	err := s.writerDB(ctx).Select("id").
		Where("workspace_id = ? AND project_id = ?", workspaceID, projectID).
		Order("version_number DESC, created_at DESC").
		First(&version).Error
	if err == nil {
		return version.ID, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return uuid.Nil, nil
	}
	return uuid.Nil, err
}

func scheduledPublicationFromModel(schedule models.ScheduledPublication, project models.Project, pub models.ProjectPlatformPublication, attempts []models.PublishAttempt) dto.ScheduledPublication {
	dtoAttempts := make([]dto.PublishAttempt, 0, len(attempts))
	for _, attempt := range attempts {
		dtoAttempts = append(dtoAttempts, dto.PublishAttempt{
			ID:                     attempt.ID,
			ScheduledPublicationID: attempt.ScheduledPublicationID,
			AttemptNo:              attempt.AttemptNo,
			StartedAt:              attempt.StartedAt,
			FinishedAt:             attempt.FinishedAt,
			Status:                 attempt.Status,
			RemoteID:               attempt.RemoteID,
			PublishURL:             attempt.PublishURL,
			ErrorCode:              attempt.ErrorCode,
			ErrorMessage:           attempt.ErrorMessage,
		})
	}
	return dto.ScheduledPublication{
		ID:                schedule.ID,
		WorkspaceID:       schedule.WorkspaceID,
		ProjectID:         schedule.ProjectID,
		PublicationID:     schedule.PublicationID,
		PlatformAccountID: schedule.PlatformAccountID,
		ProjectVersionID:  schedule.ProjectVersionID,
		Platform:          pub.Platform,
		ProjectTitle:      project.Title,
		ScheduledAt:       schedule.ScheduledAt,
		Timezone:          schedule.Timezone,
		Status:            schedule.Status,
		IdempotencyKey:    schedule.IdempotencyKey,
		CreatedBy:         schedule.CreatedBy,
		ApprovedBy:        schedule.ApprovedBy,
		CancelledBy:       schedule.CancelledBy,
		LastError:         schedule.LastError,
		ManualActionURL:   schedule.ManualActionURL,
		ManualActionUntil: schedule.ManualActionUntil,
		Attempts:          dtoAttempts,
		CreatedAt:         schedule.CreatedAt,
		UpdatedAt:         schedule.UpdatedAt,
	}
}
