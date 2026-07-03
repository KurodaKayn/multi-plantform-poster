package publish

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/kurodakayn/mpp-backend/internal/models"
)

type PublicationLifecycle struct {
	db *gorm.DB
}

type publicationCompletion struct {
	Status       string
	RemoteID     string
	PublishURL   string
	ErrorMessage string
}

type publishAttemptCompletion struct {
	Status       string
	RemoteID     string
	PublishURL   string
	ErrorMessage string
}

func NewPublicationLifecycle(db *gorm.DB) PublicationLifecycle {
	return PublicationLifecycle{db: db}
}

func (s *Service) lifecycle() PublicationLifecycle {
	return NewPublicationLifecycle(s.writerDB(s.requestContext()))
}

func (l PublicationLifecycle) MarkQueued(pub *models.ProjectPlatformPublication, queuedAt time.Time) error {
	result := l.publicationQuery(pub).
		Where("status = ?", pub.Status).
		Updates(map[string]any{
			"status":          models.PublicationStatusQueued,
			"error_message":   "",
			"last_attempt_at": &queuedAt,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return l.publicationStateChangedError(pub.ID)
	}
	pub.Status = models.PublicationStatusQueued
	pub.LastAttemptAt = &queuedAt
	return nil
}

func (l PublicationLifecycle) MarkPublishing(pub *models.ProjectPlatformPublication, startedAt time.Time) error {
	result := l.publicationQuery(pub).
		Where("status = ?", pub.Status).
		Updates(map[string]any{
			"status":          models.PublicationStatusPublishing,
			"error_message":   "",
			"last_attempt_at": &startedAt,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return l.publicationStateChangedError(pub.ID)
	}
	pub.Status = models.PublicationStatusPublishing
	pub.LastAttemptAt = &startedAt
	return nil
}

func (l PublicationLifecycle) MarkFailed(ctx context.Context, projectID uuid.UUID, platform string, message string) error {
	return l.MarkFailedForWorkspace(ctx, uuid.Nil, projectID, platform, message)
}

func (l PublicationLifecycle) MarkFailedForWorkspace(ctx context.Context, workspaceID uuid.UUID, projectID uuid.UUID, platform string, message string) error {
	db := l.db
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	query := db.Model(&models.ProjectPlatformPublication{}).
		Where("project_id = ? AND platform = ?", projectID, platform).
		Scopes(workspaceScope(workspaceID))
	return query.
		Updates(map[string]any{
			"status":        models.PublicationStatusFailed,
			"error_message": SanitizeUserFacingErrorMessage(message),
			"retry_count":   gorm.Expr("retry_count + ?", 1),
		}).Error
}

func (l PublicationLifecycle) CompletePublication(pub *models.ProjectPlatformPublication, completion publicationCompletion) error {
	updates := map[string]any{
		"status":        completion.Status,
		"remote_id":     completion.RemoteID,
		"publish_url":   completion.PublishURL,
		"error_message": completion.ErrorMessage,
	}
	if completion.Status == models.PublicationStatusSucceeded {
		publishedAt := time.Now().UTC()
		updates["published_at"] = &publishedAt
	} else {
		updates["retry_count"] = gorm.Expr("retry_count + ?", 1)
	}
	result := l.publicationQuery(pub).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return l.publicationStateChangedError(pub.ID)
	}
	pub.Status = completion.Status
	pub.RemoteID = completion.RemoteID
	pub.PublishURL = completion.PublishURL
	pub.ErrorMessage = completion.ErrorMessage
	return nil
}

func (l PublicationLifecycle) publicationQuery(pub *models.ProjectPlatformPublication) *gorm.DB {
	query := l.db.Model(&models.ProjectPlatformPublication{}).Where("id = ?", pub.ID)
	if pub.WorkspaceID != uuid.Nil {
		query = query.Where("workspace_id = ?", pub.WorkspaceID)
	}
	return query
}

func (l PublicationLifecycle) StartPublishAttemptForWorkspace(workspaceID uuid.UUID, scheduleID uuid.UUID, startedAt time.Time) (models.PublishAttempt, bool, error) {
	if scheduleID == uuid.Nil {
		return models.PublishAttempt{}, false, nil
	}
	if !l.db.Migrator().HasTable(&models.ScheduledPublication{}) || !l.db.Migrator().HasTable(&models.PublishAttempt{}) {
		return models.PublishAttempt{}, false, nil
	}
	var attempt models.PublishAttempt
	if err := l.db.Transaction(func(tx *gorm.DB) error {
		scheduleQuery := tx.Model(&models.ScheduledPublication{}).
			Where("id = ? AND status IN ?", scheduleID, []string{
				models.ScheduledPublicationStatusScheduled,
				models.ScheduledPublicationStatusFailed,
				models.ScheduledPublicationStatusNeedsManualAction,
			}).
			Scopes(workspaceScope(workspaceID))
		result := scheduleQuery.Updates(map[string]any{
			"status":     models.ScheduledPublicationStatusRunning,
			"last_error": "",
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			var schedule models.ScheduledPublication
			if err := tx.Select("status").Scopes(workspaceScope(workspaceID)).First(&schedule, "id = ?", scheduleID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return errScheduledPublicationMissing
				}
				return err
			}
			return fmt.Errorf("%w: %s", errScheduledPublicationNotStartable, schedule.Status)
		}

		var attemptNo int
		if err := tx.Model(&models.PublishAttempt{}).
			Where("scheduled_publication_id = ?", scheduleID).
			Select("COALESCE(MAX(attempt_no), 0)").
			Scan(&attemptNo).Error; err != nil {
			return err
		}

		attempt = models.PublishAttempt{
			ScheduledPublicationID: scheduleID,
			AttemptNo:              attemptNo + 1,
			StartedAt:              startedAt,
			Status:                 models.PublishAttemptStatusRunning,
		}
		return tx.Create(&attempt).Error
	}); err != nil {
		if errors.Is(err, errScheduledPublicationMissing) {
			return models.PublishAttempt{}, false, nil
		}
		if errors.Is(err, errScheduledPublicationNotStartable) {
			return models.PublishAttempt{}, false, ErrPublicationAlreadyPublishing
		}
		return models.PublishAttempt{}, false, err
	}
	return attempt, true, nil
}

func (l PublicationLifecycle) StartPublishAttempt(scheduleID uuid.UUID, startedAt time.Time) (models.PublishAttempt, bool, error) {
	return l.StartPublishAttemptForWorkspace(uuid.Nil, scheduleID, startedAt)
}

func (l PublicationLifecycle) FinishPublishAttempt(attempt *models.PublishAttempt, completion publishAttemptCompletion) error {
	return l.FinishPublishAttemptForWorkspace(uuid.Nil, attempt, completion)
}

func (l PublicationLifecycle) FinishPublishAttemptForWorkspace(workspaceID uuid.UUID, attempt *models.PublishAttempt, completion publishAttemptCompletion) error {
	if attempt == nil {
		return nil
	}
	finishedAt := time.Now().UTC()
	scheduleStatus := models.ScheduledPublicationStatusPublished
	errorCode := ""
	if completion.Status != models.PublishAttemptStatusSucceeded {
		scheduleStatus = models.ScheduledPublicationStatusFailed
		errorCode = completion.Status
	}
	return l.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.PublishAttempt{}).
			Where("id = ?", attempt.ID).
			Updates(map[string]any{
				"finished_at":   &finishedAt,
				"status":        completion.Status,
				"remote_id":     completion.RemoteID,
				"publish_url":   completion.PublishURL,
				"error_code":    errorCode,
				"error_message": completion.ErrorMessage,
			}).Error; err != nil {
			return err
		}
		return tx.Model(&models.ScheduledPublication{}).
			Where("id = ?", attempt.ScheduledPublicationID).
			Scopes(workspaceScope(workspaceID)).
			Updates(map[string]any{
				"status":     scheduleStatus,
				"last_error": completion.ErrorMessage,
			}).Error
	})
}

func (l PublicationLifecycle) MarkPrepublishSyncing(projectID uuid.UUID, platforms []string) error {
	return l.MarkPrepublishSyncingForWorkspace(uuid.Nil, projectID, platforms)
}

func (l PublicationLifecycle) MarkPrepublishSyncingForWorkspace(workspaceID uuid.UUID, projectID uuid.UUID, platforms []string) error {
	if len(platforms) == 0 {
		return nil
	}
	return l.db.Transaction(func(tx *gorm.DB) error {
		var activeCount int64
		if err := tx.Model(&models.ProjectPlatformPublication{}).
			Where("project_id = ? AND platform IN ? AND status IN ?", projectID, platforms, activePublicationStatuses()).
			Scopes(workspaceScope(workspaceID)).
			Count(&activeCount).Error; err != nil {
			return err
		}
		if activeCount > 0 {
			return ErrPublicationAlreadyPublishing
		}

		if err := tx.Model(&models.ProjectPlatformPublication{}).
			Where("project_id = ? AND platform IN ? AND status NOT IN ?", projectID, platforms, activePublicationStatuses()).
			Scopes(workspaceScope(workspaceID)).
			Updates(map[string]any{
				"draft_status":  models.PublicationDraftStatusSyncing,
				"error_message": "",
				"status":        models.PublicationStatusSyncing,
			}).Error; err != nil {
			return err
		}

		if err := tx.Model(&models.ProjectPlatformPublication{}).
			Where("project_id = ? AND platform IN ? AND status IN ?", projectID, platforms, activePublicationStatuses()).
			Scopes(workspaceScope(workspaceID)).
			Count(&activeCount).Error; err != nil {
			return err
		}
		if activeCount > 0 {
			return ErrPublicationAlreadyPublishing
		}

		return nil
	})
}

func activePublicationStatuses() []string {
	return []string{
		models.PublicationStatusQueued,
		models.PublicationStatusPublishing,
	}
}

func workspaceScope(workspaceID uuid.UUID) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if workspaceID == uuid.Nil {
			return db
		}
		return db.Where("workspace_id = ?", workspaceID)
	}
}

func (l PublicationLifecycle) publicationStateChangedError(publicationID uuid.UUID) error {
	var pub models.ProjectPlatformPublication
	if err := l.db.Select("status", "last_attempt_at").First(&pub, "id = ?", publicationID).Error; err != nil {
		return err
	}
	if pub.Status == models.PublicationStatusQueued || pub.Status == models.PublicationStatusPublishing {
		return ErrPublicationAlreadyPublishing
	}
	return ErrPublicationRequiresSync
}
