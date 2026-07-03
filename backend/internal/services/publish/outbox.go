package publish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/kurodakayn/mpp-backend/internal/models"
)

const (
	publishOutboxPollInterval = 5 * time.Second
	publishOutboxBatchSize    = 25
	publishOutboxMaxBackoff   = 5 * time.Minute
	publishOutboxClaimTimeout = 2 * time.Minute
)

func (s *Service) recordPublishJobOutbox(job PublishJob) (models.OutboxEvent, error) {
	payload, err := json.Marshal(job)
	if err != nil {
		return models.OutboxEvent{}, err
	}
	event := models.OutboxEvent{
		AggregateType: models.OutboxAggregatePublishJob,
		AggregateID:   job.JobID,
		EventType:     models.OutboxEventPublishJobRequested,
		Payload:       datatypes.JSON(payload),
		Status:        models.OutboxStatusPending,
	}
	return event, s.writerDB(s.requestContext()).Create(&event).Error
}

func (s *Service) StartPublishOutboxDispatcher(ctx context.Context) {
	if s.queue == nil || s.db == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(publishOutboxPollInterval)
		defer ticker.Stop()

		for {
			if err := s.FlushPublishOutbox(ctx, publishOutboxBatchSize); err != nil && ctx.Err() == nil {
				log.Printf("publish outbox flush failed: %v", err)
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (s *Service) FlushPublishOutbox(ctx context.Context, limit int) error {
	if s.queue == nil {
		return ErrPublishQueueEmpty
	}
	if s.db == nil {
		return nil
	}
	if limit <= 0 {
		limit = publishOutboxBatchSize
	}

	now := time.Now().UTC()
	staleProcessingCutoff := now.Add(-publishOutboxClaimTimeout)
	var events []models.OutboxEvent
	if err := s.writerDB(ctx).
		Where("event_type = ? AND ((status IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) OR (status = ? AND updated_at <= ?))",
			models.OutboxEventPublishJobRequested,
			[]string{models.OutboxStatusPending, models.OutboxStatusFailed},
			now,
			models.OutboxStatusProcessing,
			staleProcessingCutoff,
		).
		Order("created_at ASC").
		Limit(limit).
		Find(&events).Error; err != nil {
		return err
	}

	var firstErr error
	for _, event := range events {
		if err := s.dispatchOutboxEvent(ctx, event.ID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Service) dispatchOutboxEvent(ctx context.Context, eventID uuid.UUID) error {
	if eventID == uuid.Nil {
		return nil
	}
	event, claimed, err := s.claimOutboxEvent(ctx, eventID)
	if err != nil || !claimed {
		return err
	}
	if err := s.dispatchClaimedOutboxEvent(ctx, event); err != nil {
		if markErr := s.markOutboxEventFailed(ctx, event, err); markErr != nil {
			return errors.Join(err, markErr)
		}
		return err
	}
	return s.markOutboxEventDispatched(ctx, event)
}

func (s *Service) claimOutboxEvent(ctx context.Context, eventID uuid.UUID) (models.OutboxEvent, bool, error) {
	now := time.Now().UTC()
	staleProcessingCutoff := now.Add(-publishOutboxClaimTimeout)
	result := s.writerDB(ctx).Model(&models.OutboxEvent{}).
		Where("id = ? AND event_type = ? AND ((status IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) OR (status = ? AND updated_at <= ?))",
			eventID,
			models.OutboxEventPublishJobRequested,
			[]string{models.OutboxStatusPending, models.OutboxStatusFailed},
			now,
			models.OutboxStatusProcessing,
			staleProcessingCutoff,
		).
		Updates(map[string]any{
			"status":        models.OutboxStatusProcessing,
			"attempts":      gorm.Expr("attempts + ?", 1),
			"error_message": "",
		})
	if result.Error != nil {
		return models.OutboxEvent{}, false, result.Error
	}
	if result.RowsAffected == 0 {
		return models.OutboxEvent{}, false, nil
	}
	var event models.OutboxEvent
	if err := s.writerDB(ctx).First(&event, "id = ?", eventID).Error; err != nil {
		return models.OutboxEvent{}, false, err
	}
	return event, true, nil
}

func (s *Service) dispatchClaimedOutboxEvent(ctx context.Context, event models.OutboxEvent) error {
	if event.EventType != models.OutboxEventPublishJobRequested {
		return fmt.Errorf("unsupported outbox event type: %s", event.EventType)
	}
	var job PublishJob
	if err := json.Unmarshal(event.Payload, &job); err != nil {
		return fmt.Errorf("decode publish outbox payload: %w", err)
	}
	if err := s.ensurePublishJobWorkspaceID(ctx, &job); err != nil {
		return fmt.Errorf("resolve publish outbox workspace for event %s: %w", event.ID, err)
	}
	if job.JobID == uuid.Nil || job.ProjectID == uuid.Nil || job.WorkspaceID == uuid.Nil || job.UserID == uuid.Nil || job.Platform == "" {
		return fmt.Errorf("invalid publish outbox payload for event %s", event.ID)
	}
	return s.queue.Enqueue(ctx, job)
}

func (s *Service) markOutboxEventDispatched(ctx context.Context, event models.OutboxEvent) error {
	now := time.Now().UTC()
	return s.writerDB(ctx).Model(&models.OutboxEvent{}).
		Where("id = ? AND status = ? AND attempts = ?", event.ID, models.OutboxStatusProcessing, event.Attempts).
		Updates(map[string]any{
			"status":          models.OutboxStatusDispatched,
			"processed_at":    &now,
			"next_attempt_at": nil,
			"error_message":   "",
		}).Error
}

func (s *Service) markOutboxEventFailed(ctx context.Context, event models.OutboxEvent, dispatchErr error) error {
	nextAttemptAt := time.Now().UTC().Add(outboxRetryBackoff(event.Attempts))
	return s.writerDB(ctx).Model(&models.OutboxEvent{}).
		Where("id = ? AND status = ? AND attempts = ?", event.ID, models.OutboxStatusProcessing, event.Attempts).
		Updates(map[string]any{
			"status":          models.OutboxStatusFailed,
			"next_attempt_at": &nextAttemptAt,
			"error_message":   SanitizeUserFacingErrorMessage(dispatchErr.Error()),
		}).Error
}

func outboxRetryBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return time.Second
	}
	backoff := time.Duration(attempts) * time.Second
	if backoff > publishOutboxMaxBackoff {
		return publishOutboxMaxBackoff
	}
	return backoff
}
