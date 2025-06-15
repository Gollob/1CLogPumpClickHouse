package batch

import (
	"context"
	"time"

	"go.uber.org/zap"

	"1CLogPumpClickHouse/clickhouseclient"
	"1CLogPumpClickHouse/models"
)

// Batcher накапливает пачку логов и отправляет их в ClickHouse пачками
// batchSize — сколько строк отправлять за раз
// batchInterval — максимальный интервал между отправками (секунды)
type Batcher struct {
	batchSize     int
	batchInterval time.Duration
	logger        *zap.Logger
	chClient      *clickhouseclient.Client
}

// NewBatcher создает новый batcher
func NewBatcher(batchSize int, batchInterval int, logger *zap.Logger, ch *clickhouseclient.Client) *Batcher {
	return &Batcher{
		batchSize:     batchSize,
		batchInterval: time.Duration(batchInterval) * time.Second,
		logger:        logger,
		chClient:      ch,
	}
}

// Run запускает сборку и отправку batch в ClickHouse
func (b *Batcher) Run(ctx context.Context, in <-chan models.LogEntry) {
	batch := make([]models.LogEntry, 0, b.batchSize)
	timer := time.NewTimer(b.batchInterval)
	defer timer.Stop()

	flush := func(reason string) {
		if len(batch) == 0 {
			return
		}
		b.logger.Info("Отправляем batch в ClickHouse", zap.Int("count", len(batch)), zap.String("reason", reason))
		err := b.chClient.InsertTechLogBatch(ctx, batch)
		if err != nil {
			b.logger.Error("Ошибка при отправке batch в ClickHouse", zap.Error(err))
		} else {
			b.logger.Info("Batch успешно отправлен", zap.Int("count", len(batch)))
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush("graceful shutdown")
			return
		case entry := <-in:
			batch = append(batch, entry)
			if len(batch) >= b.batchSize {
				flush("batch size reached")
				timer.Reset(b.batchInterval)
			}
		case <-timer.C:
			flush("interval")
			timer.Reset(b.batchInterval)
		}
	}
}
