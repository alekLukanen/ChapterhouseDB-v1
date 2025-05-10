package operations

import (
	"context"
	"log/slog"

	taskpackets "github.com/alekLukanen/ChapterhouseDB-v1/taskPackets"
	"github.com/alekLukanen/ChapterhouseDB-v1/tasker"
	"github.com/alekLukanen/errs"
)

func BuildTasker(ctx context.Context, logger *slog.Logger, options tasker.Options) (*tasker.Tasker, error) {

	tr, err := tasker.NewTasker(ctx, logger, options)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	// register queueus used by
	tr = tr.RegisterQueue(
		tasker.Queue{Name: "tuple-processing", Type: tasker.DelayedQueue},
	)

	// register task packets
	tr = tr.RegisterTaskPacket(
		&taskpackets.TablePartitionTaskPacket{},
	)

	return tr, nil
}
