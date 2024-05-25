package runners

import (
	"log/slog"

	"github.com/alekLukanen/chapterhouseDB/warehouse"
)

type SingleThreadedRunner struct {
	logger *slog.Logger

	warehouse *warehouse.Warehouse
}
