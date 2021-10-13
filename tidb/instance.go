package tidb

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"go.uber.org/zap/zapcore"
)

type Instance struct {
	ctx    context.Context
	driver *server.TiDBDriver
}

func NewInstance() (*Instance, error) {
	log.SetLevel(zapcore.ErrorLevel)

	storage, err := mockstore.NewMockStore()
	if err != nil {
		return nil, err
	}
	session.DisableStats4Test()
	session.BootstrapSession(storage)
	driver := server.NewTiDBDriver(storage)

	ctx := context.Background()
	db := &Instance{
		ctx,
		driver,
	}
	return db, nil
}

func (i *Instance) OpenContext() (*Context, error) {
	qctx, err := i.driver.OpenCtx(uint64(0), 0, uint8(mysql.DefaultCollationID), "", nil)
	if err != nil {
		return nil, err
	}
	vars := qctx.GetSessionVars()
	vars.AllowAggPushDown = false
	vars.EnableClusteredIndex = variable.ClusteredIndexDefModeOff
	vars.EnableIndexMergeJoin = false
	vars.SetAllowInSubqToJoinAndAgg(false)

	ctx := &Context{
		i, qctx,
	}
	return ctx, nil
}
