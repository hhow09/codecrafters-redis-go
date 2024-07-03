package persistence

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/codecrafters-io/redis-starter-go/app/database"
)

type Config struct {
	Dir        string
	Dbfilename string
}

func LoadRDB(config Config) ([]*database.DB, error) {
	defaultDBs := make([]*database.DB, redisDefaultDBSize)
	for i := range defaultDBs {
		defaultDBs[i] = database.NewDB()
	}
	if config.Dbfilename == "" {
		return defaultDBs, nil
	}
	path := filepath.Join(config.Dir, config.Dbfilename)
	b, err := os.ReadFile(path)
	if err != nil {
		return defaultDBs, nil
	}
	if err := os.WriteFile("./mykey_myval.rdb", b, 0644); err != nil {
		return nil, fmt.Errorf("fail to write rdb file: %w", err)
	}
	rdb, err := UnMarshalRDB(b)
	if err != nil {
		return nil, fmt.Errorf("fail to unmarshal rdb file: %w", err)
	}
	dbs := make([]*database.DB, len(rdb.DBs))
	for i, db := range rdb.DBs {
		dbs[i] = database.NewFromLoad(db.Datas)
	}
	return dbs, nil
}
