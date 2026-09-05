package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nhatminh06/forgeci/internal/scm"
	"github.com/nhatminh06/forgeci/internal/store"
)

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

const scmRepositoryColumns = `id::text,provider,full_name,pipeline_path,enabled,created_at,updated_at`

func scanSCMRepository(row pgx.Row) (*scm.Repository, error) {
	var out scm.Repository
	if err := row.Scan(&out.ID, &out.Provider, &out.FullName, &out.PipelinePath, &out.Enabled, &out.CreatedAt, &out.UpdatedAt); err != nil {
		return nil, err
	}
	out.CreatedAt, out.UpdatedAt = out.CreatedAt.UTC(), out.UpdatedAt.UTC()
	return &out, nil
}

func (s *Store) CreateSCMRepository(ctx context.Context, in scm.Repository) (*scm.Repository, error) {
	normalized, err := scm.NormalizeRepository(in.Provider, in.FullName)
	if err != nil {
		return nil, err
	}
	pipeline, err := scm.ValidatePipelinePath(in.PipelinePath)
	if err != nil {
		return nil, err
	}
	if in.ID == "" {
		in.ID = uuid.NewString()
	}
	out, err := scanSCMRepository(s.pool.QueryRow(ctx, `INSERT INTO scm_repositories(id,provider,full_name,normalized_full_name,pipeline_path,enabled)
		VALUES($1,$2,$3,$4,$5,$6) RETURNING `+scmRepositoryColumns, in.ID, in.Provider, in.FullName, normalized, pipeline, in.Enabled))
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrConflict
		}
		return nil, err
	}
	return out, nil
}

func (s *Store) GetSCMRepository(ctx context.Context, id string) (*scm.Repository, error) {
	out, err := scanSCMRepository(s.pool.QueryRow(ctx, `SELECT `+scmRepositoryColumns+` FROM scm_repositories WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return out, err
}

func (s *Store) GetSCMRepositoryByIdentity(ctx context.Context, provider scm.Provider, fullName string) (*scm.Repository, error) {
	normalized, err := scm.NormalizeRepository(provider, fullName)
	if err != nil {
		return nil, err
	}
	out, err := scanSCMRepository(s.pool.QueryRow(ctx, `SELECT `+scmRepositoryColumns+` FROM scm_repositories WHERE provider=$1 AND normalized_full_name=$2`, provider, normalized))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return out, err
}

func (s *Store) ListSCMRepositories(ctx context.Context) ([]scm.Repository, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+scmRepositoryColumns+` FROM scm_repositories ORDER BY provider,normalized_full_name,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []scm.Repository
	for rows.Next() {
		item, err := scanSCMRepository(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *item)
	}
	return out, rows.Err()
}

func (s *Store) DeleteSCMRepository(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM scm_repositories WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}
