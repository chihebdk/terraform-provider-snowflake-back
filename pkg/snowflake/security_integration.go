package snowflake

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// SecurityIntegration returns a pointer to a Builder that abstracts the DDL operations for a security integration.
//
// Supported DDL operations are:
//   - CREATE NOTIFICATION INTEGRATION
//   - ALTER NOTIFICATION INTEGRATION
//   - DROP INTEGRATION
//   - SHOW INTEGRATIONS
//   - DESCRIBE INTEGRATION
//
// [Snowflake Reference](https://docs.snowflake.net/manuals/sql-reference/ddl-user-security.html#security-integrations)
func SecurityIntegration(name string) *Builder {
	return &Builder{
		entityType: SecurityIntegrationType,
		name:       name,
	}
}

type securityIntegration struct {
	Name            sql.NullString `db:"name"`
	Category        sql.NullString `db:"category"`
	IntegrationType sql.NullString `db:"type"`
	CreatedOn       sql.NullString `db:"created_on"`
	Enabled         sql.NullBool   `db:"enabled"`
}

func ScanSecurityIntegration(row *sqlx.Row) (*securityIntegration, error) {
	r := &securityIntegration{}
	err := row.StructScan(r)
	return r, err
}
