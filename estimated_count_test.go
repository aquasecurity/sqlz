package sqlz

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

type estimatedCountTestData struct {
	name             string
	driverName       string
	stmt             func(*DB) *SelectStmt
	createCountQuery bool
	roundedCount     bool
	expectedQuery    func(*sqlmock.Sqlmock)
	expectedCount    int64
	expectError      bool
}

var testEstCount = []estimatedCountTestData{
	{
		"estimated count with count query",
		"postgres",
		func(dbz *DB) *SelectStmt {
			return dbz.Select("*").From("audit")
		},
		true,
		true,
		func(mock *sqlmock.Sqlmock) {
			(*mock).
				ExpectQuery("^explain SELECT 1").
				WillReturnRows(
					sqlmock.NewRows([]string{"QUERY PLAN"}).
						AddRow(`Index Only Scan using audit_date_trunc_day_idx on audit  (cost=0.56..827577.76 rows=28603180 width=4)`))
		},
		20000000,
		false,
	}, {
		"estimated count with regular query",
		"pgx",
		func(dbz *DB) *SelectStmt {
			return dbz.Select("*").From("audit").Where(Eq("result", 2))
		},
		false,
		true,
		func(mock *sqlmock.Sqlmock) {
			(*mock).
				ExpectQuery(`^explain SELECT \*`).
				WillReturnRows(
					sqlmock.NewRows([]string{"QUERY PLAN"}).
						AddRow(`Seq Scan on audit  (cost=0.00..5626512.75 rows=14536136 width=1395)`))
		},
		10000000,
		false,
	}, {
		"estimated count is not supported",
		"mysql",
		func(dbz *DB) *SelectStmt {
			return dbz.Select("id").From("audit").Where(Eq("result", 4))
		},
		true,
		true,
		func(mock *sqlmock.Sqlmock) {
			(*mock).
				ExpectQuery(`^SELECT COUNT\(\*\)`).
				WillReturnRows(
					sqlmock.NewRows([]string{"count"}).
						AddRow(549))
		},
		549,
		false,
	}, {
		"estimated count without rounding",
		"postgres",
		func(dbz *DB) *SelectStmt {
			return dbz.Select("id").From("audit")
		},
		false,
		false,
		func(mock *sqlmock.Sqlmock) {
			(*mock).
				ExpectQuery("^explain SELECT id").
				WillReturnRows(
					sqlmock.NewRows([]string{"QUERY PLAN"}).
						AddRow(`Index Only Scan using audit_date_trunc_day_idx on audit  (cost=0.56..827577.76 rows=28603180 width=4)`))
		},
		28603180,
		false,
	}, {
		"estimated count multiple rows",
		"postgres",
		func(dbz *DB) *SelectStmt {
			return dbz.Select("id").From("audit")
		},
		true,
		true,
		func(mock *sqlmock.Sqlmock) {
			(*mock).
				ExpectQuery("^explain SELECT 1").
				WillReturnRows(
					sqlmock.NewRows([]string{"QUERY PLAN"}).
						AddRow(`Finalize GroupAggregate  (cost=5540869.61..5551244.43 rows=64841 width=24)`).
						AddRow(`   Group Key: (date_trunc('day'::text, to_timestamp((createtime)::double precision)))`).
						AddRow(`   ->  Gather Merge  (cost=5540869.61..5549611.77 rows=66392 width=24)`).
						AddRow(`         Workers Planned: 2`).
						AddRow(`         ->  Partial GroupAggregate  (cost=5539869.59..5540948.46 rows=33196 width=24)`).
						AddRow(`               Group Key: (date_trunc('day'::text, to_timestamp((createtime)::double precision)))`).
						AddRow(`               ->  Sort  (cost=5539869.59..5539952.58 rows=33196 width=12)`).
						AddRow(`                     Sort Key: (date_trunc('day'::text, to_timestamp((createtime)::double precision)))`).
						AddRow(`                     ->  Parallel Seq Scan on audit a  (cost=0.00..5537376.78 rows=33196 width=12)`).
						AddRow(`                          Filter: ((result = ANY ('{2,3}'::integer[])) AND ((containerid)::text <> ''::text) AND ((type)::text = 'Runtime'::text) AND ((data ->> 'hostid'::text) = '276678a8-27e5-4415-8e43-1a2b013458cd'::text))`))
		},
		60000,
		false,
	}, {
		"estimated count value in third row",
		"postgres",
		func(dbz *DB) *SelectStmt {
			return dbz.Select("id").From("audit")
		},
		true,
		true,
		func(mock *sqlmock.Sqlmock) {
			(*mock).
				ExpectQuery("^explain SELECT 1").
				WillReturnRows(
					sqlmock.NewRows([]string{"QUERY PLAN"}).
						AddRow(`Finalize GroupAggregate  (cost=5540869.61..5551244.43 width=24)`).
						AddRow(`   Group Key: (date_trunc('day'::text, to_timestamp((createtime)::double precision)))`).
						AddRow(`   ->  Gather Merge  (cost=5540869.61..5549611.77 rows=66392 width=24)`).
						AddRow(`         Workers Planned: 2`).
						AddRow(`         ->  Partial GroupAggregate  (cost=5539869.59..5540948.46 rows=33196 width=24)`).
						AddRow(`               Group Key: (date_trunc('day'::text, to_timestamp((createtime)::double precision)))`).
						AddRow(`               ->  Sort  (cost=5539869.59..5539952.58 rows=33196 width=12)`).
						AddRow(`                     Sort Key: (date_trunc('day'::text, to_timestamp((createtime)::double precision)))`).
						AddRow(`                     ->  Parallel Seq Scan on audit a  (cost=0.00..5537376.78 rows=33196 width=12)`).
						AddRow(`                          Filter: ((result = ANY ('{2,3}'::integer[])) AND ((containerid)::text <> ''::text) AND ((type)::text = 'Runtime'::text) AND ((data ->> 'hostid'::text) = '276678a8-27e5-4415-8e43-1a2b013458cd'::text))`))
		},
		60000,
		false,
	}, {
		"estimated count returns 0 due to absent rows values",
		"postgres",
		func(dbz *DB) *SelectStmt {
			return dbz.Select("id").From("audit")
		},
		true,
		true,
		func(mock *sqlmock.Sqlmock) {
			(*mock).
				ExpectQuery("^explain SELECT 1").
				WillReturnRows(
					sqlmock.NewRows([]string{"QUERY PLAN"}).
						AddRow(`Finalize GroupAggregate  (cost=5540869.61..5551244.43 width=24)`).
						AddRow(`   Group Key: (date_trunc('day'::text, to_timestamp((createtime)::double precision)))`).
						AddRow(`   ->  Gather Merge  (cost=5540869.61..5549611.77 width=24)`).
						AddRow(`         Workers Planned: 2`).
						AddRow(`         ->  Partial GroupAggregate  (cost=5539869.59..5540948.46 width=24)`).
						AddRow(`               Group Key: (date_trunc('day'::text, to_timestamp((createtime)::double precision)))`).
						AddRow(`               ->  Sort  (cost=5539869.59..5539952.58 width=12)`).
						AddRow(`                     Sort Key: (date_trunc('day'::text, to_timestamp((createtime)::double precision)))`).
						AddRow(`                     ->  Parallel Seq Scan on audit a  (cost=0.00..5537376.78 width=12)`).
						AddRow(`                          Filter: ((result = ANY ('{2,3}'::integer[])) AND ((containerid)::text <> ''::text) AND ((type)::text = 'Runtime'::text) AND ((data ->> 'hostid'::text) = '276678a8-27e5-4415-8e43-1a2b013458cd'::text))`))
		},
		0,
		true,
	}, {
		"estimated count returns 0 due to absent rows values",
		"postgres",
		func(dbz *DB) *SelectStmt {
			return dbz.Select("id").From("audit")
		},
		true,
		true,
		func(mock *sqlmock.Sqlmock) {
			(*mock).
				ExpectQuery("^explain SELECT 1").
				WillReturnRows(
					sqlmock.NewRows([]string{"QUERY PLAN"}))
		},
		0,
		true,
	}, {
		"estimated count returns 0 due to sql error",
		"postgres",
		func(dbz *DB) *SelectStmt {
			return dbz.Select("id").From("audit")
		},
		false,
		false,
		func(mock *sqlmock.Sqlmock) {
			(*mock).
				ExpectQuery("^explain SELECT id").
				WillReturnError(sql.ErrNoRows)
		},
		0,
		true,
	},
}

func TestEstimatedCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed creating mock database: %s", err)
	}
	for _, tt := range testEstCount {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.stmt(New(db, tt.driverName))
			tt.expectedQuery(&mock)
			count, err := q.GetEstimatedCount(tt.createCountQuery, tt.roundedCount)
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error %s", err)
			}
			if tt.expectError && err == nil {
				t.Error("Expecting error")
			}
			assert.EqualValues(t, tt.expectedCount, count)
			if err = mock.ExpectationsWereMet(); err != nil {
				t.Errorf("Test %s doesn't met all query expectations", tt.name)
			}
		})
	}
}
