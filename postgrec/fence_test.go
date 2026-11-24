package postgrec

import (
	"strings"
	"testing"
)

// 合法标识符_构造_产出带栅栏守卫的 upsert 语句（列序：key, cols, fence）。
func TestFencedUpsertSQLBuildsGuardedStatement(t *testing.T) {
	q, err := FencedUpsertSQL("jobs", "id", []string{"state", "progress"}, "fence")
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	want := "INSERT INTO jobs (id, state, progress, fence) VALUES ($1, $2, $3, $4) " +
		"ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, progress = EXCLUDED.progress, fence = EXCLUDED.fence " +
		"WHERE jobs.fence <= EXCLUDED.fence"
	if q != want {
		t.Fatalf("语句不符:\n got: %s\nwant: %s", q, want)
	}
}

// 无数据列_构造_产出仅守护栅栏列的语句（首写即持锁宣告场景）。
func TestFencedUpsertSQLWithoutDataCols(t *testing.T) {
	q, err := FencedUpsertSQL("leases", "id", nil, "fence")
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	want := "INSERT INTO leases (id, fence) VALUES ($1, $2) " +
		"ON CONFLICT (id) DO UPDATE SET fence = EXCLUDED.fence " +
		"WHERE leases.fence <= EXCLUDED.fence"
	if q != want {
		t.Fatalf("语句不符:\n got: %s\nwant: %s", q, want)
	}
}

// schema 限定表名_构造_各处限定一致（INSERT、ON CONFLICT 后的 WHERE 均带限定）。
func TestFencedUpsertSQLQualifiedTable(t *testing.T) {
	q, err := FencedUpsertSQL("public.jobs", "id", []string{"state"}, "fence")
	if err != nil {
		t.Fatalf("构造失败: %v", err)
	}
	want := "INSERT INTO public.jobs (id, state, fence) VALUES ($1, $2, $3) " +
		"ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, fence = EXCLUDED.fence " +
		"WHERE public.jobs.fence <= EXCLUDED.fence"
	if q != want {
		t.Fatalf("语句不符:\n got: %s\nwant: %s", q, want)
	}
}

// 非法标识符_构造_返回错误（注入面在构造期堵死，入参是开发期常量）。
func TestFencedUpsertSQLRejectsInvalidIdentifiers(t *testing.T) {
	cases := []struct {
		name           string
		table, keyCol  string
		cols           []string
		fenceCol       string
		wantContainSeg string // 报错信息应含的非法片段
	}{
		{"空表名", "", "id", nil, "fence", `invalid table name ""`},
		{"表名带分号", "jobs; DROP TABLE jobs", "id", nil, "fence", `invalid table name "jobs; DROP TABLE jobs"`},
		{"表名带引号", `jobs"--`, "id", nil, "fence", `invalid table name`},
		{"列名带空格", "jobs", "user id", nil, "fence", `invalid column name "user id"`},
		{"列名以数字开头", "jobs", "id", nil, "1fence", `invalid column name "1fence"`},
		{"数据列带括号", "jobs", "id", []string{"state)"}, "fence", `invalid column name "state)"`},
		{"空栅栏列", "jobs", "id", nil, "", `invalid column name ""`},
		{"数据列与主键重复", "jobs", "id", []string{"id"}, "fence", `duplicate column "id"`},
		{"数据列之间重复", "jobs", "id", []string{"state", "state"}, "fence", `duplicate column "state"`},
		{"数据列与栅栏列重复", "jobs", "id", []string{"fence"}, "fence", `duplicate column "fence"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := FencedUpsertSQL(c.table, c.keyCol, c.cols, c.fenceCol)
			if err == nil {
				t.Fatalf("want error, got nil")
			}
			if !strings.Contains(err.Error(), c.wantContainSeg) {
				t.Fatalf("报错不含 %s: %v", c.wantContainSeg, err)
			}
		})
	}
}

// 合法边界标识符_构造_放行（下划线开头、纯下划线、大小写混合均属常规名称）。
func TestFencedUpsertSQLAcceptsBoundaryIdentifiers(t *testing.T) {
	if _, err := FencedUpsertSQL("_tmp", "_id", []string{"State_1"}, "f_ence"); err != nil {
		t.Fatalf("边界标识符被误拒: %v", err)
	}
}
