package postgrec

import (
	"fmt"
	"regexp"
	"strings"
)

// 下游栅栏防护（ADR-0004）：Fencing Token 只有下游存储配合检查才有效——
// 业务行存住曾写入的最大栅栏，仅接受不小于它的写入，迟到的旧持有者写入
// 即被拒绝。本文件把正确的条件更新语句做成开箱即用的构造器，使用者拿
// pgx 池/连接/事务任一执行面执行即可。
//
// 语义（两个后端的下游 helper 一致）：
//
//   - 接受 incoming >= stored，拒绝 incoming < stored。相等必须放行：
//     可重入沿用同一栅栏（ADR-0003 只在获取事件递增），同代持有者的
//     再次写入不是迟到写入。手写检查最常见的错误正是把相等也拒掉。
//   - 拒绝以 0 行受影响表达（pgx CommandTag.RowsAffected()==0），是
//     正常结果而非错误：调用方自行决定放弃或重试。
//   - 防护是惰性的：迟到的写入在真实持有者（更新代）首次写入后才必然
//     被拒——这是栅栏模式的固有边界，此前的窗口由锁互斥本身覆盖。

// identRe 是标识符白名单：只放行常规名称（字母/下划线开头，字母数字下划线
// 组成）。构造器输出的标识符不加引号——加引号会把大小写变成敏感语义，与
// 使用者建表时的直觉相反；白名单内的名称交由 PG 按常规折叠规则处理。
var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// FencedUpsertSQL 构造受栅栏守护的 upsert 语句：首写插入（行不存在，无栅栏
// 可比），后续写入在 ON CONFLICT 分支里以 `stored <= incoming` 守卫，迟到的
// 旧持有者写入影响 0 行。
//
// 参数：
//
//	table    业务表名，允许 schema 限定（如 "public.jobs"，逐段校验）
//	keyCol   主键列（表上须有唯一约束，ON CONFLICT 依赖它定位冲突）
//	cols     数据列；语句的 SET 与 VALUES 均含它们，允许为空
//	fenceCol 栅栏列（须 NOT NULL DEFAULT 0：NULL 参与 <= 比较恒为假，
//	         会让该行的后续写入被永久拒绝）
//
// 返回的语句参数顺序固定为 (key, cols 的值..., fence)；对 *pgxpool.Pool、
// *pgxpool.Conn、pgx.Tx 通用：
//
//	q, err := postgrec.FencedUpsertSQL("jobs", "id", []string{"state"}, "fence")
//	tag, err := tx.Exec(ctx, q, "42", "running", lk.FencingToken())
//	if tag.RowsAffected() == 0 { /* 迟到写入被拒 */ }
//
// 标识符经白名单校验（防注入），非法即返回错误——入参是开发期常量，
// 校验失败属于构造期编程错误。
func FencedUpsertSQL(table, keyCol string, cols []string, fenceCol string) (string, error) {
	parts := strings.Split(table, ".")
	for _, p := range parts {
		if !identRe.MatchString(p) {
			return "", fmt.Errorf("postgrec: fenced upsert: invalid table name %q", table)
		}
	}
	if err := validIdent(keyCol); err != nil {
		return "", err
	}
	if err := validIdent(fenceCol); err != nil {
		return "", err
	}
	seen := map[string]struct{}{keyCol: {}, fenceCol: {}}
	for _, c := range cols {
		if err := validIdent(c); err != nil {
			return "", err
		}
		if _, dup := seen[c]; dup {
			return "", fmt.Errorf("postgrec: fenced upsert: duplicate column %q", c)
		}
		seen[c] = struct{}{}
	}

	all := append(append([]string{keyCol}, cols...), fenceCol)
	values := make([]string, len(all))
	for i := range all {
		values[i] = fmt.Sprintf("$%d", i+1)
	}
	set := make([]string, 0, len(cols)+1)
	for _, c := range cols {
		set = append(set, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
	}
	set = append(set, fmt.Sprintf("%s = EXCLUDED.%s", fenceCol, fenceCol))

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s WHERE %s.%s <= EXCLUDED.%s",
		table, strings.Join(all, ", "), strings.Join(values, ", "),
		keyCol, strings.Join(set, ", "),
		table, fenceCol, fenceCol,
	), nil
}

func validIdent(name string) error {
	if !identRe.MatchString(name) {
		return fmt.Errorf("postgrec: fenced upsert: invalid column name %q", name)
	}
	return nil
}
