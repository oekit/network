package nats

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoHardcodedErrKeyLiterals 钉住 #176：本仓非测试 Go 代码的日志错误
// 字段名必须用 logx.ErrKey 常量（值 "err"），禁止手写 "err" 字面量——
// 两矩阵落同一 Loki/ES，字段名漂移会拆散检索。修复前 7:2 分裂（nats/
// client.go 同文件内 105/464 手写、326/376 用常量）。CI grep 门禁的同款
// 语义固化为测试护栏：本仓无根包，护栏挂在 nats 包测试里、扫全仓全部
// 非 _test.go 的 .go 文件，行内出现 `"err",` 或 `"err")` 即失败（注释行
// 豁免）。
func TestNoHardcodedErrKeyLiterals(t *testing.T) {
	root := ".."
	visit := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(data), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if strings.Contains(line, `"err",`) || strings.Contains(line, `"err")`) {
				t.Errorf("%s:%d 手写 \"err\" 字面量违反 AGENTS.md（错误字段名统一用 logx.ErrKey，#176）",
					path, i+1)
			}
		}
		return nil
	}
	if err := filepath.WalkDir(root, visit); err != nil {
		t.Fatalf("walk repo: %v", err)
	}
}
