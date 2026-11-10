package sshx

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

// writePrivateKey 生成一把 ed25519 私钥写进临时目录，返回路径。
func writePrivateKey(t *testing.T, name string) string {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate a key: %v", err)
	}
	block, err := ssh.MarshalPrivateKey(key, "test")
	if err != nil {
		t.Fatalf("marshal the key: %v", err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("write the key: %v", err)
	}
	return path
}

// TestDetectKeyType_ED25519 断言 ed25519 私钥被认出来。
//
// 这一条是「密钥类型不对」能提前发现的根据：服务端接受并报告成功，客户端
// 在登录时拒绝，两端判断不一致时中间没有任何东西会说明原因。
func TestDetectKeyType_ED25519(t *testing.T) {
	path := writePrivateKey(t, "id_ed25519")
	kind, err := DetectKeyType(context.Background(), path)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if kind != "ssh-ed25519" {
		t.Errorf("类型 = %q，期望 %q", kind, "ssh-ed25519")
	}
}

// TestDetectKeyType_MissingFile 断言读不到文件是错误（路径就是错的）。
func TestDetectKeyType_MissingFile(t *testing.T) {
	if _, err := DetectKeyType(context.Background(), filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("a missing key file should be an error")
	}
}

// TestDetectKeyType_FallsBackToHeader 断言解析不了时落到文件头。
//
// 一份内容坏掉的 PEM：解析不了，ssh-keygen 也读不出——但头还在，于是类型
// 仍能给出粗粒度的结论，而不是一句「认不出」。
func TestDetectKeyType_FallsBackToHeader(t *testing.T) {
	cases := []struct {
		name string
		head string
		want string
	}{
		{"传统 RSA", "-----BEGIN RSA PRIVATE KEY-----\nnot-a-real-key\n-----END RSA PRIVATE KEY-----\n", "ssh-rsa"},
		{"传统 EC", "-----BEGIN EC PRIVATE KEY-----\nnot-a-real-key\n-----END EC PRIVATE KEY-----\n", "ecdsa"},
		{"OpenSSH 新格式", "-----BEGIN OPENSSH PRIVATE KEY-----\nnot-a-real-key\n-----END OPENSSH PRIVATE KEY-----\n",
			"openssh-format (type not detectable without ssh-keygen)"},
		{"不认识", "just some bytes\n", "unknown"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "key")
			if err := os.WriteFile(path, []byte(c.head), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			got, err := DetectKeyType(context.Background(), path)
			if err != nil {
				t.Fatalf("detect: %v", err)
			}
			if got != c.want {
				t.Errorf("类型 = %q，期望 %q", got, c.want)
			}
		})
	}
}

// TestLoadSigner 断言私钥能被读出并解析成签名器。
func TestLoadSigner(t *testing.T) {
	path := writePrivateKey(t, "id_ed25519")
	signer, err := LoadSigner(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if signer.PublicKey().Type() != "ssh-ed25519" {
		t.Errorf("公钥类型 = %q，期望 %q", signer.PublicKey().Type(), "ssh-ed25519")
	}
}

// TestLoadSigner_MissingFile 断言读不到文件是错误。
func TestLoadSigner_MissingFile(t *testing.T) {
	if _, err := LoadSigner(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("a missing key file should be an error")
	}
}

// TestPublicKeyType 断言公钥类型被认出来。
func TestPublicKeyType(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	pub := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey())))

	if got := PublicKeyType(pub); got != "ssh-ed25519" {
		t.Errorf("类型 = %q，期望 %q", got, "ssh-ed25519")
	}
	if _, err := ParsePublicKey(pub); err != nil {
		t.Errorf("一份合法的公钥不该被拒：%v", err)
	}
}

// TestPublicKeyType_BrokenKey 断言坏公钥的答案是「它说自己是什么」，
// 而不是「认不出」。
//
// 调用方要的往往正是那句「你给的是 ed25519，而这里只收 ssh-rsa」——所以
// 连一把坏钥匙也得报得出它的类型。
func TestPublicKeyType_BrokenKey(t *testing.T) {
	if got := PublicKeyType("ssh-ed25519 AAAA-not-base64 user@host"); got != "ssh-ed25519" {
		t.Errorf("类型 = %q，期望 %q", got, "ssh-ed25519")
	}
	if _, err := ParsePublicKey("ssh-ed25519 AAAA-not-base64 user@host"); err == nil {
		t.Error("一把坏公钥应当被 ParsePublicKey 拒掉")
	}
	if got := PublicKeyType("   "); got != "" {
		t.Errorf("空输入的类型 = %q，期望空串", got)
	}
}

// TestParseSinglePublicKey 断言「恰好一把」被守住。
//
// ParseAuthorizedKey 是逐行扫描的：解析不了的行被跳过，取第一条能解析的。
// 于是「第一行 ed25519、第二行 rsa」会被判成 rsa——而调用方接下来通常是
// 把整个字符串存下去。**被检查的东西与被存下的东西不是同一个**。
func TestParseSinglePublicKey(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	ed := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey())))

	t.Run("一把钥匙", func(t *testing.T) {
		if _, err := ParseSinglePublicKey(ed); err != nil {
			t.Errorf("一把合法的钥匙应当通过：%v", err)
		}
	})

	t.Run("多行：第一行是 ed25519", func(t *testing.T) {
		// 这正是漏洞的形状：逐行扫描会跳过第一行（这里它能解析，所以更糟）。
		_, err := ParseSinglePublicKey("garbage\n" + ed)
		if err == nil {
			t.Error("多行输入应当被拒")
		}
	})

	t.Run("一行塞两把：第二把落在 comment 位置也算两把（#151）", func(t *testing.T) {
		// authorized_keys 的语法是「[options] type base64 [comment]」⇒ 一行里
		// 第二个 type/base64 形状的东西会落进 comment（comment 是自由文本）。
		// 但这一行存进系统时存的就是两把钥匙——「恰好一把」的承诺必须拒掉它
		// （#151：旧判据查 rest，而单行输入的 rest 恒为空，那条防线是不可达
		// 的死代码，这一行被放了过去）。
		if _, err := ParseSinglePublicKey(ed + " " + ed); err == nil {
			t.Error("一行塞两把应当被拒")
		}
	})

	t.Run("注释文字后跟第二把：同样算两把（#151）", func(t *testing.T) {
		if _, err := ParseSinglePublicKey(ed + " note " + ed); err == nil {
			t.Error("注释文字后藏着第二把也应当被拒")
		}
	})

	t.Run("一把加普通注释：注释不是公钥就放行（#151 不误伤）", func(t *testing.T) {
		if _, err := ParseSinglePublicKey(ed + " user@host"); err != nil {
			t.Errorf("一把钥匙加普通注释应当通过：%v", err)
		}
	})

	t.Run("空", func(t *testing.T) {
		if _, err := ParseSinglePublicKey("   "); err == nil {
			t.Error("空输入应当被拒")
		}
	})
}

// TestDetectKeyType_NilContext 回归（#100）：解析不了的私钥（带口令/损坏/
// 非 OpenSSH 格式）必然走到第二级 ssh-keygen，而 exec.CommandContext 对
// nil ctx 直接 panic——#79 的兜底只盖了 Dial，没盖这条路径。现在入口兜底
// Background（#100，与 #79 同仓口径）：nil ctx 走完三级回落拿头结论，不再
// 崩进程。
func TestDetectKeyType_NilContext(t *testing.T) {
	// 一份内容坏掉的 OpenSSH 私钥：ParsePrivateKey 解析不了 ⇒ 进第二级
	// （此前在这里 panic），ssh-keygen 也读不出 ⇒ 落到第三级文件头。
	path := filepath.Join(t.TempDir(), "broken.pem")
	broken := "-----BEGIN OPENSSH PRIVATE KEY-----\nnot-a-real-key\n-----END OPENSSH PRIVATE KEY-----\n"
	if err := os.WriteFile(path, []byte(broken), 0o600); err != nil {
		t.Fatalf("write the broken key: %v", err)
	}

	// 类型化的 nil（而非字面 nil）：staticcheck SA1012 拦字面 nil 传参，
	// 而 nil 值的 context.Context 变量在运行时就是 nil 接口——回归语义不变。
	var nilCtx context.Context
	kind, err := DetectKeyType(nilCtx, path)
	if err != nil {
		t.Fatalf("DetectKeyType(nil ctx): %v (nil ctx must fall back to Background, not panic, #100)", err)
	}
	if !strings.Contains(kind, "openssh") {
		t.Errorf("type = %q, want the header-level openssh-format conclusion", kind)
	}
}
