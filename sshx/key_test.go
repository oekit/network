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
