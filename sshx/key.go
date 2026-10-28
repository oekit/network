package sshx

// key.go 是凭据这一侧的原语：读私钥、认私钥、认公钥。
//
// 为什么值得单独一个文件：「密钥类型不对」这件事在部署里极难排查——
// 服务端常常接受并报告成功（字段写进去了、状态是好的），而客户端在登录
// 时才拒绝它。两端对同一个密钥的判断不一致时，中间没有任何东西会说明
// 原因。所以「这个密钥是什么类型」得有一个能提前问的答案。

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/crypto/ssh"
)

// LoadSigner 读私钥文件并解析成签名器。
//
// 只认未加密的私钥：带口令的私钥要口令或者要 agent，这两样都不是本包的
// 事（调用方手里有 ssh-agent 的话，agent 本身会完成握手）。
func LoadSigner(path string) (ssh.Signer, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read key %s: %w", path, err)
	}
	signer, err := ssh.ParsePrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("parse key %s: %w", path, err)
	}
	return signer, nil
}

// DetectKeyType 报告私钥文件的密钥类型（"ssh-rsa" / "ssh-ed25519" /
// "ecdsa-sha2-nistp256" 这类）。
//
// 三级回落，越靠前越准：
//
//  1. 直接解析私钥。最准，而且不需要外部进程——公钥类型就在签名器里。
//  2. ssh-keygen -y。解析不了的私钥（比如带口令的）在这里还有救：有
//     agent 时 ssh-keygen 能读出公钥。
//  3. 文件头嗅探。最后的手段，只能给出粗粒度的结论。
//
// 读不出文件是错误（调用方拿到的路径就是错的）；读得出但认不出类型不是
// 错误——返回 "unknown" 或粗粒度结论，让调用方自己去判断。
func DetectKeyType(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("cannot read %s: %w", path, err)
	}
	// 第一级：直接解析。
	if signer, perr := ssh.ParsePrivateKey(raw); perr == nil {
		return signer.PublicKey().Type(), nil
	}
	// 第二级：ssh-keygen。ssh-keygen 不在或拒绝了文件时落到第三级。
	if out, kerr := exec.Command("ssh-keygen", "-y", "-f", path).Output(); kerr == nil {
		if fields := strings.Fields(string(out)); len(fields) > 0 {
			return fields[0], nil
		}
	}
	// 第三级：文件头。
	return headerKeyType(raw), nil
}

// headerKeyType 从私钥文件的 PEM 头猜类型。
//
// 猜得到什么：RSA 与 EC 的传统 PEM 头是分开的。OpenSSH 新格式（一个头包
// 所有类型）猜不出来——所以这一级对 OpenSSH 格式只能给出「格式对、类型
// 定不了」的结论。
func headerKeyType(raw []byte) string {
	head := string(raw)
	switch {
	case strings.Contains(head, "BEGIN RSA PRIVATE KEY"):
		return "ssh-rsa"
	case strings.Contains(head, "BEGIN EC PRIVATE KEY"):
		return "ecdsa"
	case strings.Contains(head, "BEGIN OPENSSH PRIVATE KEY"):
		return "openssh-format (type not detectable without ssh-keygen)"
	default:
		return "unknown"
	}
}

// ParsePublicKey 解析一行 SSH 公钥（authorized_keys 的那种格式）。
//
// 它比「看第一个字段」严格：字段对但内容是坏 base64 的公钥，在这里会被
// 拒掉，而在字符串前缀检查里它是「通过」的——然后在登录那一刻才失败。
func ParsePublicKey(publicKey string) (ssh.PublicKey, error) {
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(strings.TrimSpace(publicKey)))
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}
	return pub, nil
}

// PublicKeyType 报告一行 SSH 公钥的类型（"ssh-rsa" / "ssh-ed25519" /
// "ecdsa-sha2-nistp256" 这类）。
//
// 解析不了就退回首字段——「这是什么类型」这个问题即使公钥本身是坏的也
// 答得出来，而调用方要的往往正是那句「你给的是 ed25519，而这里只收
// ssh-rsa」。空串表示这一行什么都没说。
func PublicKeyType(publicKey string) string {
	fields := strings.Fields(strings.TrimSpace(publicKey))
	if len(fields) == 0 {
		return ""
	}
	if pub, err := ParsePublicKey(publicKey); err == nil {
		return pub.Type()
	}
	return fields[0]
}
