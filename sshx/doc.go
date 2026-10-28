// Package sshx 是 SSH 客户端的通用原语：拨号、私钥、密钥类型识别、保活。
//
// 为什么不直接用 golang.org/x/crypto/ssh：它给的是协议原语，拨号这一步要
// 做的判断一个都没替你做——连接超时、握手超时、调用方的总预算、以及
// 「超时」与「被拒绝」这两种失败的区分，都要自己写一遍。写错一次的后果
// 不是报错难看，而是调用方永远等下去。
//
// 本包的四件事：
//
//   - Dial：两段界限的拨号（连接一段、握手一段），并接受调用方的总预算
//   - LoadSigner / DetectKeyType：私钥的读、解析与类型识别
//   - PublicKeyType：公钥的类型识别
//   - StartKeepAlive：空闲连接的保活
//
// 用法：
//
//	signer, err := sshx.LoadSigner("/root/.ssh/id_rsa")
//	if err != nil {
//		return err
//	}
//	client, err := sshx.Dial(ctx, "10.0.0.1:2222", &ssh.ClientConfig{
//		User:            "ops",
//		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
//		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
//	}, sshx.DialOptions{Budget: 10 * time.Second})
//
// 与 x/crypto/ssh 的关系是叠加不是替代：拨号与凭据用本包，会话与通道
// 仍然用 x/crypto/ssh。
package sshx
