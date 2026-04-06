# 启动凭证 API 文档

本文档说明按邮箱启动凭证的接口：[`POST /admin/oauth/enable-by-email`](src/routes/admin.js:1000)。

该接口可用于按邮箱启动对应的 `antigravity` 或 `geminicli` 凭证，并执行一次启用校验。

---

## 1. 接口用途

该接口用于：

- 通过邮箱查找对应凭证
- 按模式选择 `antigravity` 或 `geminicli`
- 调用现有启用逻辑进行一次完整启动校验
- 返回是否启用成功及实际命中的凭证信息

适用场景：

- 某个邮箱账号已被禁用后，外部程序希望直接触发一次重新启用检测
- 已知邮箱，希望不通过后台页面，直接调用 API 重新校验该凭证

---

## 2. 接口地址

`POST /admin/oauth/enable-by-email`

对应实现位置：

- 路由实现：[router.post("/oauth/enable-by-email")](src/routes/admin.js:1000)
- Antigravity 启用校验：[enableTokenById()](src/auth/token_manager.js:1662)
- Gemini CLI 启用校验：[enableTokenById()](src/auth/geminicli_token_manager.js:1616)

---

## 3. 鉴权方式

该接口要求传入管理员密码 `password`。

### 请求头

```http
Content-Type: application/json
```

### 请求体参数

| 参数       | 类型   | 必填 | 说明                                                        |
| ---------- | ------ | ---: | ----------------------------------------------------------- |
| `password` | string |   是 | 管理员密码                                                  |
| `email`    | string |   是 | 目标邮箱                                                    |
| `mode`     | string |   否 | 启动模式，支持 `auto` / `antigravity` / `geminicli` / `cli` |

### mode 说明

- `auto`：默认值，优先尝试 `antigravity`，若没找到再尝试 `geminicli`
- `antigravity`：仅在 antigravity 凭证中查找并启动
- `geminicli`：仅在 Gemini CLI 凭证中查找并启动
- `cli`：`geminicli` 的别名

---

## 4. 调用示例

### 4.1 自动模式

```json
{
  "password": "your-admin-password",
  "email": "user@example.com",
  "mode": "auto"
}
```

### 4.2 指定 antigravity

```json
{
  "password": "your-admin-password",
  "email": "user@example.com",
  "mode": "antigravity"
}
```

### 4.3 指定 Gemini CLI

```json
{
  "password": "your-admin-password",
  "email": "user@example.com",
  "mode": "geminicli"
}
```

---

## 5. 启动校验逻辑

接口本身不会直接把凭证强行设为启用，而是复用现有启用检测流程：

### Antigravity 检测流程

由 [enableTokenById()](src/auth/token_manager.js:1662) 执行：

1. 刷新 token
2. 获取 `projectId`
3. 发送测试消息验证 API 是否可用
4. 全部通过后才真正启用

### Gemini CLI 检测流程

由 [enableTokenById()](src/auth/geminicli_token_manager.js:1616) 执行：

1. 刷新 token
2. 获取 `projectId`
3. 发送测试消息验证 API 是否可用
4. 全部通过后才真正启用

如果检测失败，返回的 `message` 会带出失败原因。

---

## 6. 成功返回示例

```json
{
  "success": true,
  "message": "Token启用成功",
  "data": {
    "email": "user@example.com",
    "mode": "antigravity",
    "tokenId": "token_xxx",
    "enable": true
  }
}
```

### 返回字段说明

| 字段           | 说明                                                |
| -------------- | --------------------------------------------------- |
| `success`      | 是否成功                                            |
| `message`      | 启用结果说明                                        |
| `data.email`   | 命中的邮箱                                          |
| `data.mode`    | 实际启用的来源，可能为 `antigravity` 或 `geminicli` |
| `data.tokenId` | 命中的凭证 ID                                       |
| `data.enable`  | 最终启用结果                                        |

---

## 7. 失败返回示例

### 7.1 密码错误

```json
{
  "success": false,
  "message": "密码验证失败"
}
```

### 7.2 未传邮箱

```json
{
  "success": false,
  "message": "email必填"
}
```

### 7.3 mode 不合法

```json
{
  "success": false,
  "message": "mode仅支持 auto / antigravity / geminicli / cli"
}
```

### 7.4 未找到该邮箱对应凭证

```json
{
  "success": false,
  "message": "未找到该邮箱对应的凭证"
}
```

### 7.5 启用检测失败

```json
{
  "success": false,
  "message": "凭证不可用，API 测试失败(403): ...",
  "data": {
    "email": "user@example.com",
    "mode": "antigravity",
    "tokenId": "token_xxx",
    "enable": false
  }
}
```

---

## 8. 补充说明

- 该接口适合外部程序自动化调用，不依赖前端页面。
- 若使用 `auto` 模式且同邮箱同时存在于两边，优先命中 `antigravity`。
- 若需要先获取 403 账号及对应 URL，可配合 [403api.md](403api.md) 中的 [`GET /admin/oauth/403-accounts`](src/routes/admin.js:947) 一起使用。
