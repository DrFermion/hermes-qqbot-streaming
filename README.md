# hermes-qqbot-streaming

给 [Hermes Agent](https://github.com/NousResearch/hermes-agent) 的 QQ 机器人加**打字机式流式回复**——模型一边生成，QQ 里那条消息一边长出来，而不是等半分钟砸一大坨文字。顺带把 QQ 平台几个坑补齐：被动回复窗口、工具进度帧、长消息切分。

> 一个 Hermes 插件：加载时接管内置 `qqbot` 平台适配器，**不改 Hermes 源码**，卸载即还原。

## 效果

| | 默认 Hermes | 装了本插件 |
|---|---|---|
| 私聊回复 | 想完 → 一次性发一大条 | 边想边写，同一条消息里逐字长出来 |
| 工具跑一半 | 没有中间反馈 | 先冒一个"🔧 terminal…"气泡，正文随后单独一条流式 |
| 回复超长 | 按 (1/3) 切条，表格可能被切两半 | 表格整张不切；超过单条上限时自动改走普通分段发送，不丢字 |
| 被动回复窗口过期 | 那条回复直接失败 | 自动去掉 `msg_id` 改发主动消息，回复照样落地 |

## 安装

三种方式任选：

```bash
# 1) 从 GitHub 装（推荐，可随 hermes 插件系统升级）
hermes plugins install DrFermion/hermes-qqbot-streaming
hermes plugins enable qqbot-streaming
hermes gateway restart

# 2) 手动丢进插件目录
git clone https://github.com/DrFermion/hermes-qqbot-streaming ~/.hermes/plugins/qqbot-streaming
hermes plugins enable qqbot-streaming && hermes gateway restart

# 3) pip 安装（走 hermes_agent.plugins 入口点）
pip install git+https://github.com/DrFermion/hermes-qqbot-streaming
```

前提：Hermes 里 QQ 平台已经配好（`.env` 里有 `QQ_APP_ID` / `QQ_CLIENT_SECRET`，`platforms.qqbot.enabled: true`）。

## 配置

- **默认全开**，不需要额外配置。
- 关掉流式、退回一次性回复：

  ```bash
  hermes config set platforms.qqbot.extra.streaming false
  ```

- 插件会顺带把 `display.platforms.qqbot.streaming` 的默认值设为 `true`（Hermes 只给了 WeCom 这个默认值）。
  想单独用配置覆盖：`hermes config set display.platforms.qqbot.streaming false`（用户配置优先）。

## 它做了什么

**1. C2C 原生流式**（`stream_messages` 协议，私聊专属）

QQ 的流式消息是**一条服务端消息被反复重写**（`input_mode: replace`，每帧带全文）。本插件把这套协议接到 Hermes 的 stream consumer 上：同一 `msg_seq`、`index` 递增、首帧返回的 `stream_msg_id` 挂在后续帧、`input_state` 10 收尾、帧间隔 ≥300ms、429/50002 指数退避。

**2. 帧必须"只长不改"**（实测规则，见下表）

网关的 native 帧内容 = 正文 + `\n\n---\n` + 工具进度行，而工具进度在正文继续的瞬间被清掉 —— 于是下一帧变成"重画"，QQ 直接拒。插件把中间帧的工具进度遮罩剥掉（回复自己的 `---` 分节线不受影响），让帧保持单调；万一还是分叉（分段重置等），就**先把当前气泡封口、再开一条新消息**接着写，而不是丢掉内容或把整个聊天拉黑。

**3. 超长回复不丢字**

单条 QQ 消息有上限，replace 模式的流没法装下超出的部分。收尾帧一旦超预算，插件会以已显示的头部封口，然后**把全文交回 Hermes 的普通分段发送**（网关会回滚投递标记并补发完整回复），而不是静默截断。

**4. 出站加固**

- **被动回复窗口**：QQ 只允许在入站消息的回复窗口内被动回复，且对单条入站消息的回复条数有上限（腾讯官方 SDK 记 ~4 条/小时）。被拒时自动去掉 `msg_id` 重发一次（主动消息），长回合的最后一条不会凭空消失。
- **REST 解析容错**：CDN 返回 HTML/空 body 时报出带状态码和片段的可读错误，而不是几层之后炸成 `AttributeError: 'list' object has no attribute 'get'`。
- **限流退避**：HTTP 429 / biz 50002 指数退避，不再紧循环怼。
- **表格不切**：`truncate_message` 不在 GFM 表格中间断开（表格拆条后会渲染成散行）。

## QQ 流式协议：实测规则表

文档没写、只有真机能问出来的部分（每一条都是在真实平台上 POST 试出来的）：

| 行为 | 平台回答 |
|---|---|
| 首帧 `index=0` 或 `index=1` | `200`（不强制从 0 开始） |
| 后续帧 `index` 不递增 | `404 / 40006 请求参数index需要递增` |
| **帧内容不是上一帧的前缀续写**（重画/变短/换内容） | **`404 / 40007 已经提交的消息内容不可修改`** |
| 续写（在旧文本尾巴上追加） | `200` |
| 收尾帧（`input_state=10`）+ 续写内容 | `200` |
| 收尾帧 + 原文重复 | `200` |
| 后续帧不带 `stream_msg_id` | `400 / 40054005 消息被去重，请检查请求msgseq` |
| 同 `msg_seq` 重发相同内容 | `400 / 40054005`（去重） |
| 无 `msg_id` 的主动消息 | `200`（可用，但受平台风控配额约束） |

一句话：**QQ 的流只能长个子，不能改头换面。** 组/频道没有流式接口，只有私聊（C2C）有。

## 验证装好了

重启网关后，在 QQ 里随便发一句，然后看日志：

```bash
grep "C2C stream" ~/.hermes/logs/gateway.log
```

成功一轮长这样（三行齐 = 干净，只发一条、不重复）：

```
C2C stream opened for <openid> (msg_seq=...)
C2C stream closed after N frame(s) (M chars)
Suppressing normal final send for session ... (streamed=True content_delivered=True)
```

故障特征：

- `QQ streaming unavailable for this chat, using one-shot replies` → 平台拒绝且被判为永久（本插件已把 40006/40007/40054005 归为可恢复）。
- `Normal final-send NOT suppressed ... possible duplicate send` → 流式没投递成功、网关补发了全文（内容不丢，属降级路径）。

## 限制

- **只有私聊能流**：群/频道没有 `stream_messages`，群里回复仍是一次性消息（平台限制，不是插件能绕的）。
- **流式必须挂在真实入站消息的回复窗口上**：机器人自己造不出第一条，所以"重启网关后日志里没有 stream"通常只是没人发过消息。
- **超长回复**（> `MAX_MESSAGE_LENGTH`，QQ 用 4000 字符）走普通分段发送：能保证不丢字，但不是打字机效果。
- **纯文本**：流式期间发的是纯文本——`*`、`` ` ``、`#` 会被剔掉（用逐字符删除，保证帧始终是前缀延伸）。原因是客户端一旦把消息判成 markdown，就会**原样摊出源码**（`##`、`**` 全露着），所以流式帧里不带标记，读起来才正常。代价是流式那条没有排版。（注意：剔标记**并不能**去掉下面那条「暂不支持查看」的提示——那句是流式消息本身带来的，与内容无关。）
- **⚠ 已知限制：那句「该类型消息暂不支持查看」消不掉。** QQ 客户端对**流式消息**本身就会贴这句提示。live 探测过所有能想到的绕法，全部无效：
  - 删掉内容里的 markdown 标记、改发纯文本 → 照旧；把换行换成 U+2028 → 照旧；长消息/短消息、多行/单行 → 照旧
  - 把消息**撤回**也不行：`DELETE /v2/users/{openid}/messages/{message_id}` 对**流式消息**返回 `400 / 40061001 请求参数无效`（流式响应返回的是「流 id」，不是可撤回的消息 id），而**普通消息**的撤回是好的（实测 HTTP 200）
  - 同一个群的**普通发送**（`POST /v2/users/{openid}/messages`）**没有**这句提示
  
  结论：**「打字机效果」和「开头干净」在 QQ 这一侧只能二选一**，本插件默认选打字机。想要干净消息就把流式关掉——在 `config.yaml` 里写 `display.platforms.qqbot.streaming: false`（插件读的是平台级配置，这个显式关闭会覆盖插件默认值），重启网关即可。
- **版本兼容**：如果将来 Hermes 内置了 QQ 流式，本插件会自动改走"加固"部分（不再叠加 mixin，避免 MRO 冲突）。
- 平台条目一旦注册，**创建适配器失败时 Hermes 不会回退到内置适配器**（`run_adapters.py` 的设计）。所以别在缺依赖（aiohttp/httpx）的环境里启用本插件。

## 开发

```bash
# 需要一个装好 Hermes 的解释器
~/.hermes/hermes-agent/venv/Scripts/python.exe -m pytest tests/ -q
```

测试覆盖：帧协议（同 msg_seq / index 递增 / stream_msg_id 接力 / 收尾帧）、单调性（遮罩剥离、光标剥离、纯文本投影、分叉时**只追加未显示部分**而不重开消息）、首帧扣留、超预算交回、限流与永久错误分类、被动回复兜底、表格切分。

## 许可

MIT。QQ 流式协议细节来自对 QQ 开放平台的真机实测；实现思路对照过腾讯官方 `@tencent-connect/qqbot-nodejs` SDK。

---

## English (short)

A Hermes plugin that gives the QQ bot **typewriter streaming** for private chats: the reply grows
inside one QQ message while the model generates, instead of arriving as a single wall of text.

```bash
hermes plugins install DrFermion/hermes-qqbot-streaming
hermes plugins enable qqbot-streaming
hermes gateway restart
```

It registers a `qqbot` platform adapter (plugin registry beats built-ins) that subclasses Hermes's
own adapter — nothing in the Hermes tree is patched. Beyond streaming it hardens: QQ's passive-reply
window (retry once as a proactive send), frame monotonicity (QQ refuses repaints with
`404 / 40007`), over-long replies (hand the tail back to the normal chunked send), tolerant REST
error parsing, rate-limit backoff and GFM-table-safe chunking.

Groups and channels cannot stream — QQ only exposes `stream_messages` for C2C. See the table above
for the full protocol rules probed against the live platform.

MIT licensed.
