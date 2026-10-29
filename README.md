# AI 岗位核查台 · 站点

`https://giabom.online` 的静态站点，基于 [Fuwari](https://github.com/saicaca/fuwari)（Astro）构建。

## 这个仓库是什么

只负责**展示**。数据由 [ai-jobs-console](https://github.com/giabom163/ai-jobs-console) 生成：

```
公开招聘页  ->  规则判定是否真 AI 岗  ->  核验链接存活  ->  导出静态数据  ->  本站
```

## 本地开发

需要 Node.js >= 20、pnpm >= 9。

```bash
pnpm install
pnpm dev       # http://localhost:4321
pnpm build     # 构建 + pagefind 全文索引
```

部署：`deploy.bat`

## 目录

| 路径 | 内容 |
|---|---|
| `src/` | 页面、组件、布局 |
| `src/content/` | 岗位数据（由流水线写入） |
| `docs/` | 交接文档 |
| `deploy.bat` | 一键部署 |

## 说明

- 站点模板来自 [Fuwari](https://github.com/saicaca/fuwari)，MIT 协议
- 只聚合**公开招聘信息**，不存储个人简历
- 旧「舆情观察台」已下线，代码在归档仓库
