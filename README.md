# bcoin-stratum

A segwit-capable stratum server on top of bcoin. This is a bcoin plugin which
will run a stratum server in the same process as a bcoin fullnode. WIP.

## Cutting out the middleman

While having a stratum+fullnode marriage violates separation of concerns, it
provides a benefit to large competitive miners: because it sits in the same
process, there is no overhead of hitting/longpolling a JSON-rpc api to submit
or be notified of new blocks. It has direct in-memory access to all of the data
it needs. No getwork or getblocktemplate required.

It can also broadcast submitted blocks before verifying and saving them to disk
(since we created the block and know it's going to be valid ahead of time).

### Single point of failure?

There's nothing to say you can't have multiple bcoin-nodes/stratum-servers
behind a reverse/failover proxy still. It's only a single point of failure if
you treat it that way.

## Payouts

Shares are currently tracked by username and will be dumped to
`~/.bcoin/stratum/shares/[height]-[hash].json` when a block is found. A script
can parse through these later and either add the user's balance to a webserver
or pay directly to an address. Users are stored in a line-separated json file
in `~/.bcoin/stratum/users.json`.

## Administration

bcoin-stratum exposes some custom stratum calls:
`mining.authorize_admin('password')` to auth as an admin and
`mining.add_user('username', 'password')` to create a user during runtime.

## Todo

- Reverse/failover proxy for `HASH(sid)->bcoin-stratum-ip`.

## Contribution and License Agreement

If you contribute code to this project, you are implicitly allowing your code
to be distributed under the MIT license. You are also implicitly verifying that
all code is your original work. `</legalese>`

## License

Copyright (c) 2014-2016, Christopher Jeffrey (MIT License).

See LICENSE for more info.

# bcoin-stratum

Bcoin-stratum是一个插件，是bcoin之上的一个能支持隔离见证（segwit）的stratum server。这是bocin的一个插件，将可以和bcoin作为一个完整节点在同一个进程中运行stratum。正在开发中。

## 避开中间环节

尽管使用这个“stratum”+“全节点”的联姻违背了关注度分离（的软件设计准则），但它为竞争性的大矿工提供了优势：

由于它们在同一个进程中运行，所以在需要提交block和通知新block的时候，能够消除不断访问和轮询JSON-rpc api所带来的开销。

它拥有着所有所需数据的直接内存访问能力（权限）。故而不需要getwork或者getblocktemplate。

它也可以在将区块验证和将存盘之前广播提交区块（因为我们可以创建区块，并且可以提前知道它是有效的）。

### 单点故障？

因为无法使用反向/故障转移代理背后来运行多个bcoin-nodes/stratum-servers，所以即使是单个节点出现故障就没什么可说的了，这只是单个节点故障。

## 付款（Payouts）

在找一个区块后，根据用户名区分的哈希计算结果（Shares）将会存储在：（dumped）~/.bcoin/stratum/shares/[height]-[hash].json。之后用一个脚本来解析，并将用户的余额添加到网络服务器，或者直接支付到一个地址。不同的用户的json是分别分行储存中文件： ~/.bcoin/stratum/users.json。

## 管理

bcoin-stratum暴露了一些自定义层接口调用，如在运行时，通过mining.authorize_admin('password') 可授权一个管理员，通过mining.add_user('username', 'password') 可建立一个用户

## 接下来要开发的功能

- 为HASH(sid)->bcoin-stratum-ip做反向/故障转移代理。

## 贡献与许可协议

如果您为这个项目贡献代码，就默认你允许你的代码在MIT许可证下分发。你应该保证你的所有代码都是原创工作。</legalese>

## 许可

版权所有（C） 2014-2016, Christopher Jeffrey (MIT 许可证)。

更多的许可信息请查阅LICENSE。
