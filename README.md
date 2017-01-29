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
