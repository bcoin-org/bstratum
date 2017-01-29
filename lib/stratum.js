/*!
 * stratum.js - stratum server for bcoin
 * Copyright (c) 2014-2016, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

/* jshint -W069 */

var assert = require('assert');
var fs = require('fs');
var StringDecoder = require('string_decoder').StringDecoder;
var EventEmitter = require('events').EventEmitter;
var AsyncObject = require('bcoin/lib/utils/async');
var util = require('bcoin/lib/utils/util');
var Lock = require('bcoin/lib/utils/lock');
var encoding = require('bcoin/lib/utils/encoding');
var StaticWriter = require('bcoin/lib/utils/staticwriter');
var Address = require('bcoin/lib/primitives/address');
var co = require('bcoin/lib/utils/co');
var crypto = require('bcoin/lib/crypto/crypto');
var tcp = require('bcoin/lib/net/tcp');
var List = require('bcoin/lib/utils/list');
var IP = require('bcoin/lib/utils/ip');
var consensus = require('bcoin/lib/protocol/consensus');
var BN = require('bcoin/node_modules/bn.js');
var Block = require('bcoin/lib/primitives/block');
var Network = require('bcoin/lib/protocol/network');
var Logger = require('bcoin/lib/node/logger');

/*
 * Constants
 */

var DUMMY = new Buffer(0);
var NONCE_SIZE = 4;
var DIFF_TARGET = new BN(
  '00000000ffff0000000000000000000000000000000000000000000000000000',
  'hex'
);

/**
 * Stratum Server
 * @constructor
 * @param {Object} options
 */

function Stratum(options) {
  if (!(this instanceof Stratum))
    return new Stratum(options);

  AsyncObject.call(this);

  this.options = new StratumOptions(options);

  this.node = this.options.node;
  this.chain = this.options.chain;
  this.network = this.options.network;
  this.logger = this.options.logger;
  this.difficulty = this.options.difficulty;
  this.sharedb = new ShareDB(this.options);
  this.userdb = new UserDB(this.options);
  this.locker = new Lock();
  this.server = tcp.createServer();
  this.jobMap = Object.create(null);
  this.banned = Object.create(null);
  this.jobs = new List();
  this.current = null;
  this.inbound = new List();
  this.lastActive = 0;
  this.subscribed = false;
  this.uid = 0;

  this._init();
}

util.inherits(Stratum, AsyncObject);

Stratum.ACTIVE_TIME = 60;
Stratum.MAX_JOBS = 6;
Stratum.SHARES_PER_MINUTE = 8;
Stratum.BAN_SCORE = 1000;
Stratum.BAN_TIME = 10 * 60;

Stratum.prototype.sid = function sid() {
  var sid = this.uid;
  this.uid += 1;
  this.uid >>>= 0;
  return sid;
};

Stratum.prototype.jid = function jid() {
  var id = this.uid;
  this.uid += 1;
  this.uid >>>= 0;
  return util.now() + ':' + id;
};

Stratum.prototype._init = function _init() {
  var self = this;

  this.server.on('connection', function(socket) {
    self.handleSocket(socket);
  });

  this.node.on('connect', co(function* () {
    try {
      yield self.handleBlock();
    } catch (e) {
      self.emit('error', e);
    }
  }));

  this.node.on('tx', co(function* () {
    try {
      yield self.handleTX();
    } catch (e) {
      self.emit('error', e);
    }
  }));
};

Stratum.prototype.handleBlock = co(function* handleBlock() {
  var unlock = yield this.locker.lock();
  try {
    return yield this._handleBlock();
  } finally {
    unlock();
  }
});

Stratum.prototype._handleBlock = co(function* handleBlock() {
  var now = util.now();

  if (!this.subscribed) {
    this.lastActive = now;
    return;
  }

  this.current = null;
  this.lastActive = now;

  yield this.notifyAll();
});

Stratum.prototype.handleTX = co(function* handleTX() {
  var unlock = yield this.locker.lock();
  try {
    return yield this._handleTX();
  } finally {
    unlock();
  }
});

Stratum.prototype._handleTX = co(function* handleTX() {
  var now = util.now();

  if (!this.subscribed) {
    this.lastActive = now;
    return;
  }

  if (now > this.lastActive + Stratum.ACTIVE_TIME) {
    this.current = null;
    this.lastActive = now;

    yield this.notifyAll();
  }
});

Stratum.prototype.handleSocket = co(function* handleSocket(socket) {
  var host;

  if (!socket.remoteAddress) {
    this.logger.debug('Stratum: ignoring disconnected client.');
    socket.destroy();
    return;
  }

  host = IP.normalize(socket.remoteAddress);

  if (this.inbound.size >= this.options.maxInbound) {
    this.logger.debug(
      'Stratum: ignoring client: too many inbound (%s).',
      host);
    socket.destroy();
    return;
  }

  if (this.isBanned(host)) {
    this.logger.debug(
      'Stratum: ignoring banned client (%s).',
      host);
    socket.destroy();
    return;
  }

  socket.setKeepAlive(true);

  this.addClient(socket);
});

Stratum.prototype.addClient = function addClient(socket) {
  var self = this;
  var conn = new Connection(this, socket);

  conn.on('error', function(err) {
    self.emit('error', err);
  });

  conn.on('close', function() {
    assert(self.inbound.remove(conn));
  });

  conn.on('ban', function() {
    self.handleBan(conn);
  });

  this.inbound.push(conn);
};

Stratum.prototype.handleBan = function handleBan(conn) {
  this.logger.warning('Stratum: banning client (%s).', conn.id());
  this.banned[conn.host] = util.now();
  conn.destroy();
};

Stratum.prototype.isBanned = function isBanned(host) {
  var time = this.banned[host];

  if (time == null)
    return false;

  if (util.now() - time > Stratum.BAN_TIME) {
    delete this.banned[host];
    return false;
  }

  return true;
};

Stratum.prototype.listen = co(function* listen() {
  yield this.server.listen(this.options.port, this.options.host);
  this.logger.info('Stratum: server listening on %d.', this.options.port);
});

Stratum.prototype._open = co(function* _open() {
  yield this.userdb.open();
  yield this.sharedb.open();
  yield this.listen();

  if (!this.userdb.get('bitcoinrpc')) {
    var addr = Address('mi5PANCf3rZn6BWDHbCDHc2CRfgTnqRPwB');
    addr.network = this.network;
    this.userdb.add('bitcoinrpc', 'foo', addr);
  }

  this.lastActive = util.now();
});

Stratum.prototype._close = co(function* close() {
  var conn, next;

  for (conn = this.inbound.head; conn; conn = next) {
    next = conn.next;
    conn.destroy();
  }

  yield this.server.close();
  yield this.userdb.close();
  yield this.sharedb.close();
});

Stratum.prototype.notifyAll = co(function* notifyAll() {
  var job = yield this.getJob();
  var conn;

  this.logger.debug(
    'Stratum: notifying all clients of new job: %s.',
    job.id);

  for (conn = this.inbound.head; conn; conn = conn.next) {
    if (conn.sid === -1)
      continue;

    conn.sendJob(job);
  }
});

Stratum.prototype.createBlock = co(function* createBlock() {
  var attempt = yield this.node.miner.createBlock();
  return attempt.block;
});

Stratum.prototype.addJob = function addJob(job) {
  if (this.jobs.size >= Stratum.MAX_JOBS)
    this.removeJob(this.jobs.head);

  assert(this.jobs.push(job));

  assert(!this.jobMap[job.id]);
  this.jobMap[job.id] = job;

  this.current = job;
};

Stratum.prototype.removeJob = function removeJob(job) {
  assert(this.jobs.remove(job));

  assert(this.jobMap[job.id]);
  delete this.jobMap[job.id];

  if (job === this.current)
    this.current = null;
};

Stratum.prototype.getJob = co(function* getJob() {
  var block, job;

  if (!this.current) {
    block = yield this.createBlock();
    job = Job.fromBlock(this.jid(), block);

    this.addJob(job);

    this.logger.debug(
      'Stratum: new job (id=%s, prev=%s).',
      job.id, util.revHex(job.block.prevBlock));
  }

  return this.current;
});

Stratum.prototype.tryCommit = co(function* tryCommit(entry, block) {
  try {
    yield this.sharedb.commit(entry, block);
  } catch (e) {
    this.emit('error', e);
  }
});

Stratum.prototype.auth = function auth(username, password) {
  var user = this.userdb.get(username);
  var passwd, hash;

  if (!user)
    return false;

  passwd = new Buffer(password, 'utf8');
  hash = crypto.hash256(passwd);

  if (!crypto.ccmp(hash, user.password))
    return false;

  return true;
};

Stratum.prototype.authAdmin = function authAdmin(password) {
  var data, hash;

  if (!this.options.password)
    return false;

  data = new Buffer(password, 'utf8');
  hash = crypto.hash256(data);

  if (!crypto.ccmp(hash, this.options.password))
    return false;

  return true;
};

Stratum.prototype.addBlock = co(function* addBlock(conn, block) {
  var entry;

  // Broadcast immediately.
  this.node.broadcast(block);

  try {
    entry = yield this.chain.add(block);
  } catch (e) {
    if (e.type === 'VerifyError') {
      switch (e.reason) {
        case 'high-hash':
          return new StratumError(23, 'high-hash');
        case 'duplicate':
          return new StratumError(22, 'duplicate');
        case 'bad-prevblk':
          return new StratumError(21, 'stale-prevblk');
      }
      return new StratumError(20, e.reason);
    }
    throw e;
  }

  if (entry.hash !== this.chain.tip.hash)
    return new StratumError(21, 'stale-work');

  this.tryCommit(entry, block);

  this.logger.info('Stratum: Client found block %s (%d) (%s).',
    entry.rhash(),
    entry.height,
    conn.id());

  return null;
});

Stratum.prototype.handlePacket = co(function* handlePacket(conn, msg) {
  var unlock = yield this.locker.lock();
  try {
    return yield this._handlePacket(conn, msg);
  } finally {
    unlock();
  }
});

Stratum.prototype._handlePacket = co(function* handlePacket(conn, msg) {
  switch (msg.method) {
    case 'mining.authorize':
      return yield this.handleAuthorize(conn, msg);
    case 'mining.subscribe':
      return yield this.handleSubscribe(conn, msg);
    case 'mining.submit':
      return yield this.handleSubmit(conn, msg);
    case 'mining.get_transactions':
      return yield this.handleTransactions(conn, msg);
    case 'mining.authorize_admin':
      return yield this.handleAuthAdmin(conn, msg);
    case 'mining.add_user':
      return yield this.handleAddUser(conn, msg);
    default:
      return yield this.handleUnknown(conn, msg);
  }
});

Stratum.prototype.handleAuthorize = co(function* handleAuthorize(conn, msg) {
  var user, pass;

  if (typeof msg.params.length < 2) {
    conn.sendError(msg, 0, 'invalid params');
    return;
  }

  user = msg.params[0];
  pass = msg.params[1];

  if (!isUsername(user) || !isPassword(pass)) {
    conn.sendError(msg, 0, 'invalid params');
    return;
  }

  if (!this.auth(user, pass)) {
    this.logger.debug(
      'Stratum: client failed auth for user %s (%s).',
      user, conn.id());
    conn.sendResponse(msg, false);
    return;
  }

  this.logger.debug(
    'Stratum: client successfully authd for %s (%s).',
    user, conn.id());

  conn.addUser(user);
  conn.sendResponse(msg, true);
});

Stratum.prototype.handleSubscribe = co(function* handleSubscribe(conn, msg) {
  var sid, job;

  if (!this.chain.synced) {
    conn.sendError(msg, 0, 'not up to date');
    return;
  }

  if (!conn.agent && msg.params.length > 0) {
    if (!isAgent(msg.params[0])) {
      conn.sendError(msg, 0, 'invalid params');
      return;
    }
    conn.agent = msg.params[0];
  }

  if (msg.params.length > 1) {
    if (!isSID(msg.params[1])) {
      conn.sendError(msg, 0, 'invalid params');
      return;
    }
    conn.sid = this.sid();
  } else {
    conn.sid = this.sid();
  }

  if (!this.subscribed) {
    this.logger.debug('Stratum: first subscriber (%s).', conn.id());
    this.subscribed = true;
  }

  sid = util.hex32(conn.sid);
  job = yield this.getJob();

  this.logger.debug(
    'Stratum: client is subscribing with sid=%s (%s).',
    sid, conn.id());

  conn.sendResponse(msg, [
    [
      ['mining.notify', sid],
      ['mining.set_difficulty', sid]
    ],
    sid,
    NONCE_SIZE
  ]);

  conn.setDifficulty(this.difficulty);
  conn.sendJob(job);
});

Stratum.prototype.handleSubmit = co(function* handleSubmit(conn, msg) {
  var now = this.network.now();
  var subm, job, share, block, error;

  try {
    subm = Submission.fromPacket(msg);
  } catch (e) {
    conn.sendError(msg, 0, 'invalid params');
    return;
  }

  this.logger.spam(
    'Stratum: client submitted job %s (%s).',
    subm.job, conn.id());

  if (!conn.hasUser(subm.username)) {
    conn.sendError(msg, 24, 'unauthorized user');
    return;
  }

  if (conn.sid === -1) {
    conn.sendError(msg, 25, 'not subscribed');
    return;
  }

  job = this.jobMap[subm.job];

  if (!job || job.committed) {
    conn.sendError(msg, 21, 'job not found');
    return;
  }

  if (job !== this.current) {
    this.logger.warning(
      'Stratum: client is submitting a stale job %s (%s).',
      job.id, conn.id());
  }

  // Non-consensus sanity check.
  // 2 hours should be less than MTP in 99% of cases.
  if (subm.ts < now - 7200) {
    conn.sendError(msg, 20, 'time too old');
    return;
  }

  if (subm.ts > now + 7200) {
    conn.sendError(msg, 20, 'time too new');
    return;
  }

  share = job.check(conn.sid, subm);

  if (share.difficulty < conn.difficulty - 1) {
    this.logger.debug(
      'Stratum: client submitted a low share of %d, hash=%s, ban=%d (%s).',
      share.difficulty, share.rhash(), conn.banScore, conn.id());

    conn.increaseBan(1);
    conn.sendError(msg, 23, 'high-hash');
    conn.sendDifficulty(conn.difficulty);

    return;
  }

  if (!job.insert(share.hash)) {
    this.logger.debug(
      'Stratum: client submitted a duplicate share: %s (%s).',
      share.rhash(), conn.id());
    conn.increaseBan(10);
    conn.sendError(msg, 22, 'duplicate');
    return;
  }

  this.sharedb.add(subm.username, share.difficulty);

  this.logger.debug(
    'Stratum: client submitted share of %d, hash=%s (%s).',
    share.difficulty, share.rhash(), conn.id());

  if (share.verify(job.target)) {
    block = job.commit(share);
    error = yield this.addBlock(conn, block);
  }

  if (error) {
    this.logger.warning(
      'Stratum: Client found an invalid block: %s (%s).',
      error.reason, conn.id());
    conn.sendError(msg, error.code, error.reason);
  } else {
    conn.sendResponse(msg, true);
  }

  if (this.options.dynamic) {
    if (conn.retarget(job.difficulty)) {
      this.logger.debug(
        'Stratum: retargeted client to %d (%s).',
        conn.difficulty, conn.id());
    }
  }
});

Stratum.prototype.handleTransactions = co(function* handleTransactions(conn, msg) {
  var result = [];
  var i, id, tx, job, block;

  if (conn.sid === -1) {
    conn.sendError(msg, 25, 'not subscribed');
    return;
  }

  if (msg.params.length < 1) {
    conn.sendError(msg, 21, 'job not found');
    return;
  }

  id = msg.params[0];

  if (!isJob(id)) {
    conn.sendError(msg, 21, 'job not found');
    return;
  }

  job = this.jobMap[id];

  if (!job || job.committed) {
    conn.sendError(msg, 21, 'job not found');
    return;
  }

  this.logger.debug(
    'Stratum: sending tx list (%s).',
    conn.id());

  block = job.block;

  for (i = 1; i < block.txs.length; i++) {
    tx = block.txs[i];
    result.push(tx.hash('hex'));
  }

  conn.sendResponse(msg, result);
});

Stratum.prototype.handleAuthAdmin = co(function* handleAuthAdmin(conn, msg) {
  var password;

  if (typeof msg.params.length < 1) {
    conn.sendError(msg, 0, 'invalid params');
    return;
  }

  password = msg.params[0];

  if (!isPassword(password)) {
    conn.sendError(msg, 0, 'invalid params');
    return;
  }

  if (!this.authAdmin(password)) {
    this.logger.debug(
      'Stratum: client sent bad admin password (%s).',
      conn.id());
    conn.increaseBan(10);
    conn.sendError(msg, 0, 'invalid password');
    return;
  }

  conn.admin = true;
  conn.sendResponse(msg, true);
});

Stratum.prototype.handleAddUser = co(function* handleAddUser(conn, msg) {
  var user, pass, addr;

  if (typeof msg.params.length < 3) {
    conn.sendError(msg, 0, 'invalid params');
    return;
  }

  user = msg.params[0];
  pass = msg.params[1];
  addr = msg.params[2];

  if (!isUsername(user)
      || !isPassword(pass)
      || !isAddress(addr)) {
    conn.sendError(msg, 0, 'invalid params');
    return;
  }

  if (!conn.admin) {
    this.logger.debug(
      'Stratum: client is not an admin (%s).',
      conn.id());
    conn.sendError(msg, 0, 'invalid password');
    return;
  }

  try {
    this.userdb.add(user, pass, addr);
  } catch (e) {
    conn.sendError(msg, 0, e.message);
    return;
  }

  conn.sendResponse(msg, true);
});

Stratum.prototype.handleUnknown = co(function* handleUnknown(conn, msg) {
  this.logger.debug(
    'Stratum: client sent an unknown message (%s):',
    conn.id());

  this.logger.debug(msg);

  conn.send({
    id: msg.id,
    result: null,
    error: true
  });
});

/**
 * Stratum Options
 * @constructor
 * @param {Object} options
 */

function StratumOptions(options) {
  if (!(this instanceof StratumOptions))
    return new StratumOptions(options);

  this.node = null;
  this.chain = null;
  this.logger = Logger.global;
  this.network = Network.primary;
  this.host = '0.0.0.0';
  this.port = 3008;
  this.publicHost = '127.0.0.1';
  this.publicPort = 3008;
  this.maxInbound = 50;
  this.difficulty = 8;
  this.dynamic = false;
  this.prefix = util.HOME + '/.bcoin/stratum';
  this.password = null;

  this.fromOptions(options);
}

StratumOptions.prototype.fromOptions = function fromOptions(options) {
  assert(options, 'Options are required.');
  assert(options.node && typeof options.node === 'object',
    'Node is required.');

  this.node = options.node;
  this.chain = this.node.chain;
  this.network = this.node.network;
  this.logger = this.node.logger;
  this.prefix = this.node.location('stratum');

  if (options.host != null) {
    assert(typeof options.host === 'string');
    this.host = options.host;
  }

  if (options.port != null) {
    assert(typeof options.port === 'number');
    this.port = options.port;
  }

  if (options.publicHost != null) {
    assert(typeof options.publicHost === 'string');
    this.publicHost = options.publicHost;
  }

  if (options.publicPort != null) {
    assert(typeof options.publicPort === 'number');
    this.publicPort = options.publicPort;
  }

  if (options.maxInbound != null) {
    assert(typeof options.maxInbound === 'number');
    this.maxInbound = options.maxInbound;
  }

  if (options.difficulty != null) {
    assert(typeof options.difficulty === 'number');
    this.difficulty = options.difficulty;
  }

  if (options.dynamic != null) {
    assert(typeof options.dynamic === 'boolean');
    this.dynamic = options.dynamic;
  }

  if (options.password != null) {
    assert(isPassword(options.password));
    this.password = crypto.hash256(new Buffer(options.password, 'utf8'));
  }

  return this;
};

StratumOptions.fromOptions = function fromOptions(options) {
  return new StratumOptions().fromOptions(options);
};

/**
 * Stratum Connection
 * @constructor
 * @param {Stratum} stratum
 * @param {net.Socket} socket
 */

function Connection(stratum, socket) {
  if (!(this instanceof Connection))
    return new Connection(stratum, socket);

  EventEmitter.call(this);

  this.locker = new Lock();
  this.stratum = stratum;
  this.logger = stratum.logger;
  this.socket = socket;
  this.host = IP.normalize(socket.remoteAddress);
  this.port = socket.remotePort;
  this.hostname = IP.toHostname(this.host, this.port);
  this.decoder = new StringDecoder('utf8');
  this.agent = '';
  this.recv = '';
  this.admin = false;
  this.users = Object.create(null);
  this.sid = -1;
  this.difficulty = -1;
  this.banScore = 0;
  this.lastBan = 0;
  this.drainSize = 0;
  this.destroyed = false;
  this.lastRetarget = -1;
  this.submissions = 0;
  this.prev = null;
  this.next = null;

  this._init();
}

util.inherits(Connection, EventEmitter);

Connection.prototype.retarget = function retarget(max) {
  var now = util.ms();
  var difficulty, target, actual;

  assert(this.difficulty > 0);

  if (this.lastRetarget === -1) {
    assert(this.submissions === 0);
    this.lastRetarget = now;
    this.submissions += 1;
    return false;
  }

  this.submissions += 1;

  if (this.submissions >= Stratum.SHARES_PER_MINUTE) {
    target = 60000;
    actual = now - this.lastRetarget;
    difficulty = 0x100000000 / this.difficulty;
    max = Math.min(0xffffffff, max);

    if (Math.abs(target - actual) <= 10000) {
      this.lastRetarget = now;
      this.submissions = 0;
      return false;
    }

    if (actual < target / 4)
      actual = target / 4;

    if (actual > target * 4)
      actual = target * 4;

    difficulty *= actual;
    difficulty /= target;
    difficulty = 0x100000000 / difficulty;
    difficulty >>>= 0;
    difficulty = Math.min(max, difficulty);
    difficulty = Math.max(1, difficulty);

    this.lastRetarget = now;
    this.submissions = 0;

    if (Math.abs(this.difficulty - difficulty) >= 10) {
      this.setDifficulty(difficulty);
      return true;
    }
  }

  return false;
};

Connection.prototype._init = function _init() {
  var self = this;

  this.on('packet', co(function* (msg) {
    try {
      yield self.readPacket(msg);
    } catch (e) {
      self.error(e);
    }
  }));

  this.socket.on('data', function(data) {
    self.feed(data);
  });

  this.socket.on('error', function(err) {
    self.emit('error', err);
  });

  this.socket.on('close', function() {
    self.error('Socket hangup.');
    self.destroy();
  });

  this.socket.on('drain', function() {
    self.drainSize = 0;
  });
};

Connection.prototype.destroy = function destroy() {
  if (this.destroyed)
    return;

  this.destroyed = true;

  this.locker.destroy();
  this.socket.destroy();
  this.socket = null;

  this.emit('close');
};

Connection.prototype.send = function send(json) {
  if (this.destroyed)
    return;

  json = JSON.stringify(json);
  json += '\n';

  this.write(json);
};

Connection.prototype.write = function write(text) {
  if (this.destroyed)
    return;

  if (this.socket.write(text, 'utf8') === false) {
    this.drainSize += Buffer.byteLength(text, 'utf8');
    if (this.drainSize > (5 << 20)) {
      this.logger.warning(
        'Stratum: client is not reading (%s).',
        this.id());
      this.destroy();
    }
  }
};

Connection.prototype.error = function error(err) {
  var msg;

  if (this.destroyed)
    return;

  if (err instanceof Error) {
    err.message = 'Stratum: ' + err.message;
    err.message += ' (' + this.id() + ')';
    this.emit('error', err);
    return;
  }

  msg = util.fmt.apply(util, arguments);

  msg = 'Stratum: ' + msg;
  msg += ' (' + this.id() + ')';

  this.emit('error', new Error(msg));
};

Connection.prototype.redirect = function redirect() {
  var host = this.stratum.options.publicHost;
  var port = this.stratum.options.publicPort;
  var res;

  res = [
    'HTTP/1.1 200 OK',
    'X-Stratum: stratum+tcp://' + host + ':' + port,
    'Connection: Close',
    'Content-Type: application/json; charset=utf-8',
    'Content-Length: 37',
    '',
    '',
    '{"error":null,"result":false,"id":0}'
  ];

  this.write(res.join('\r\n'));

  this.logger.debug('Stratum: redirecting client (%s).', this.id());

  this.destroy();
};

Connection.prototype.feed = function feed(data) {
  var i, line, lines, msg;

  this.recv += this.decoder.write(data);

  if (this.recv.length >= 100000) {
    this.error('Stratum: too much data buffered (%s).', this.id());
    this.destroy();
    return;
  }

  if (/HTTP\/1\.1/i.test(this.recv)) {
    this.redirect();
    return;
  }

  lines = this.recv.replace(/\r+/g, '').split(/\n+/);

  this.recv = lines.pop();

  for (i = 0; i < lines.length; i++) {
    line = lines[i];

    if (line.length === 0)
      continue;

    try {
      msg = ClientPacket.fromRaw(line);
    } catch (e) {
      this.error(e);
      continue;
    }

    this.emit('packet', msg);
  }
};

Connection.prototype.readPacket = co(function* readPacket(msg) {
  var unlock = yield this.locker.lock();
  try {
    this.socket.pause();
    yield this.handlePacket(msg);
  } finally {
    if (!this.destroyed)
      this.socket.resume();
    unlock();
  }
});

Connection.prototype.handlePacket = co(function* handlePacket(msg) {
  return yield this.stratum.handlePacket(this, msg);
});

Connection.prototype.addUser = function addUser(username) {
  if (this.users[username])
    return false;

  this.users[username] = true;

  return true;
};

Connection.prototype.hasUser = function hasUser(username) {
  return this.users[username] != null;
};

Connection.prototype.increaseBan = function increaseBan(score) {
  var now = util.ms();

  this.banScore *= Math.pow(1 - 1 / 60000, now - this.lastBan);
  this.banScore += score;
  this.lastBan = now;

  if (this.banScore >= Stratum.BAN_SCORE) {
    this.logger.warning('Stratum: banning client (%s).', this.id());
    this.ban();
  }
};

Connection.prototype.ban = function ban() {
  this.emit('ban');
};

Connection.prototype.sendError = function sendError(msg, code, reason) {
  this.logger.spam(
    'Stratum: sending error %s (%s).',
    reason, this.id());

  this.send({
    id: msg.id,
    result: null,
    error: [code, reason, false]
  });
};

Connection.prototype.sendResponse = function sendResponse(msg, result) {
  this.logger.spam(
    'Stratum: sending response %s (%s).',
    msg.id, this.id());

  this.send({
    id: msg.id,
    result: result,
    error: null
  });
};

Connection.prototype.sendMethod = function sendMethod(method, params) {
  this.logger.spam(
    'Stratum: sending method %s (%s).',
    method, this.id());

  this.send({
    id: null,
    method: method,
    params: params
  });
};

Connection.prototype.sendDifficulty = function sendDifficulty(difficulty) {
  assert(difficulty > 0, 'Difficulty must be at least 1.');

  this.logger.debug(
    'Stratum: setting difficulty=%d for client (%s).',
    difficulty, this.id());

  this.sendMethod('mining.set_difficulty', [difficulty]);
};

Connection.prototype.setDifficulty = function setDifficulty(difficulty) {
  if (this.difficulty === difficulty)
    return;

  this.difficulty = difficulty;
  this.sendDifficulty(difficulty);
};

Connection.prototype.sendJob = function sendJob(job) {
  this.logger.debug(
    'Stratum: sending job %s to client (%s).',
    job.id, this.id());

  this.sendMethod('mining.notify', job.toJSON());
};

Connection.prototype.id = function id() {
  var id = this.host;

  if (this.agent)
    id += '/' + this.agent;

  return id;
};

/**
 * User
 * @constructor
 * @param {Object} options
 */

function User(options) {
  if (!(this instanceof User))
    return new User(options);

  this.username = '';
  this.password = encoding.ZERO_HASH;
  this.address = '';

  if (options)
    this.fromOptions(options);
}

User.prototype.fromOptions = function fromOptions(options) {
  assert(options, 'Options required.');
  assert(isUsername(options.username), 'Username required.');
  assert(options.hash || options.password, 'Password required.');
  assert(options.address, 'Address required.');

  this.setUsername(options.username);

  if (options.hash != null)
    this.setHash(options.hash);

  if (options.password != null)
    this.setPassword(options.password);

  if (options.address != null)
    this.setAddress(options.address, options.network);

  return this;
};

User.fromOptions = function fromOptions(options) {
  return new User().fromOptions(options);
};

User.prototype.setUsername = function setUsername(username) {
  assert(isUsername(username), 'Username must be a string.');
  this.username = username;
};

User.prototype.setHash = function setHash(hash) {
  if (typeof hash === 'string') {
    assert(util.isHex(hash), 'Hash must be a hex string.');
    assert(hash.length === 64, 'Hash must be 32 bytes.');
    this.password = new Buffer(hash, 'hex');
  } else {
    assert(Buffer.isBuffer(hash), 'Hash must be a buffer.');
    assert(hash.length === 32, 'Hash must be 32 bytes.');
    this.password = hash;
  }
};

User.prototype.setPassword = function setPassword(password) {
  assert(isPassword(password), 'Password must be a string.');
  password = new Buffer(password, 'utf8');
  this.password = crypto.hash256(password);
};

User.prototype.setAddress = function setAddress(address, network) {
  var addr;

  assert(address, 'Address must be a string.');

  network = Network.get(network);
  addr = Address.fromOptions(address);

  assert(addr.verifyNetwork(network), 'Wrong network for address.');

  this.address = addr.toBase58(network);
};

User.prototype.toJSON = function toJSON() {
  return {
    username: this.username,
    password: this.password.toString('hex'),
    address: this.address
  };
};

User.prototype.fromJSON = function fromJSON(json, network) {
  assert(json);
  assert(typeof json.username === 'string');
  this.username = json.username;
  this.setHash(json.password);
  this.setAddress(json.address, network);
  return this;
};

User.fromJSON = function fromJSON(json, network) {
  return new User().fromJSON(json, network);
};

/**
 * ClientPacket
 */

function ClientPacket() {
  if (!(this instanceof ClientPacket))
    return new ClientPacket();

  this.id = null;
  this.method = 'unknown';
  this.params = [];
}

ClientPacket.fromRaw = function fromRaw(json) {
  var packet = new ClientPacket();
  var msg = JSON.parse(json);

  if (msg.id != null) {
    assert(typeof msg.id === 'string'
      || typeof msg.id === 'number');
    packet.id = msg.id;
  }

  assert(typeof msg.method === 'string');
  assert(msg.method.length <= 50);
  packet.method = msg.method;

  if (msg.params) {
    assert(Array.isArray(msg.params));
    packet.params = msg.params;
  }

  return packet;
};

/**
 * Submission Packet
 */

function Submission() {
  if (!(this instanceof Submission))
    return new Submission();

  this.username = '';
  this.job = '';
  this.nonce2 = 0;
  this.ts = 0;
  this.nonce = 0;
}

Submission.fromPacket = function fromPacket(msg) {
  var subm = new Submission();

  assert(msg.params.length >= 5, 'Invalid parameters.');

  assert(isUsername(msg.params[0]), 'Name must be a string.');
  assert(isJob(msg.params[1]), 'Job ID must be a string.');

  assert(typeof msg.params[2] === 'string', 'Nonce 2 must be a string.');
  assert(msg.params[2].length === NONCE_SIZE * 2, 'Nonce 2 must be a string.');
  assert(util.isHex(msg.params[2]), 'Nonce 2 must be a string.');

  assert(typeof msg.params[3] === 'string', 'Time must be a string.');
  assert(msg.params[3].length === 8, 'Time must be a string.');
  assert(util.isHex(msg.params[3]), 'Time must be a string.');

  assert(typeof msg.params[4] === 'string', 'Nonce must be a string.');
  assert(util.isHex(msg.params[4]), 'Nonce must be a string.');
  assert(msg.params[4].length === 8, 'Nonce must be a string.');

  subm.username = msg.params[0];
  subm.job = msg.params[1];
  subm.nonce2 = parseInt(msg.params[2], 16);
  subm.ts = parseInt(msg.params[3], 16);
  subm.nonce = parseInt(msg.params[4], 16);

  return subm;
};

/**
 * Job
 * @constructor
 */

function Job(id) {
  if (!(this instanceof Job))
    return new Job(id);

  assert(typeof id === 'string');

  this.id = id;
  this.block = new Block();
  this.tree = new MerkleTree();
  this.left = DUMMY;
  this.right = DUMMY;
  this.target = encoding.ZERO_HASH;
  this.difficulty = 0;
  this.submissions = {};
  this.committed = false;
  this.prev = null;
  this.next = null;
}

Job.prototype.fromBlock = function fromBlock(block) {
  var target = consensus.fromCompact(block.bits);
  var parts;

  this.block = block;
  this.tree.fromBlock(block);
  this.target = target.toArrayLike(Buffer, 'le', 32);
  this.difficulty = getDifficulty(target);

  parts = this.getParts();

  this.left = parts.left;
  this.right = parts.right;

  return this;
};

Job.fromBlock = function fromBlock(id, block) {
  return new Job(id).fromBlock(block);
};

Job.prototype.insert = function insert(hash) {
  hash = hash.toString('hex');

  if (this.submissions[hash])
    return false;

  this.submissions[hash] = true;

  return true;
};

Job.prototype.getParts = function getParts() {
  var cb = this.block.txs[0];
  var raw = cb.toNormal();
  var size = 0;
  var left, right;

  size += 4; // version
  size += 1; // varint inputs length
  size += cb.inputs[0].getSize(); // input size
  size -= NONCE_SIZE * 2 + 4; // -(extranonce + sequence)

  // Cut off right after the nonce
  // push and before the sequence.
  left = raw.slice(0, size);

  // Include the sequence.
  size += NONCE_SIZE * 2; // extranonce
  right = raw.slice(size);

  return new Parts(left, right);
};

Job.prototype.getNonce = function getNonce(nonce1, nonce2) {
  var bw = new StaticWriter(NONCE_SIZE * 2);
  bw.writeU32BE(nonce1);
  bw.writeU32BE(nonce2);
  return bw.render();
};

Job.prototype.getCoinbase = function getCoinbase(nonce1, nonce2) {
  var size = 0;
  var bw;

  size += this.left.length;
  size += NONCE_SIZE * 2;
  size += this.right.length;

  bw = new StaticWriter(size);
  bw.writeBytes(this.left);
  bw.writeU32BE(nonce1);
  bw.writeU32BE(nonce2);
  bw.writeBytes(this.right);

  return bw.render();
};

Job.prototype.getRoot = function getRoot(nonce1, nonce2) {
  var raw = this.getCoinbase(nonce1, nonce2);
  var hash = crypto.hash256(raw);
  return this.tree.withFirst(hash);
};

Job.prototype.hash = function hash(nonce1, nonce2, ts, nonce) {
  var bw = new StaticWriter(80);
  var root = this.getRoot(nonce1, nonce2);
  var hash;

  bw.writeU32(this.block.version);
  bw.writeHash(this.block.prevBlock);
  bw.writeHash(root);
  bw.writeU32(ts);
  bw.writeU32(this.block.bits);
  bw.writeU32(nonce);

  hash = crypto.hash256(bw.render());

  return new Share(hash, root, nonce1, nonce2, ts, nonce);
};

Job.prototype.check = function check(nonce1, subm) {
  var nonce2 = subm.nonce2;
  var ts = subm.ts;
  var nonce = subm.nonce;
  return this.hash(nonce1, nonce2, ts, nonce);
};

Job.prototype.commit = function commit(share) {
  var block = this.block;
  var cb = block.txs[0];
  var input = cb.inputs[0];
  var extra = this.getNonce(share.nonce1, share.nonce2);

  assert(!this.committed, 'Already committed.');

  input.script.set(3, extra);
  input.script.compile();

  block.merkleRoot = share.root.toString('hex');
  block.nonce = share.nonce;
  block.ts = share.ts;

  cb.mutable = false;
  block.mutable = false;

  this.committed = true;

  return block;
};

Job.prototype.toJSON = function toJSON() {
  return [
    this.id,
    swap32(this.block.prevBlock),
    this.left.toString('hex'),
    this.right.toString('hex'),
    this.tree.toJSON(),
    util.hex32(this.block.version),
    util.hex32(this.block.bits),
    util.hex32(this.block.ts),
    false
  ];
};

/*
 * Coinbase Parts
 * @constructor
 * @param {Buffer} left
 * @param {Buffer} right
 */

function Parts(left, right) {
  this.left = left;
  this.right = right;
}

/*
 * Share
 * @constructor
 * @param {Hash} hash
 * @param {Hash} root
 * @param {Number} nonce2
 * @param {Number} ts
 * @param {Number} nonce
 */

function Share(hash, root, nonce1, nonce2, ts, nonce) {
  this.hash = hash;
  this.root = root;
  this.nonce1 = nonce1;
  this.nonce2 = nonce2;
  this.ts = ts;
  this.nonce = nonce;
  this.difficulty = fromTarget(hash);
}

Share.prototype.rhash = function rhash() {
  return util.revHex(this.hash.toString('hex'));
};

Share.prototype.verify = function verify(target) {
  return rcmp(this.hash, target) <= 0;
};

/*
 * MerkleTree
 * @constructor
 * @property {Hash[]} steps
 */

function MerkleTree() {
  this.steps = [];
}

MerkleTree.prototype.withFirst = function withFirst(hash) {
  var i, step, data;

  for (i = 0; i < this.steps.length; i++) {
    step = this.steps[i];
    data = util.concat(hash, step);
    hash = crypto.hash256(data);
  }

  return hash;
};

MerkleTree.prototype.toJSON = function toJSON() {
  var steps = [];
  var i, step;

  for (i = 0; i < this.steps.length; i++) {
    step = this.steps[i];
    steps.push(step.toString('hex'));
  }

  return steps;
};

MerkleTree.prototype.fromBlock = function fromBlock(block) {
  var leaves = [];
  var i, tx;

  for (i = 0; i < block.txs.length; i++) {
    tx = block.txs[i];
    leaves.push(tx.hash());
  }

  return this.fromLeaves(leaves);
};

MerkleTree.fromBlock = function fromBlock(block) {
  return new MerkleTree().fromBlock(block);
};

MerkleTree.prototype.fromLeaves = function fromLeaves(leaves) {
  var len = leaves.length;
  var i, hashes, buf, hash;

  while (len > 1) {
    this.steps.push(leaves[1]);

    if (len % 2)
      leaves.push(leaves[len - 1]);

    hashes = [null];

    for (i = 2; i < len; i += 2) {
      buf = util.concat(leaves[i], leaves[i + 1]);
      hash = crypto.hash256(buf);
      hashes.push(hash);
    }

    leaves = hashes;
    len = leaves.length;
  }

  return this;
};

MerkleTree.fromLeaves = function fromLeaves(leaves) {
  return new MerkleTree().fromLeaves(leaves);
};

/**
 * Stratum Error
 * @constructor
 * @param {Number} code
 * @param {String} reason
 */

function StratumError(code, reason) {
  this.code = code;
  this.reason = reason;
}

/**
 * Share DB
 * @constructor
 * @param {Object} options
 */

function ShareDB(options) {
  if (!(this instanceof ShareDB))
    return new ShareDB(options);

  this.network = options.network;
  this.logger = options.logger;
  this.location = options.prefix + '/shares';
  this.ensured = false;

  this.map = Object.create(null);
  this.total = 0;
  this.size = 0;
}

ShareDB.prototype.open = co(function* open() {
  ;
});

ShareDB.prototype.close = co(function* close() {
  ;
});

ShareDB.prototype.file = function file(entry) {
  var name = entry.height + '-' + entry.rhash();
  return this.location + '/' + name + '.json';
};

ShareDB.prototype.add = function add(username, difficulty) {
  if (!this.map[username]) {
    this.map[username] = 0;
    this.size++;
  }

  this.map[username] += difficulty;
  this.total += difficulty;
};

ShareDB.prototype.clear = function clear() {
  this.map = Object.create(null);
  this.size = 0;
  this.total = 0;
};

ShareDB.prototype.commit = co(function* commit(entry, block) {
  var cb = block.txs[0];
  var addr = cb.outputs[0].getAddress();
  var data, json, file;

  assert(addr);

  data = {
    network: this.network.type,
    height: entry.height,
    block: block.rhash(),
    ts: block.ts,
    time: util.now(),
    txid: cb.txid(),
    address: addr.toBase58(this.network),
    reward: cb.getOutputValue(),
    size: this.size,
    total: this.total,
    shares: this.map
  };

  this.clear();

  if (!this.ensured) {
    util.mkdir(this.location);
    this.ensured = true;
  }

  file = this.file(entry);
  json = JSON.stringify(data, null, 2);

  this.logger.info(
    'Stratum: committing %d payouts to disk for block %d (file=%s).',
    data.size, entry.height, file);

  yield writeFile(file, json);
});

/**
 * User DB
 * @constructor
 * @param {Object} options
 */

function UserDB(options) {
  if (!(this instanceof UserDB))
    return new UserDB(options);

  this.network = options.network;
  this.logger = options.logger;
  this.location = options.prefix + '/users.json';
  this.locker = new Lock();
  this.ensured = false;
  this.lastFail = 0;
  this.stream = null;

  this.map = Object.create(null);
  this.size = 0;
}

UserDB.prototype.open = co(function* open() {
  var unlock = yield this.locker.lock();
  try {
    return yield this._open();
  } finally {
    unlock();
  }
});

UserDB.prototype._open = co(function* _open() {
  yield this.load();
});

UserDB.prototype.close = co(function* close() {
  var unlock = yield this.locker.lock();
  try {
    return yield this._close();
  } finally {
    unlock();
  }
});

UserDB.prototype._close = co(function* _close() {
  if (!this.stream)
    return;

  try {
    this.stream.close();
  } catch (e) {
    ;
  }

  this.stream = null;
});

UserDB.prototype.load = function load() {
  var self = this;
  return new Promise(function(resolve, reject) {
    self._load(resolve, reject);
  });
};

UserDB.prototype._load = function load(resolve, reject) {
  var self = this;
  var buf = '';
  var lineno = 0;
  var i, stream, lines, line, json, user;

  stream = fs.createReadStream(this.location, {
    flags: 'r',
    encoding: 'utf8',
    autoClose: true
  });

  function close() {
    if (!stream)
      return;

    try {
      stream.close();
    } catch (e) {
      ;
    }

    stream = null;
  }

  stream.on('error', function(e) {
    if (!stream)
      return;

    if (e.code === 'ENOENT') {
      close();
      resolve();
      return;
    }

    close();
    reject(e);
  });

  stream.on('data', function(data) {
    if (!stream)
      return;

    buf += data;

    if (buf.length >= 10000) {
      close();
      reject(new Error('UserDB parse error. Line: ' + lineno));
      return;
    }

    lines = buf.split(/\n+/);
    buf = lines.pop();

    for (i = 0; i < lines.length; i++) {
      line = lines[i];
      lineno++;

      if (line.length === 0)
        continue;

      try {
        json = JSON.parse(line);
        user = User.fromJSON(json, self.network);
      } catch (e) {
        close();
        reject(new Error('UserDB parse error. Line: ' + lineno));
        return;
      }

      if (!self.map[user.username])
        self.size++;

      self.map[user.username] = user;
    }
  });

  stream.on('end', function() {
    if (!stream)
      return;

    self.logger.debug(
      'Stratum: loaded %d users into memory.',
      self.size);

    stream = null;
    resolve();
  });
};

UserDB.prototype.get = function get(username) {
  return this.map[username];
};

UserDB.prototype.has = function has(username) {
  return this.map[username] != null;
};

UserDB.prototype.add = function add(username, password, address) {
  var user = this.map[username];

  assert(!user, 'User already exists.');

  user = new User({
    network: this.network,
    username: username,
    password: password,
    address: address
  });

  this.logger.debug(
    'Stratum: adding new user (%s:%s).',
    username, address);

  this.map[username] = user;
  this.size++;

  this.write(user.toJSON());
};

UserDB.prototype.setPassword = function setPassword(username, password) {
  var user = this.map[username];
  assert(user, 'User does not exist.');
  user.setPassword(password);
  this.write(user.toJSON());
};

UserDB.prototype.setAddress = function setAddress(username, address) {
  var user = this.map[username];
  assert(user, 'User does not exist.');
  user.setAddress(address);
  this.write(user.toJSON());
};

UserDB.prototype.write = function write(data) {
  var stream = this.getStream();
  var json;

  if (!stream)
    return;

  json = JSON.stringify(data) + '\n';
  stream.write(json, 'utf8');
};

UserDB.prototype.getStream = function getStream() {
  var self = this;

  if (this.stream)
    return this.stream;

  if (this.lastFail > util.now() - 10)
    return;

  this.lastFail = 0;

  if (!this.ensured) {
    try {
      util.mkdir(this.location, true);
    } catch (e) {
      this.logger.warning('Stratum: could not create userdb directory.');
      this.logger.error(e);
      this.lastFail = util.now();
      return;
    }
    this.ensured = true;
  }

  this.stream = fs.createWriteStream(this.location, { flags: 'a' });

  this.stream.on('error', function(err) {
    self.logger.warning('Stratum: UserDB file stream died!');
    self.logger.error(err);

    try {
      self.stream.close();
    } catch (e) {
      ;
    }

    // Retry in ten seconds.
    self.stream = null;
    self.lastFail = util.now();
  });

  return this.stream;
};

/*
 * Helpers
 */

function writeFile(file, data) {
  return new Promise(function(resolve, reject) {
    if (fs.unsupported) {
      resolve();
      return;
    }
    fs.writeFile(file, data, 'utf8', co.wrap(resolve, reject));
  });
}

function swap32(hex) {
  var data = new Buffer(hex, 'hex');
  var i, field;

  for (i = 0; i < data.length; i += 4) {
    field = data.readUInt32LE(i, true);
    data.writeUInt32BE(field, i, true);
  }

  return data.toString('hex');
}

function rcmp(a, b) {
  var i;

  assert(a.length === b.length);

  for (i = a.length - 1; i >= 0; i--) {
    if (a[i] < b[i])
      return -1;
    if (a[i] > b[i])
      return 1;
  }

  return 0;
}

function fromTarget(data) {
  var target = new BN(data, 'le');
  return getDifficulty(target);
}

function fromBits(bits) {
  var target = consensus.fromCompact(bits);
  return getDifficulty(target);
}

function getDifficulty(target) {
  var difficulty;

  if (target.cmpn(0) === 0)
    return util.MAX_SAFE_INTEGER;

  difficulty = DIFF_TARGET.div(target);

  if (difficulty.bitLength() > 53)
    return util.MAX_SAFE_INTEGER;

  return difficulty.toNumber();
}

function isJob(id) {
  if (typeof id !== 'string')
    return false;

  return id.length >= 12 && id.length <= 21;
}

function isSID(sid) {
  if (typeof sid !== 'string')
    return false;

  return sid.length === 8 && util.isHex(sid);
}

function isUsername(username) {
  if (typeof username !== 'string')
    return false;

  return username.length > 0 && username.length <= 100;
}

function isPassword(password) {
  if (typeof password !== 'string')
    return false;

  return password.length > 0 && password.length <= 255;
}

function isAddress(addr) {
  if (typeof addr !== 'string')
    return false;

  return addr.length > 0 && addr.length <= 255;
}

function isAgent(agent) {
  if (typeof agent !== 'string')
    return false;

  return agent.length > 0 && agent.length <= 255;
}

/*
 * Expose
 */

module.exports = Stratum;
