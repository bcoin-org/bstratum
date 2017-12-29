/*!
 * stratum.js - stratum server for bcoin
 * Copyright (c) 2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const path = require('path');
const os = require('os');
const {StringDecoder} = require('string_decoder');
const EventEmitter = require('events');
const {format} = require('util');
const {Lock} = require('bmutex');
const tcp = require('btcp');
const IP = require('binet');
const Logger = require('blgr');
const fs = require('bfile');
const hash256 = require('bcrypto/lib/hash256');
const ccmp = require('bcrypto/lib/ccmp');
const util = require('bcoin/lib/utils/util');
const consensus = require('bcoin/lib/protocol/consensus');
const List = require('bcoin/lib/utils/list');
const Network = require('bcoin/lib/protocol/network');
const common = require('bcoin/lib/mining/common');

/*
 * Constants
 */

const NONCE_SIZE = 4;

/**
 * Stratum Server
 * @extends {EventEmitter}
 */

class Stratum extends EventEmitter {
  /**
   * Create a stratum server.
   * @constructor
   * @param {Object} options
   */

  constructor(options) {
    super();

    this.options = new StratumOptions(options);

    this.node = this.options.node;
    this.chain = this.options.chain;
    this.network = this.options.network;
    this.logger = this.options.logger.context('stratum');
    this.difficulty = this.options.difficulty;

    this.server = tcp.createServer();
    this.sharedb = new ShareDB(this.options);
    this.userdb = new UserDB(this.options);
    this.locker = new Lock();
    this.jobMap = new Map();
    this.banned = new Map();
    this.jobs = new List();
    this.current = null;
    this.inbound = new List();
    this.lastActive = 0;
    this.subscribed = false;
    this.uid = 0;
    this.suid = 0;

    this._init();
  }

  static init(node) {
    const config = node.config;
    return new Stratum({
      node: node,
      prefix: config.prefix,
      logger: node.logger,
      host: config.str('stratum-host'),
      port: config.uint('stratum-port'),
      publicHost: config.str('stratum-public-host'),
      publicPort: config.uint('stratum-public-port'),
      maxInbound: config.uint('stratum-max-inbound'),
      difficulty: config.uint('stratum-difficulty'),
      dynamic: config.bool('stratum-dynamic'),
      password: config.str('stratum-password')
    });
  }

  sid() {
    const sid = this.suid;
    this.suid += 1;
    this.suid >>>= 0;
    return sid;
  }

  jid() {
    const now = util.now();
    const id = this.uid;
    this.uid += 1;
    this.uid >>>= 0;
    return `${now}:${id}`;
  }

  _init() {
    this.server.on('connection', (socket) => {
      this.handleSocket(socket);
    });

    this.node.on('connect', async () => {
      try {
        await this.handleBlock();
      } catch (e) {
        this.emit('error', e);
      }
    });

    this.node.on('tx', async () => {
      try {
        await this.handleTX();
      } catch (e) {
        this.emit('error', e);
      }
    });
  }

  async handleBlock() {
    const unlock = await this.locker.lock();
    try {
      return await this._handleBlock();
    } finally {
      unlock();
    }
  }

  async _handleBlock() {
    const now = util.now();

    if (!this.subscribed) {
      this.lastActive = now;
      return;
    }

    this.current = null;
    this.lastActive = now;

    await this.notifyAll();
  }

  async handleTX() {
    const unlock = await this.locker.lock();
    try {
      return await this._handleTX();
    } finally {
      unlock();
    }
  }

  async _handleTX() {
    const now = util.now();

    if (!this.subscribed) {
      this.lastActive = now;
      return;
    }

    if (now > this.lastActive + Stratum.ACTIVE_TIME) {
      this.current = null;
      this.lastActive = now;

      await this.notifyAll();
    }
  }

  async handleSocket(socket) {
    if (!socket.remoteAddress) {
      this.logger.debug('Ignoring disconnected client.');
      socket.destroy();
      return;
    }

    const host = IP.normalize(socket.remoteAddress);

    if (this.inbound.size >= this.options.maxInbound) {
      this.logger.debug('Ignoring client: too many inbound (%s).', host);
      socket.destroy();
      return;
    }

    if (this.isBanned(host)) {
      this.logger.debug('Ignoring banned client (%s).', host);
      socket.destroy();
      return;
    }

    socket.setKeepAlive(true);
    socket.setNoDelay(true);

    this.addClient(socket);
  }

  addClient(socket) {
    const conn = new Connection(this, socket);

    conn.on('error', (err) => {
      this.emit('error', err);
    });

    conn.on('close', () => {
      assert(this.inbound.remove(conn));
    });

    conn.on('ban', () => {
      this.handleBan(conn);
    });

    this.inbound.push(conn);
  }

  handleBan(conn) {
    this.logger.warning('Banning client (%s).', conn.id());
    this.banned.set(conn.host, util.now());
    conn.destroy();
  }

  isBanned(host) {
    const time = this.banned.get(host);

    if (time == null)
      return false;

    if (util.now() - time > Stratum.BAN_TIME) {
      this.banned.delete(host);
      return false;
    }

    return true;
  }

  async listen() {
    this.server.maxConnections = this.options.maxInbound;

    await this.server.listen(this.options.port, this.options.host);

    this.logger.info('Server listening on %d.', this.options.port);
  }

  async open() {
    if (this.node.miner.addresses.length === 0)
      throw new Error('No addresses available for coinbase.');

    await this.userdb.open();
    await this.sharedb.open();
    await this.listen();

    if (this.options.password) {
      if (!this.userdb.get('admin')) {
        this.userdb.add({
          username: 'admin',
          hash: this.options.password
        });
      }
    }

    this.lastActive = util.now();
  }

  async close() {
    let conn, next;

    for (conn = this.inbound.head; conn; conn = next) {
      next = conn.next;
      conn.destroy();
    }

    await this.server.close();
    await this.userdb.close();
    await this.sharedb.close();
  }

  async notifyAll() {
    const job = await this.getJob();
    let conn;

    this.logger.debug('Notifying all clients of new job: %s.', job.id);

    for (conn = this.inbound.head; conn; conn = conn.next) {
      if (conn.sid === -1)
        continue;

      conn.sendJob(job);
    }
  }

  createBlock() {
    if (this.node.miner.addresses.length === 0)
      throw new Error('No addresses available for coinbase.');

    return this.node.miner.createBlock();
  }

  addJob(job) {
    if (this.jobs.size >= Stratum.MAX_JOBS)
      this.removeJob(this.jobs.head);

    assert(this.jobs.push(job));

    assert(!this.jobMap.has(job.id));
    this.jobMap.set(job.id, job);

    this.current = job;
  }

  removeJob(job) {
    assert(this.jobs.remove(job));

    assert(this.jobMap.has(job.id));
    this.jobMap.delete(job.id);

    if (job === this.current)
      this.current = null;
  }

  async getJob() {
    if (!this.current) {
      const attempt = await this.createBlock();
      const job = Job.fromTemplate(this.jid(), attempt);

      this.addJob(job);

      this.logger.debug(
        'New job (id=%s, prev=%s).',
        job.id, util.revHex(job.attempt.prevBlock));
    }

    return this.current;
  }

  async tryCommit(entry, block) {
    try {
      await this.sharedb.commit(entry, block);
    } catch (e) {
      this.emit('error', e);
    }
  }

  auth(username, password) {
    const user = this.userdb.get(username);

    if (!user)
      return false;

    const passwd = Buffer.from(password, 'utf8');
    const hash = hash256.digest(passwd);

    if (!ccmp(hash, user.password))
      return false;

    return true;
  }

  authAdmin(password) {
    if (!this.options.password)
      return false;

    const data = Buffer.from(password, 'utf8');
    const hash = hash256.digest(data);

    if (!ccmp(hash, this.options.password))
      return false;

    return true;
  }

  async addBlock(conn, block) {
    // Broadcast immediately.
    this.node.broadcast(block);

    let entry;
    try {
      entry = await this.chain.add(block);
    } catch (e) {
      if (e.type === 'VerifyError') {
        switch (e.reason) {
          case 'high-hash':
            return new StratumError(23, 'high-hash');
          case 'duplicate':
            return new StratumError(22, 'duplicate');
        }
        return new StratumError(20, e.reason);
      }
      throw e;
    }

    if (!entry)
      return new StratumError(21, 'stale-prevblk');

    if (entry.hash !== this.chain.tip.hash)
      return new StratumError(21, 'stale-work');

    this.tryCommit(entry, block);

    this.logger.info('Client found block %s (%d) (%s).',
      entry.rhash(),
      entry.height,
      conn.id());

    return null;
  }

  async handlePacket(conn, msg) {
    const unlock = await this.locker.lock();
    try {
      return await this._handlePacket(conn, msg);
    } finally {
      unlock();
    }
  }

  async _handlePacket(conn, msg) {
    switch (msg.method) {
      case 'mining.authorize':
        return this.handleAuthorize(conn, msg);
      case 'mining.subscribe':
        return this.handleSubscribe(conn, msg);
      case 'mining.submit':
        return this.handleSubmit(conn, msg);
      case 'mining.get_transactions':
        return this.handleTransactions(conn, msg);
      case 'mining.authorize_admin':
        return this.handleAuthAdmin(conn, msg);
      case 'mining.add_user':
        return this.handleAddUser(conn, msg);
      default:
        return this.handleUnknown(conn, msg);
    }
  }

  async handleAuthorize(conn, msg) {
    if (typeof msg.params.length < 2) {
      conn.sendError(msg, 0, 'invalid params');
      return;
    }

    const user = msg.params[0];
    const pass = msg.params[1];

    if (!isUsername(user) || !isPassword(pass)) {
      conn.sendError(msg, 0, 'invalid params');
      return;
    }

    if (!this.auth(user, pass)) {
      this.logger.debug(
        'Client failed auth for user %s (%s).',
        user, conn.id());
      conn.sendResponse(msg, false);
      return;
    }

    this.logger.debug(
      'Client successfully authd for %s (%s).',
      user, conn.id());

    conn.addUser(user);
    conn.sendResponse(msg, true);
  }

  async handleSubscribe(conn, msg) {
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
      this.logger.debug('First subscriber (%s).', conn.id());
      this.subscribed = true;
    }

    const sid = hex32(conn.sid);
    const job = await this.getJob();

    this.logger.debug(
      'Client is subscribing with sid=%s (%s).',
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
  }

  async handleSubmit(conn, msg) {
    const now = this.network.now();

    let subm;
    try {
      subm = Submission.fromPacket(msg);
    } catch (e) {
      conn.sendError(msg, 0, 'invalid params');
      return;
    }

    this.logger.spam(
      'Client submitted job %s (%s).',
      subm.job, conn.id());

    if (!conn.hasUser(subm.username)) {
      conn.sendError(msg, 24, 'unauthorized user');
      return;
    }

    if (conn.sid === -1) {
      conn.sendError(msg, 25, 'not subscribed');
      return;
    }

    const job = this.jobMap.get(subm.job);

    if (!job || job.committed) {
      conn.sendError(msg, 21, 'job not found');
      return;
    }

    if (job !== this.current) {
      this.logger.warning(
        'Client is submitting a stale job %s (%s).',
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

    const share = job.check(conn.sid, subm);
    const difficulty = share.getDifficulty();

    if (difficulty < conn.difficulty - 1) {
      this.logger.debug(
        'Client submitted a low share of %d, hash=%s, ban=%d (%s).',
        difficulty, share.rhash(), conn.banScore, conn.id());

      conn.increaseBan(1);
      conn.sendError(msg, 23, 'high-hash');
      conn.sendDifficulty(conn.difficulty);

      return;
    }

    if (!job.insert(share.hash)) {
      this.logger.debug(
        'Client submitted a duplicate share: %s (%s).',
        share.rhash(), conn.id());
      conn.increaseBan(10);
      conn.sendError(msg, 22, 'duplicate');
      return;
    }

    this.sharedb.add(subm.username, difficulty);

    this.logger.debug(
      'Client submitted share of %d, hash=%s (%s).',
      difficulty, share.rhash(), conn.id());

    let error;
    if (share.verify(job.target)) {
      const block = job.commit(share);
      error = await this.addBlock(conn, block);
    }

    if (error) {
      this.logger.warning(
        'Client found an invalid block: %s (%s).',
        error.reason, conn.id());
      conn.sendError(msg, error.code, error.reason);
    } else {
      conn.sendResponse(msg, true);
    }

    if (this.options.dynamic) {
      if (conn.retarget(job.difficulty)) {
        this.logger.debug(
          'Retargeted client to %d (%s).',
          conn.nextDifficulty, conn.id());
      }
    }
  }

  async handleTransactions(conn, msg) {
    if (conn.sid === -1) {
      conn.sendError(msg, 25, 'not subscribed');
      return;
    }

    if (msg.params.length < 1) {
      conn.sendError(msg, 21, 'job not found');
      return;
    }

    const id = msg.params[0];

    if (!isJob(id)) {
      conn.sendError(msg, 21, 'job not found');
      return;
    }

    const job = this.jobMap.get(id);

    if (!job || job.committed) {
      conn.sendError(msg, 21, 'job not found');
      return;
    }

    this.logger.debug(
      'Sending tx list (%s).',
      conn.id());

    const attempt = job.attempt;
    const result = [];

    for (const item of attempt.items)
      result.push(item.tx.hash('hex'));

    conn.sendResponse(msg, result);
  }

  async handleAuthAdmin(conn, msg) {
    if (typeof msg.params.length < 1) {
      conn.sendError(msg, 0, 'invalid params');
      return;
    }

    const password = msg.params[0];

    if (!isPassword(password)) {
      conn.sendError(msg, 0, 'invalid params');
      return;
    }

    if (!this.authAdmin(password)) {
      this.logger.debug(
        'Client sent bad admin password (%s).',
        conn.id());
      conn.increaseBan(10);
      conn.sendError(msg, 0, 'invalid password');
      return;
    }

    conn.admin = true;
    conn.sendResponse(msg, true);
  }

  async handleAddUser(conn, msg) {
    if (typeof msg.params.length < 3) {
      conn.sendError(msg, 0, 'invalid params');
      return;
    }

    const user = msg.params[0];
    const pass = msg.params[1];

    if (!isUsername(user) || !isPassword(pass)) {
      conn.sendError(msg, 0, 'invalid params');
      return;
    }

    if (!conn.admin) {
      this.logger.debug(
        'Client is not an admin (%s).',
        conn.id());
      conn.sendError(msg, 0, 'invalid password');
      return;
    }

    try {
      this.userdb.add({
        username: user,
        password: pass
      });
    } catch (e) {
      conn.sendError(msg, 0, e.message);
      return;
    }

    conn.sendResponse(msg, true);
  }

  async handleUnknown(conn, msg) {
    this.logger.debug(
      'Client sent an unknown message (%s):',
      conn.id());

    this.logger.debug(msg);

    conn.send({
      id: msg.id,
      result: null,
      error: true
    });
  }
}

Stratum.id = 'stratum';

Stratum.ACTIVE_TIME = 60;
Stratum.MAX_JOBS = 6;
Stratum.SHARES_PER_MINUTE = 8;
Stratum.BAN_SCORE = 1000;
Stratum.BAN_TIME = 10 * 60;

/**
 * Stratum Options
 */

class StratumOptions {
  /**
   * Create stratum options.
   * @constructor
   * @param {Object} options
   */

  constructor(options) {
    this.node = null;
    this.chain = null;
    this.logger = Logger.global;
    this.network = Network.primary;
    this.host = '0.0.0.0';
    this.port = 3008;
    this.publicHost = '127.0.0.1';
    this.publicPort = 3008;
    this.maxInbound = 50;
    this.difficulty = 1000;
    this.dynamic = true;
    this.prefix = path.resolve(os.homedir(), '.bcoin', 'stratum');
    this.password = null;

    this.fromOptions(options);
  }

  fromOptions(options) {
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
      this.password = hash256.digest(Buffer.from(options.password, 'utf8'));
    }

    return this;
  }

  static fromOptions(options) {
    return new this().fromOptions(options);
  }
}

/**
 * Stratum Connection
 */

class Connection extends EventEmitter {
  /**
   * Create a stratum connection.
   * @constructor
   * @param {Stratum} stratum
   * @param {net.Socket} socket
   */

  constructor(stratum, socket) {
    super();

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
    this.users = new Set();
    this.sid = -1;
    this.difficulty = -1;
    this.nextDifficulty = -1;
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

  _init() {
    this.on('packet', async (msg) => {
      try {
        await this.readPacket(msg);
      } catch (e) {
        this.error(e);
      }
    });

    this.socket.on('data', (data) => {
      this.feed(data);
    });

    this.socket.on('error', (err) => {
      this.emit('error', err);
    });

    this.socket.on('close', () => {
      this.error('Socket hangup.');
      this.destroy();
    });

    this.socket.on('drain', () => {
      this.drainSize = 0;
    });
  }

  destroy() {
    if (this.destroyed)
      return;

    this.destroyed = true;

    this.locker.destroy();
    this.socket.destroy();
    this.socket = null;

    this.emit('close');
  }

  send(json) {
    if (this.destroyed)
      return;

    json = JSON.stringify(json);
    json += '\n';

    this.write(json);
  }

  write(text) {
    if (this.destroyed)
      return;

    if (this.socket.write(text, 'utf8') === false) {
      this.drainSize += Buffer.byteLength(text, 'utf8');
      if (this.drainSize > (5 << 20)) {
        this.logger.warning(
          'Client is not reading (%s).',
          this.id());
        this.destroy();
      }
    }
  }

  error(err) {
    if (this.destroyed)
      return;

    if (err instanceof Error) {
      err.message += ` (${this.id()})`;
      this.emit('error', err);
      return;
    }

    let msg = format.apply(null, arguments);

    msg += ` (${this.id()})`;

    this.emit('error', new Error(msg));
  }

  redirect() {
    const host = this.stratum.options.publicHost;
    const port = this.stratum.options.publicPort;

    const res = [
      'HTTP/1.1 200 OK',
      `X-Stratum: stratum+tcp://${host}:${port}`,
      'Connection: Close',
      'Content-Type: application/json; charset=utf-8',
      'Content-Length: 38',
      '',
      '',
      '{"error":null,"result":false,"id":0}'
    ];

    this.write(res.join('\r\n'));

    this.logger.debug('Redirecting client (%s).', this.id());

    this.destroy();
  }

  feed(data) {
    this.recv += this.decoder.write(data);

    if (this.recv.length >= 100000) {
      this.error('Too much data buffered (%s).', this.id());
      this.destroy();
      return;
    }

    if (/HTTP\/1\.1/i.test(this.recv)) {
      this.redirect();
      return;
    }

    const lines = this.recv.replace(/\r+/g, '').split(/\n+/);

    this.recv = lines.pop();

    for (const line of lines) {
      if (line.length === 0)
        continue;

      let msg;
      try {
        msg = ClientPacket.fromRaw(line);
      } catch (e) {
        this.error(e);
        continue;
      }

      this.emit('packet', msg);
    }
  }

  async readPacket(msg) {
    const unlock = await this.locker.lock();
    try {
      this.socket.pause();
      await this.handlePacket(msg);
    } finally {
      if (!this.destroyed)
        this.socket.resume();
      unlock();
    }
  }

  async handlePacket(msg) {
    return await this.stratum.handlePacket(this, msg);
  }

  addUser(username) {
    if (this.users.has(username))
      return false;

    this.users.add(username);

    return true;
  }

  hasUser(username) {
    return this.users.has(username);
  }

  increaseBan(score) {
    const now = Date.now();

    this.banScore *= Math.pow(1 - 1 / 60000, now - this.lastBan);
    this.banScore += score;
    this.lastBan = now;

    if (this.banScore >= Stratum.BAN_SCORE) {
      this.logger.debug(
        'Ban score exceeds threshold %d (%s).',
        this.banScore, this.id());
      this.ban();
    }
  }

  ban() {
    this.emit('ban');
  }

  sendError(msg, code, reason) {
    this.logger.spam(
      'Sending error %s (%s).',
      reason, this.id());

    this.send({
      id: msg.id,
      result: null,
      error: [code, reason, false]
    });
  }

  sendResponse(msg, result) {
    this.logger.spam(
      'Sending response %s (%s).',
      msg.id, this.id());

    this.send({
      id: msg.id,
      result: result,
      error: null
    });
  }

  sendMethod(method, params) {
    this.logger.spam(
      'Sending method %s (%s).',
      method, this.id());

    this.send({
      id: null,
      method: method,
      params: params
    });
  }

  sendDifficulty(difficulty) {
    assert(difficulty > 0, 'Difficulty must be at least 1.');

    this.logger.debug(
      'Setting difficulty=%d for client (%s).',
      difficulty, this.id());

    this.sendMethod('mining.set_difficulty', [difficulty]);
  }

  setDifficulty(difficulty) {
    this.nextDifficulty = difficulty;
  }

  sendJob(job) {
    this.logger.debug(
      'Sending job %s to client (%s).',
      job.id, this.id());

    if (this.nextDifficulty !== -1) {
      this.submissions = 0;
      this.lastRetarget = Date.now();
      this.sendDifficulty(this.nextDifficulty);
      this.difficulty = this.nextDifficulty;
      this.nextDifficulty = -1;
    }

    this.sendMethod('mining.notify', job.toJSON());
  }

  retarget(max) {
    const now = Date.now();
    const pm = Stratum.SHARES_PER_MINUTE;

    assert(this.difficulty > 0);
    assert(this.lastRetarget !== -1);

    this.submissions += 1;

    if (this.submissions % pm === 0) {
      const target = (this.submissions / pm) * 60000;
      let actual = now - this.lastRetarget;
      let difficulty = 0x100000000 / this.difficulty;

      if (max > (-1 >>> 0))
        max = -1 >>> 0;

      if (Math.abs(target - actual) <= 5000)
        return false;

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

      this.setDifficulty(difficulty);

      return true;
    }

    return false;
  }

  id() {
    let id = this.host;

    if (this.agent)
      id += '/' + this.agent;

    return id;
  }
}

/**
 * User
 */

class User {
  /**
   * Create a user.
   * @constructor
   * @param {Object} options
   */

  constructor(options) {
    this.username = '';
    this.password = consensus.ZERO_HASH;

    if (options)
      this.fromOptions(options);
  }

  fromOptions(options) {
    assert(options, 'Options required.');
    assert(isUsername(options.username), 'Username required.');
    assert(options.hash || options.password, 'Password required.');

    this.setUsername(options.username);

    if (options.hash != null)
      this.setHash(options.hash);

    if (options.password != null)
      this.setPassword(options.password);

    return this;
  }

  static fromOptions(options) {
    return new this().fromOptions(options);
  }

  setUsername(username) {
    assert(isUsername(username), 'Username must be a string.');
    this.username = username;
  }

  setHash(hash) {
    if (typeof hash === 'string') {
      assert(isHex(hash), 'Hash must be a hex string.');
      assert(hash.length === 64, 'Hash must be 32 bytes.');
      this.password = Buffer.from(hash, 'hex');
    } else {
      assert(Buffer.isBuffer(hash), 'Hash must be a buffer.');
      assert(hash.length === 32, 'Hash must be 32 bytes.');
      this.password = hash;
    }
  }

  setPassword(password) {
    assert(isPassword(password), 'Password must be a string.');
    password = Buffer.from(password, 'utf8');
    this.password = hash256.digest(password);
  }

  toJSON() {
    return {
      username: this.username,
      password: this.password.toString('hex')
    };
  }

  fromJSON(json) {
    assert(json);
    assert(typeof json.username === 'string');
    this.username = json.username;
    this.setHash(json.password);
    return this;
  }

  static fromJSON(json) {
    return new this().fromJSON(json);
  }
}

/**
 * ClientPacket
 */

class ClientPacket {
  /**
   * Create a packet.
   */

  constructor() {
    this.id = null;
    this.method = 'unknown';
    this.params = [];
  }

  static fromRaw(json) {
    const packet = new ClientPacket();
    const msg = JSON.parse(json);

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
  }
}

/**
 * Submission Packet
 */

class Submission {
  /**
   * Create a submission packet.
   */

  constructor() {
    this.username = '';
    this.job = '';
    this.nonce2 = 0;
    this.ts = 0;
    this.nonce = 0;
  }

  static fromPacket(msg) {
    const subm = new Submission();

    assert(msg.params.length >= 5, 'Invalid parameters.');

    assert(isUsername(msg.params[0]), 'Name must be a string.');
    assert(isJob(msg.params[1]), 'Job ID must be a string.');

    assert(typeof msg.params[2] === 'string', 'Nonce2 must be a string.');
    assert(msg.params[2].length === NONCE_SIZE * 2, 'Nonce2 must be a string.');
    assert(isHex(msg.params[2]), 'Nonce2 must be a string.');

    assert(typeof msg.params[3] === 'string', 'Time must be a string.');
    assert(msg.params[3].length === 8, 'Time must be a string.');
    assert(isHex(msg.params[3]), 'Time must be a string.');

    assert(typeof msg.params[4] === 'string', 'Nonce must be a string.');
    assert(msg.params[4].length === 8, 'Nonce must be a string.');
    assert(isHex(msg.params[4]), 'Nonce must be a string.');

    subm.username = msg.params[0];
    subm.job = msg.params[1];
    subm.nonce2 = parseInt(msg.params[2], 16);
    subm.ts = parseInt(msg.params[3], 16);
    subm.nonce = parseInt(msg.params[4], 16);

    return subm;
  }
}

/**
 * Job
 */

class Job {
  /**
   * Create a job.
   * @constructor
   */

  constructor(id) {
    assert(typeof id === 'string');

    this.id = id;
    this.attempt = null;
    this.target = consensus.ZERO_HASH;
    this.difficulty = 0;
    this.submissions = {};
    this.committed = false;
    this.prev = null;
    this.next = null;
  }

  fromTemplate(attempt) {
    this.attempt = attempt;
    this.attempt.refresh();
    this.target = attempt.target;
    this.difficulty = attempt.getDifficulty();
    return this;
  }

  static fromTemplate(id, attempt) {
    return new this(id).fromTemplate(attempt);
  }

  insert(hash) {
    hash = hash.toString('hex');

    if (this.submissions[hash])
      return false;

    this.submissions[hash] = true;

    return true;
  }

  check(nonce1, subm) {
    const nonce2 = subm.nonce2;
    const ts = subm.ts;
    const nonce = subm.nonce;
    return this.attempt.getProof(nonce1, nonce2, ts, nonce);
  }

  commit(share) {
    assert(!this.committed, 'Already committed.');
    this.committed = true;
    return this.attempt.commit(share);
  }

  toJSON() {
    return [
      this.id,
      common.swap32hex(this.attempt.prevBlock),
      this.attempt.left.toString('hex'),
      this.attempt.right.toString('hex'),
      this.attempt.tree.toJSON(),
      hex32(this.attempt.version),
      hex32(this.attempt.bits),
      hex32(this.attempt.ts),
      false
    ];
  }
}

/**
 * Stratum Error
 */

class StratumError {
  /**
   * Create a stratum error.
   * @constructor
   * @param {Number} code
   * @param {String} reason
   */

  constructor(code, reason) {
    this.code = code;
    this.reason = reason;
  }
}

/**
 * Share DB
 */

class ShareDB {
  /**
   * Create a Share DB
   * @constructor
   * @param {Object} options
   */

  constructor(options) {
    this.network = options.network;
    this.logger = options.logger;
    this.location = path.resolve(options.prefix, 'shares');

    this.map = Object.create(null);
    this.total = 0;
    this.size = 0;
  }

  async open() {
    await fs.mkdirp(this.location);
  }

  async close() {
    ;
  }

  file(entry) {
    const name = entry.height + '-' + entry.rhash();
    return path.resolve(this.location, name + '.json');
  }

  add(username, difficulty) {
    if (!this.map[username]) {
      this.map[username] = 0;
      this.size++;
    }

    this.map[username] += difficulty;
    this.total += difficulty;
  }

  clear() {
    this.map = Object.create(null);
    this.size = 0;
    this.total = 0;
  }

  async commit(entry, block) {
    const cb = block.txs[0];
    const addr = cb.outputs[0].getAddress();

    assert(addr);

    const data = {
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

    const file = this.file(entry);
    const json = JSON.stringify(data, null, 2);

    this.logger.info(
      'Committing %d payouts to disk for block %d (file=%s).',
      data.size, entry.height, file);

    await fs.writeFile(file, json);
  }
}

/**
 * User DB
 */

class UserDB {
  /**
   * Create a user DB.
   * @constructor
   * @param {Object} options
   */

  constructor(options) {
    this.network = options.network;
    this.logger = options.logger;
    this.location = path.resolve(options.prefix, 'users.json');
    this.locker = new Lock();
    this.lastFail = 0;
    this.stream = null;

    this.map = new Map();
    this.size = 0;
  }

  async open() {
    const unlock = await this.locker.lock();
    try {
      return await this._open();
    } finally {
      unlock();
    }
  }

  async _open() {
    await this.load();
  }

  async close() {
    const unlock = await this.locker.lock();
    try {
      return await this._close();
    } finally {
      unlock();
    }
  }

  async _close() {
    if (!this.stream)
      return;

    try {
      this.stream.close();
    } catch (e) {
      ;
    }

    this.stream = null;
  }

  load() {
    return new Promise((resolve, reject) => {
      this._load(resolve, reject);
    });
  }

  _load(resolve, reject) {
    let buf = '';
    let lineno = 0;

    let stream = fs.createReadStream(this.location, {
      flags: 'r',
      encoding: 'utf8',
      autoClose: true
    });

    const close = () => {
      if (!stream)
        return;

      try {
        stream.close();
      } catch (e) {
        ;
      }

      stream = null;
    };

    stream.on('error', (err) => {
      if (!stream)
        return;

      if (err.code === 'ENOENT') {
        close();
        resolve();
        return;
      }

      close();
      reject(err);
    });

    stream.on('data', (data) => {
      if (!stream)
        return;

      buf += data;

      if (buf.length >= 10000) {
        close();
        reject(new Error(`UserDB parse error. Line: ${lineno}.`));
        return;
      }

      const lines = buf.split(/\n+/);

      buf = lines.pop();

      for (const line of lines) {
        lineno += 1;

        if (line.length === 0)
          continue;

        let json, user;
        try {
          json = JSON.parse(line);
          user = User.fromJSON(json);
        } catch (e) {
          close();
          reject(new Error(`UserDB parse error. Line: ${lineno}.`));
          return;
        }

        if (!this.map.has(user.username))
          this.size += 1;

        this.map.set(user.username, user);
      }
    });

    stream.on('end', () => {
      if (!stream)
        return;

      this.logger.debug(
        'Loaded %d users into memory.',
        this.size);

      stream = null;
      resolve();
    });
  }

  get(username) {
    return this.map.get(username);
  }

  has(username) {
    return this.map.has(username);
  }

  add(options) {
    const user = new User(options);

    assert(!this.map.has(user.username), 'User already exists.');

    this.logger.debug(
      'Adding new user (%s).',
      user.username);

    this.map.set(user.username, user);
    this.size += 1;

    this.write(user.toJSON());
  }

  setPassword(username, password) {
    const user = this.map.get(username);
    assert(user, 'User does not exist.');
    user.setPassword(password);
    this.write(user.toJSON());
  }

  write(data) {
    const stream = this.getStream();

    if (!stream)
      return;

    const json = JSON.stringify(data) + '\n';
    stream.write(json, 'utf8');
  }

  getStream() {
    if (this.stream)
      return this.stream;

    if (this.lastFail > util.now() - 10)
      return null;

    this.lastFail = 0;

    this.stream = fs.createWriteStream(this.location, { flags: 'a' });

    this.stream.on('error', (err) => {
      this.logger.warning('UserDB file stream died!');
      this.logger.error(err);

      try {
        this.stream.close();
      } catch (e) {
        ;
      }

      // Retry in ten seconds.
      this.stream = null;
      this.lastFail = util.now();
    });

    return this.stream;
  }
}

/*
 * Helpers
 */

function isJob(id) {
  if (typeof id !== 'string')
    return false;

  return id.length >= 12 && id.length <= 21;
}

function isSID(sid) {
  if (typeof sid !== 'string')
    return false;

  return sid.length === 8 && isHex(sid);
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

function isAgent(agent) {
  if (typeof agent !== 'string')
    return false;

  return agent.length > 0 && agent.length <= 255;
}

function isHex(str) {
  return typeof str === 'string'
    && str.length % 2 === 0
    && /^[0-9a-f]$/i.test(str);
}

function hex32(num) {
  assert((num >>> 0) === num);
  num = num.toString(16);
  switch (num.length) {
    case 1:
      return `0000000${num}`;
    case 2:
      return `000000${num}`;
    case 3:
      return `00000${num}`;
    case 4:
      return `0000${num}`;
    case 5:
      return `000${num}`;
    case 6:
      return `00${num}`;
    case 7:
      return `0${num}`;
    case 8:
      return `${num}`;
    default:
      throw new Error();
  }
}

/*
 * Expose
 */

module.exports = Stratum;
