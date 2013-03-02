import threading
import time
import zmq
import binascii
import os
from socket import gethostname
import thread

PING_INTERVAL = 2000
REDISPATCH_INTERVAL = 0.5

def set_hwm(s, hwm):
  major = zmq.core._version.zmq_version_info()[0]
#  major = zmq.core.version.zmq_version_info()[0]
  if major < 3:
    s.hwm = hwm
  else:
    s.sndhwm = hwm
    s.rcvhwm = hwm

def zpipe(ctx):
  a = ctx.socket(zmq.PAIR)
  a.linger = 0
  b = ctx.socket(zmq.PAIR)
  b.linger = 0
  set_hwm(a, 1)
  set_hwm(b, 1)
  iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
  a.bind(iface)
  b.connect(iface)
  return a,b

class FreelanceClient(object):
  ctx = None
  pipe = None
  agent = None

  def __init__(self, context, appid, changed_host_cb, global_timeout=20000, server_ttl=6000):
    self.ctx = context
    self.pipe, peer = zpipe(self.ctx)
    self.agent = threading.Thread(target=agent_task, args=(self.ctx, peer, appid, changed_host_cb, global_timeout, server_ttl))
    self.agent.daemon = True
    self.agent.start()

  def connect(self, endpoint):
    self.pipe.send_multipart(["CONNECT", endpoint])
    time.sleep(0.1)

  def request(self, msg):
    request = ["REQUEST"] + msg
    self.pipe.send_multipart(request)
    reply = self.pipe.recv_multipart()
    status = reply.pop(0)
    if status != "FAILED":
      return reply


class FreelanceServer(object):
  endpoint = None
  alive = True
  ping_at = 0
  expires = 0

  def __init__(self, endpoint, server_ttl):
    self.endpoint = endpoint
    self.alive = True
    self.server_ttl = server_ttl
    self.ping_at = time.time() + 1e-3 * PING_INTERVAL
    self.expires = time.time() + 1e-3 * self.server_ttl

  def __str__(self):
    return self.endpoint

  def __repr__(self):
    return self.endpoint

  def ping(self, socket):
    if time.time() > self.ping_at:
      socket.send_multipart([self.endpoint, "PING"])
      self.ping_at = time.time() + 1e-3 * PING_INTERVAL

  def tickless(self, tickless):
    if tickless > self.ping_at:
      tickless = self.ping_at
    return tickless


class FreelanceAgent(object):
  ctx = None
  pipe = None
  router = None
  servers = None
  actives = None
  sequence = 0
  request = None
  reply = None
  expires = 0

  def __init__(self, ctx, pipe, appid, global_timeout, server_ttl):
    self.ctx = ctx
    self.pipe = pipe
    self.router = ctx.socket(zmq.ROUTER)
    self.router.identity = "%s-%s-%s-%s" % (appid, os.getpid(), thread.get_ident(), gethostname())
    set_hwm(self.router, 10)
    self.servers = {}
    self.actives = []
    self.request_id = ""
    self.last_endpoint = ""
    self.next_try = 0
    self.global_timeout = global_timeout
    self.server_ttl = server_ttl

  def control_message(self):
    msg = self.pipe.recv_multipart()
    command = msg.pop(0)

    if command == "CONNECT":
      endpoint = msg.pop(0)
      self.router.connect(endpoint)
      server = FreelanceServer(endpoint, self.server_ttl)
      self.servers[endpoint] = server
      self.actives.append(server)
      server.ping_at = time.time() + 1e-3 * PING_INTERVAL
      server.expires = time.time() + 1e-3 * self.server_ttl
    elif command == "REQUEST":
      assert not self.request
      self.request = [str(self.sequence)] + msg
      self.expires = time.time() + 1e-3 * self.global_timeout

  def router_message(self):
    reply = self.router.recv_multipart()
    endpoint = reply.pop(0)
    server = self.servers[endpoint]
    if not server.alive:
      self.actives.append(server)
      server.alive = 1

    server.ping_at = time.time() + 1e-3 * PING_INTERVAL
    server.expires = time.time() + 1e-3 * self.server_ttl

    sequence = reply.pop(0)
    try:
      if int(sequence) == self.sequence:
        self.sequence += 1
        reply = ["OK"] + reply
        self.pipe.send_multipart(reply)
        self.request = None
        return endpoint
    except:
      pass

  def dispatch_request(self, endpoint):
    dispatch = self.request_id != self.request[0] or endpoint != self.last_endpoint or time.time() > self.next_try
    if dispatch:
      request = [endpoint] + self.request
      self.request_id = self.request[0]
      self.last_endpoint = endpoint
      self.next_try = time.time() + REDISPATCH_INTERVAL
      self.router.send_multipart(request)

def agent_task(ctx, pipe, appid, changed_host_cb, global_timeout, server_ttl):
  agent = FreelanceAgent(ctx, pipe, appid, global_timeout, server_ttl)
  poller = zmq.Poller()
  poller.register(agent.pipe, zmq.POLLIN)
  poller.register(agent.router, zmq.POLLIN)
  current_endpoint = None
  new_endpoint = None

  while True:
    tickless = time.time() + 5
    if agent.request and tickless > agent.expires:
      tickless = agent.expires
      for server in agent.servers.values():
        tickless = server.tickless(tickless)
    try:
      items = dict(poller.poll(1000 * (tickless - time.time())))
    except:
      break

    if agent.pipe in items:
      agent.control_message()

    if agent.router in items:
      tmp_endpoint = agent.router_message()
      if tmp_endpoint:
        new_endpoint = tmp_endpoint


    if agent.request:
      if time.time() >= agent.expires:
        agent.pipe.send("FAILED")
        agent.request = None
      else:
        while agent.actives:
          server = agent.actives[0]
          if time.time() >= server.expires:
            server.alive = 0
            agent.actives.pop(0)
          else:
            agent.dispatch_request(server.endpoint)
            break
            
    if current_endpoint != new_endpoint:
      if current_endpoint and changed_host_cb:
        changed_host_cb(new_endpoint)
      current_endpoint = new_endpoint

    for server in agent.servers.values():
      server.ping(agent.router)
