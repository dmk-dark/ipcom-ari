import { once } from 'node:events';
import { createServer, type Server, type Socket } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { WebSocketServer } from 'ws';
import { AriClient } from '../ariClient';
import { BaseClient } from '../baseClient';
import { WebSocketClient } from '../websocketClient';

const clients: WebSocketClient[] = [];
const servers: Array<Server | WebSocketServer> = [];
const serverSockets = new Set<Socket>();

function clientFor(port: number): WebSocketClient {
  const client = new WebSocketClient(
    new BaseClient(`http://127.0.0.1:${port}/ari`, 'test', 'test'),
    ['test']
  );
  clients.push(client);
  return client;
}

function portOf(server: Server | WebSocketServer): number {
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Missing TCP address');
  return address.port;
}

async function stalledServer(): Promise<Server> {
  const server = createServer((socket) => {
    serverSockets.add(socket);
    socket.once('close', () => serverSockets.delete(socket));
    socket.once('data', () => server.emit('handshake'));
  });
  servers.push(server);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return server;
}

async function healthyServer(): Promise<WebSocketServer> {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  servers.push(server);
  await once(server, 'listening');
  return server;
}

afterEach(async () => {
  await Promise.all(clients.splice(0).map((client) => client.close()));
  for (const socket of serverSockets) socket.destroy();
  serverSockets.clear();
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve) => server.close(() => resolve()))));
  vi.restoreAllMocks();
});

describe('WebSocket shutdown', () => {
  it('rejects a pending handshake and releases its TCP connection', async () => {
    const server = await stalledServer();
    const client = clientFor(portOf(server));
    const handshake = once(server, 'handshake');
    const result = client.connect().then(() => undefined, (error: unknown) => error);
    await handshake;
    await Promise.all([client.close(), client.close()]);
    expect(await result).toBeInstanceOf(Error);
    await delay(20);
    expect(serverSockets.size).toBe(0);
    expect(client.getState()).toBe(3);
  });

  it('cancels a retry delay without opening another connection', async () => {
    const server = await stalledServer();
    let accepted = 0;
    server.on('connection', (socket) => {
      accepted++;
      socket.once('data', () => socket.end('HTTP/1.1 503 Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'));
    });
    vi.spyOn(Math, 'random').mockReturnValue(1);
    const retrying = new Promise<void>((resolve) => {
      vi.spyOn(console, 'warn').mockImplementation((message: unknown) => {
        if (typeof message === 'string' && message.startsWith('Connection attempt #')) resolve();
      });
    });
    const client = clientFor(portOf(server));
    const result = client.connect().then(() => undefined, (error: unknown) => error);
    await retrying;
    await client.close();
    const cancelled = await Promise.race([result, delay(100).then(() => undefined)]);
    expect(cancelled).toBeInstanceOf(Error);
    await delay(600);
    expect(accepted).toBe(1);
    expect(serverSockets.size).toBe(0);
  });

  it('keeps another client receiving events after cancelling a handshake', async () => {
    const healthy = await healthyServer();
    const active = clientFor(portOf(healthy));
    await active.connect();
    const stalled = await stalledServer();
    const cancelled = clientFor(portOf(stalled));
    const handshake = once(stalled, 'handshake');
    const result = cancelled.connect().catch((error: unknown) => error);
    await handshake;
    await cancelled.close();
    await result;
    const received = once(active, 'StasisStart');
    for (const socket of healthy.clients) {
      socket.send(JSON.stringify({ type: 'StasisStart', application: 'test', channel: { id: 'still-active' }, args: [] }));
    }
    const [event] = await received;
    expect(event.channel.id).toBe('still-active');
    expect(active.isConnected()).toBe(true);
  });

  it('stops heartbeat when closing an established connection', async () => {
    const server = await healthyServer();
    const client = clientFor(portOf(server));
    const cleared = vi.spyOn(globalThis, 'clearInterval');
    await client.connect();
    await client.close();
    expect(cleared).toHaveBeenCalledOnce();
    await delay(20);
    expect(server.clients.size).toBe(0);
  });

  it('allows an explicit connect after closing the previous connection', async () => {
    const server = await healthyServer();
    const client = clientFor(portOf(server));
    await client.connect();
    await client.close();
    await client.connect();
    const received = once(client, 'StasisStart');
    for (const socket of server.clients) {
      socket.send(JSON.stringify({ type: 'StasisStart', application: 'test', channel: { id: 'reopened' }, args: [] }));
    }
    const [event] = await received;
    expect(event.channel.id).toBe('reopened');
  });

  it('ARI cleanup waits for cancellation and is safe when repeated', async () => {
    const server = await stalledServer();
    const client = new AriClient({ host: '127.0.0.1', port: portOf(server), username: 'test', password: 'test' });
    const handshake = once(server, 'handshake');
    const result = client.connectWebSocket(['test']).then(() => undefined, (error: unknown) => error);
    await handshake;
    await Promise.all([client.cleanup(), client.cleanup()]);
    expect(await result).toBeInstanceOf(Error);
    await delay(20);
    expect(serverSockets.size).toBe(0);
    await client.cleanup();
  });
});
