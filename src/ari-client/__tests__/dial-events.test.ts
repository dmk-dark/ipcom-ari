import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { describe, expect, it } from 'vitest';
import { WebSocketServer } from 'ws';
import { AriClient } from '../ariClient';

describe('Dial delivery', () => {
  it('preserves concurrent peers and ordered transitions for the same peer', async () => {
    const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
    await once(server, 'listening');
    const address = server.address();
    if (typeof address === 'string') throw new Error('TCP address required');
    const client = new AriClient({ host: '127.0.0.1', port: address.port, username: 'test', password: 'test' });
    const received: string[] = [];
    try {
      await client.connectWebSocket(['test']);
      client.on('Dial', (event) => received.push(`${event.peer.id}:${event.dialstatus}`));
      for (const socket of server.clients) {
        for (const [id, status] of [['a', 'RINGING'], ['b', 'ANSWER'], ['c', 'ANSWER'], ['a', 'ANSWER']]) {
          socket.send(JSON.stringify({ type: 'Dial', application: 'test', peer: { id }, dialstatus: status }));
        }
      }
      await delay(200);
      expect(received).toEqual(['a:RINGING', 'b:ANSWER', 'c:ANSWER', 'a:ANSWER']);
    } finally {
      await client.cleanup();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });
});
