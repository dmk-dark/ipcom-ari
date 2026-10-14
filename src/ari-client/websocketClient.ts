import { EventEmitter } from 'events';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket from 'ws';
import type { AriClient } from './ariClient';
import type { BaseClient } from './baseClient.js';
import type { WebSocketEvent, WebSocketEventType } from './interfaces';

const DEFAULT_MAX_RECONNECT_ATTEMPTS = 30;
const DEFAULT_STARTING_DELAY = 500;
const DEFAULT_MAX_DELAY = 10000;

/**
 * WebSocketClient handles real-time communication with the Asterisk server.
 * Extends EventEmitter to provide event-based communication patterns.
 */
export class WebSocketClient extends EventEmitter {
  private ws?: WebSocket;
  private isReconnecting = false;
  private isConnecting = false; // 🔹 Evita múltiplas conexões simultâneas
  private shouldReconnect = true; // 🔹 Nova flag para impedir reconexão se for um fechamento intencional
  private readonly maxReconnectAttempts = DEFAULT_MAX_RECONNECT_ATTEMPTS;
  private reconnectionAttempts = 0;
  private lastWsUrl: string = '';
  private eventQueue: Map<string, NodeJS.Timeout> = new Map();
  private connectionAbort = new AbortController();
  private closing?: Promise<void>;

  /**
   * Logs the current connection status of the WebSocket client at regular intervals.
   *
   * This method sets up an interval that logs various connection-related metrics every 60 seconds.
   * The logged information includes:
   * - The number of active connections (0 or 1)
   * - The current state of the WebSocket connection
   * - The number of reconnection attempts made
   * - The size of the event queue
   *
   * This can be useful for monitoring the health and status of the WebSocket connection over time.
   *
   * @private
   * @returns {void}
   */
  private logConnectionStatus(): void {
    setInterval(() => {
      console.log({
        connections: this.ws ? 1 : 0,
        state: this.getState(),
        reconnectAttempts: this.reconnectionAttempts,
        eventQueueSize: this.eventQueue.size,
      });
    }, 60000);
  }

  /**
   * Sets up a heartbeat mechanism for the WebSocket connection.
   *
   * This method creates an interval that sends a ping message every 30 seconds
   * to keep the connection alive. The heartbeat is automatically cleared when
   * the WebSocket connection is closed.
   *
   * @private
   * @returns {void}
   */
  private setupHeartbeat(socket: WebSocket): void {
    const interval = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) socket.ping();
    }, 30000);
    socket.once('close', () => clearInterval(interval));
  }


  /**
   * Creates a new WebSocketClient instance.
   *
   * This constructor initializes a WebSocketClient with the necessary dependencies and configuration.
   * It ensures that at least one application name is provided.
   *
   * @param baseClient - The BaseClient instance used for basic ARI operations and authentication.
   * @param apps - An array of application names to connect to via the WebSocket.
   * @param subscribedEvents - Optional. An array of WebSocketEventTypes to subscribe to. If not provided, all events will be subscribed.
   * @param ariClient - Optional. The AriClient instance, used for creating Channel and Playback instances when processing events.
   *
   * @throws {Error} Throws an error if the apps array is empty.
   */
  constructor(
    private readonly baseClient: BaseClient,
    private apps: string[],
    private subscribedEvents?: WebSocketEventType[],
    private readonly ariClient?: AriClient
  ) {
    super();

    if (!apps.length) {
      throw new Error('At least one application name is required');
    }
  }

  /**
   * Establishes a WebSocket connection to the Asterisk server.
   *
   * This method constructs the WebSocket URL using the base URL, credentials,
   * application names, and subscribed events. It then initiates the connection
   * using the constructed URL.
   *
   * @returns A Promise that resolves when the WebSocket connection is successfully established.
   * @throws Will throw an error if the connection cannot be established.
   */
  public async connect(): Promise<void> {
    if (this.isConnecting || this.isConnected()) {
      console.warn(
        'WebSocket is already connecting or connected. Skipping new connection.'
      );
      return;
    }

    if (this.closing) await this.closing;
    this.connectionAbort = new AbortController();
    this.shouldReconnect = true; // 🔹 Permite reconexão caso o WebSocket caia inesperadamente
    this.isConnecting = true;
    const { baseUrl, username, password, secure } = this.baseClient.getCredentials();

    const protocol = secure ? 'wss' : 'ws';
    const normalizedHost = baseUrl
      .replace(/^https?:\/\//, '')
      .replace(/\/ari$/, '');

    const queryParams = new URLSearchParams();
    
    // ✅ Usar api_key para conexões não seguras (HTTP/WS)
    if (!secure) {
      queryParams.append('api_key', `${username}:${password}`);
    }
    
    queryParams.append('app', this.apps.join(','));
    this.subscribedEvents?.forEach((event) =>
      queryParams.append('event', event)
    );

    // ✅ Construir URL baseada no tipo de conexão
    if (secure) {
      // HTTPS/WSS: usar HTTP Basic Auth na URL (seu caso)
      this.lastWsUrl = `${protocol}://${encodeURIComponent(username)}:${encodeURIComponent(password)}@${normalizedHost}/ari/events?${queryParams.toString()}`;
    } else {
      // HTTP/WS: usar api_key query parameter (caso da issue)
      this.lastWsUrl = `${protocol}://${normalizedHost}/ari/events?${queryParams.toString()}`;
    }

    console.log(`WebSocket URL: ${this.lastWsUrl.replace(/(api_key=)[^&]*/, '$1***')}`); // Log sem mostrar credenciais

    try {
      await this.initializeWebSocket(this.lastWsUrl);
    } finally {
      this.isConnecting = false;
    }
  }

  /**
   * Reconecta o WebSocket com uma lista atualizada de aplicações.
   *
   * @param {string[]} newApps - Lista de aplicações para reconectar
   * @param {WebSocketEventType[]} [subscribedEvents] - Tipos de eventos para se inscrever (opcional)
   * @returns {Promise<void>} Promise resolvida quando reconectado com sucesso
   */
  public async reconnectWithApps(
    newApps: string[],
    subscribedEvents?: WebSocketEventType[]
  ): Promise<void> {
    if (!newApps.length) {
      throw new Error('At least one application name is required');
    }

    // Mesclar aplicações existentes com novas
    const uniqueApps = Array.from(new Set([...this.apps, ...newApps]));

    // Se não há mudanças nas aplicações, não reconectar
    if (
      uniqueApps.length === this.apps.length &&
      uniqueApps.every((app) => this.apps.includes(app))
    ) {
      console.log(
        'No changes in applications list, maintaining current connection'
      );
      return;
    }

    console.log(
      `Reconnecting WebSocket with updated applications: ${uniqueApps.join(', ')}`
    );

    // Armazenar os aplicativos atualizados
    this.apps = uniqueApps;

    // Atualizar eventos inscritos se fornecidos
    if (subscribedEvents) {
      this.subscribedEvents = subscribedEvents;
    }

    // Fechar conexão existente
    if (this.ws) {
      await new Promise<void>((resolve) => {
        this.once('disconnected', () => resolve());
        this.close();
      });
    }

    // Reconectar com apps atualizados
    await this.connect();
    console.log('WebSocket reconnected successfully with updated applications');
  }

  /**
   * Adiciona novas aplicações à conexão WebSocket existente.
   *
   * @param {string[]} newApps - Lista de novas aplicações para adicionar
   * @param {WebSocketEventType[]} [subscribedEvents] - Tipos de eventos para se inscrever (opcional)
   * @returns {Promise<void>} Promise resolvida quando as aplicações são adicionadas com sucesso
   */
  public async addApps(
    newApps: string[],
    subscribedEvents?: WebSocketEventType[]
  ): Promise<void> {
    if (!newApps.length) {
      throw new Error('At least one application name is required');
    }

    // Verificar se há novas aplicações que ainda não estão na lista
    const appsToAdd = newApps.filter((app) => !this.apps.includes(app));

    if (appsToAdd.length === 0) {
      console.log('All applications are already registered');
      return;
    }

    // Adicionar novas aplicações à lista atual
    await this.reconnectWithApps(appsToAdd, subscribedEvents);
  }

  /**
   * Initializes a WebSocket connection with exponential backoff retry mechanism.
   *
   * This method attempts to establish a WebSocket connection to the specified URL.
   * It sets up event listeners for the WebSocket's 'open', 'message', 'close', and 'error' events.
   * If the connection is successful, it emits a 'connected' event. If it's a reconnection,
   * it also emits a 'reconnected' event with the current apps and subscribed events.
   * In case of connection failure, it uses an exponential backoff strategy to retry.
   *
   * @param wsUrl - The WebSocket URL to connect to.
   * @returns A Promise that resolves when the connection is successfully established,
   *          or rejects if an error occurs during the connection process.
   * @throws Will throw an error if the WebSocket connection cannot be established
   *         after the maximum number of retry attempts.
   */
  private async initializeWebSocket(wsUrl: string): Promise<void> {
    const signal = this.connectionAbort.signal;
    for (let attempt = 1; ; attempt++) {
      signal.throwIfAborted();
      try {
        await new Promise<void>((resolve, reject) => {
          const socket = new WebSocket(wsUrl);
          this.ws = socket;
          let opened = false;
          const onAbort = (): void => socket.close();
          signal.addEventListener('abort', onAbort, { once: true });

          socket.once('open', () => {
            if (signal.aborted) return socket.close();
            opened = true;
            this.setupHeartbeat(socket);
            if (this.isReconnecting) {
              this.emit('reconnected', {
                apps: this.apps,
                subscribedEvents: this.subscribedEvents,
              });
            }
            this.isReconnecting = false;
            this.reconnectionAttempts = 0;
            this.emit('connected');
            resolve();
          });
          socket.on('message', (data) => {
            if (!signal.aborted) this.handleMessage(data.toString());
          });
          // CONNECTING close emits error before close; keep this listener until closure.
          socket.on('error', (error: Error) => {
            if (!signal.aborted) console.error('WebSocket error:', error.message);
            reject(error);
          });
          socket.once('close', () => {
            signal.removeEventListener('abort', onAbort);
            if (this.ws === socket) this.ws = undefined;
            if (!opened) reject(new Error('WebSocket closed before opening'));
            if (opened && !signal.aborted && !this.isReconnecting) {
              void this.reconnect(wsUrl);
            }
          });
        });
        return;
      } catch (error) {
        signal.throwIfAborted();
        if (attempt >= this.maxReconnectAttempts) throw error;
        console.warn(
          `Connection attempt #${attempt} failed:`,
          error instanceof Error ? error.message : String(error)
        );
        const maximumDelay = Math.min(
          DEFAULT_STARTING_DELAY * 2 ** (attempt - 1),
          DEFAULT_MAX_DELAY
        );
        await delay(Math.random() * maximumDelay, undefined, { signal });
      }
    }
  }

  private getEventKey(event: WebSocketEvent): string {
    // Cria uma chave única baseada no tipo de evento e IDs relevantes
    const ids = [];
    if ('channel' in event && event.channel?.id) ids.push(event.channel.id);
    if ('playback' in event && event.playback?.id) ids.push(event.playback.id);
    if ('bridge' in event && event.bridge?.id) ids.push(event.bridge.id);
    return `${event.type}-${ids.join('-')}`;
  }

  private processEvent(event: WebSocketEvent): void {
    if (
      this.subscribedEvents?.length &&
      !this.subscribedEvents.includes(event.type)
    ) {
      return;
    }

    if ('channel' in event && event.channel?.id && this.ariClient) {
      const instanceChannel = this.ariClient.Channel(event.channel.id);
      instanceChannel.emitEvent(event);
      event.instanceChannel = instanceChannel;
    }

    if ('playback' in event && event.playback?.id && this.ariClient) {
      const instancePlayback = this.ariClient.Playback(event.playback.id);
      instancePlayback.emitEvent(event);
      event.instancePlayback = instancePlayback;
    }

    if ('bridge' in event && event.bridge?.id && this.ariClient) {
      const instanceBridge = this.ariClient.Bridge(event.bridge.id);
      instanceBridge.emitEvent(event);
      event.instanceBridge = instanceBridge;
    }

    this.emit(event.type, event);
  }

  /**
   * Handles incoming WebSocket messages by parsing and processing events.
   *
   * This method parses the raw message into a WebSocketEvent, filters it based on
   * subscribed events (if any), processes channel and playback events, and emits
   * the event to listeners. It also handles any errors that occur during processing.
   *
   * @param rawMessage - The raw message string received from the WebSocket connection.
   * @returns void This method doesn't return a value but emits events.
   *
   * @throws Will emit an 'error' event if the message cannot be parsed or processed.
   */
  private handleMessage(rawMessage: string): void {
    try {
      const parsed: unknown = JSON.parse(rawMessage);
      if (
        !parsed ||
        typeof parsed !== 'object' ||
        !('type' in parsed) ||
        typeof (parsed as Record<string, unknown>).type !== 'string'
      ) {
        console.warn('Received malformed WebSocket message (missing type)');
        return;
      }
      const event = parsed as WebSocketEvent;

      // Dial is a transition stream, not a state snapshot: never coalesce calls or statuses.
      if (event.type === 'Dial') {
        this.processEvent(event);
        return;
      }

      // Debounce eventos similares
      const key = this.getEventKey(event);
      const existing = this.eventQueue.get(key);
      if (existing) {
        clearTimeout(existing);
      }

      this.eventQueue.set(
        key,
        setTimeout(() => {
          this.processEvent(event);
          this.eventQueue.delete(key);
        }, 100)
      );
    } catch (error) {
      console.error('Error processing WebSocket message:', error);
      this.emit('error', new Error('Failed to decode WebSocket message'));
    }
  }

  /**
   * Attempts to reconnect to the WebSocket server using an exponential backoff strategy.
   *
   * This method is called when the WebSocket connection is closed unexpectedly.
   * It increments the reconnection attempt counter, logs the attempt, and uses
   * the backOff utility to retry the connection with exponential delays between attempts.
   *
   * @param wsUrl - The WebSocket URL to reconnect to.
   * @returns void - This method doesn't return a value.
   *
   * @emits reconnectFailed - Emitted if all reconnection attempts fail.
   */
  private async reconnect(wsUrl: string): Promise<void> {
    if (!this.shouldReconnect) {
      console.warn(
        'Reconnection skipped because WebSocket was intentionally closed.'
      );
      return;
    }

    if (this.isReconnecting) {
      console.warn('Já há uma tentativa de reconexão em andamento.');
      return;
    }

    this.isReconnecting = true;
    this.reconnectionAttempts++;
    console.log(`Tentando reconexão #${this.reconnectionAttempts}...`);

    try {
      await this.initializeWebSocket(wsUrl);
    } catch (error) {
      if (!this.connectionAbort.signal.aborted) {
        console.error('Failed to reconnect:', error);
        this.emit('reconnectFailed', error);
      }
    } finally {
      this.isReconnecting = false;
    }
  }

  /**
   * Cancels pending connects and retry delays, then closes the current socket.
   * Keeps error/close listeners until closure so handshake cancellation rejects
   * connect() without an unhandled error and heartbeat cleanup still runs.
   * Concurrent calls wait for the same bounded close operation.
   */
  public close(): Promise<void> {
    if (this.closing) return this.closing;
    const closing = this.runClose().finally(() => {
      this.closing = undefined;
    });
    this.closing = closing;
    return closing;
  }

  private async runClose(): Promise<void> {
    this.shouldReconnect = false;
    const socket = this.ws;
    this.connectionAbort.abort(new Error('WebSocket connection cancelled'));
    this.eventQueue.forEach((timeout) => clearTimeout(timeout));
    this.eventQueue.clear();

    if (socket && socket.readyState !== WebSocket.CLOSED) {
      const closeTimeout = setTimeout(() => socket.terminate(), 5000);
      try {
        await new Promise<void>((resolve) => {
          socket.once('close', resolve);
          socket.close();
        });
      } finally {
        clearTimeout(closeTimeout);
        socket.removeAllListeners();
      }
    }
    if (this.ws === socket) this.ws = undefined;
    this.emit('disconnected');
  }

  /**
   * Checks if the WebSocket connection is currently open and active.
   *
   * This method provides a way to determine the current state of the WebSocket connection.
   * It checks if the WebSocket's readyState property is equal to WebSocket.OPEN,
   * which indicates an active connection.
   *
   * @returns {boolean} True if the WebSocket connection is open and active, false otherwise.
   */
  public isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  /**
   * Retrieves the current state of the WebSocket connection.
   *
   * This method provides a way to check the current state of the WebSocket connection.
   * It returns a number corresponding to one of the WebSocket readyState values:
   * - 0 (CONNECTING): The connection is not yet open.
   * - 1 (OPEN): The connection is open and ready to communicate.
   * - 2 (CLOSING): The connection is in the process of closing.
   * - 3 (CLOSED): The connection is closed or couldn't be opened.
   *
   * If the WebSocket instance doesn't exist, it returns WebSocket.CLOSED (3).
   *
   * @returns {number} A number representing the current state of the WebSocket connection.
   */
  public getState(): number {
    return this.ws?.readyState ?? WebSocket.CLOSED;
  }

  /**
   * Cleans up the WebSocketClient instance, resetting its state and clearing resources.
   *
   * This method performs the following cleanup operations:
   * - Clears the event queue and cancels any pending timeouts.
   * - Stops any ongoing reconnection attempts.
   * - Clears the stored WebSocket URL.
   * - Resets the reconnection attempt counter.
   * - Removes all event listeners attached to this instance.
   *
   * This method is typically called when the WebSocketClient is no longer needed or
   * before reinitializing the client to ensure a clean slate.
   *
   * @returns {void} This method doesn't return a value.
   */
  public cleanup(): void {
    // Limpar event queue
    this.eventQueue.forEach((timeout) => clearTimeout(timeout));
    this.eventQueue.clear();

    // Parar tentativas de reconexão
    this.shouldReconnect = false;
    this.connectionAbort.abort(new Error('WebSocket connection cancelled'));
    this.isReconnecting = false;

    // Limpar URL armazenada
    this.lastWsUrl = '';

    // Resetar contadores
    this.reconnectionAttempts = 0;

    // Remover todos os listeners
    this.removeAllListeners();
  }
}
