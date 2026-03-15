// A2UI v0.8 Protocol Type Definitions
// Server-to-client and client-to-server message interfaces

// ── Primitive value wrapper ───────────────────────────────────────────────────

/** Wraps a single primitive value or a data-model path reference.
 *
 *  All four fields are optional in TypeScript (making `{}` structurally valid),
 *  but the A2UI protocol always sends exactly one of the four fields set.
 *  This is a receiver type — the server controls the wire format, so a
 *  discriminated union would not add safety over what the protocol already
 *  guarantees.
 */
export interface BoundValue {
  literalString?: string;
  literalNumber?: number;
  literalBoolean?: boolean;
  path?: string;
}

// ── Server-to-client messages ─────────────────────────────────────────────────

/** Sent when a surface starts rendering. Contains the component tree root. */
export interface BeginRenderingPayload {
  surfaceId: string;
  root: ComponentNode;
  catalogId?: string;
  styles?: Record<string, unknown>;
}

/** A node in the component tree. */
export interface ComponentNode {
  id: string;
  weight?: number;
  component: Record<string, Record<string, unknown>>;
}

/** Partial or full update to the components on a surface. */
export interface SurfaceUpdatePayload {
  surfaceId: string;
  components: ComponentNode[];
}

/** A single entry in a data model. */
export interface DataEntry {
  key: string;
  valueString?: string;
  valueNumber?: number;
  valueBoolean?: boolean;
  /** Map of nested entries. The map key duplicates each entry's own `.key` field
   *  by protocol convention — both are always equal for entries stored in a map. */
  valueMap?: Record<string, DataEntry>;
  valueList?: DataEntry[];
}

/** Update (full or partial) to the data model for a surface. */
export interface DataModelUpdatePayload {
  surfaceId: string;
  path?: string;
  contents: DataEntry[];
}

/** Instructs the client to tear down a surface. */
export interface DeleteSurfacePayload {
  surfaceId: string;
}

/** Union of all server-to-client A2UI message payloads. */
export type A2UIServerMessage =
  | { beginRendering: BeginRenderingPayload }
  | { surfaceUpdate: SurfaceUpdatePayload }
  | { dataModelUpdate: DataModelUpdatePayload }
  | { deleteSurface: DeleteSurfacePayload };

// ── Client-to-server messages ─────────────────────────────────────────────────

/** A user interaction event sent from the client to the server. */
export interface UserActionPayload {
  name: string;
  surfaceId: string;
  sourceComponentId: string;
  timestamp: number;
  context: Record<string, unknown>;
}

/** An error report from the client to the server. */
export interface A2UIErrorPayload {
  code?: string;
  surfaceId?: string;
  message: string;
}

/** Union of all client-to-server A2UI message payloads. */
export type A2UIClientMessage =
  | { userAction: UserActionPayload }
  | { error: A2UIErrorPayload };

// ── Bridge-specific messages (WebSocket bridge layer) ─────────────────────────

/** Sent by the bridge when a session is created. */
export interface SessionCreatedMessage {
  type: 'sessionCreated';
  sessionId: string;
}

/** A response from the bridge carrying a server payload.
 *
 *  `payload` is `unknown` by design — the bridge forwards raw server data
 *  without validating its shape. Callers should narrow before use:
 *  - `isA2UIMessage(payload)` → narrows to `A2UIServerMessage`
 *  - `isBridgeMessage(payload)` → narrows to `BridgeMessage`
 */
export interface BridgeResponseMessage {
  type: 'response';
  payload: unknown;
}

/** An error from the bridge layer. */
export interface BridgeErrorMessage {
  type: 'error';
  code?: string;
  message: string;
}

/** Acknowledgement that a user action was received by the bridge. */
export interface ActionAckMessage {
  type: 'actionAck';
  actionId: string;
}

/** Union of all bridge-layer messages (discriminated by `type`). */
export type BridgeMessage =
  | SessionCreatedMessage
  | BridgeResponseMessage
  | BridgeErrorMessage
  | ActionAckMessage;

// ── Type guards ───────────────────────────────────────────────────────────────

/**
 * Returns true if `value` is an A2UI server message
 * (contains one of the known A2UI message keys).
 */
export function isA2UIMessage(value: unknown): value is A2UIServerMessage {
  if (typeof value !== 'object' || value === null) return false;
  const obj = value as Record<string, unknown>;
  return (
    'beginRendering' in obj ||
    'surfaceUpdate' in obj ||
    'dataModelUpdate' in obj ||
    'deleteSurface' in obj
  );
}

/**
 * Returns true if `value` is a BridgeMessage
 * (has a string `type` discriminator field).
 */
export function isBridgeMessage(value: unknown): value is BridgeMessage {
  if (typeof value !== 'object' || value === null) return false;
  return typeof (value as Record<string, unknown>)['type'] === 'string';
}

// ── Runtime state ─────────────────────────────────────────────────────────────

/** The full runtime state of a mounted surface. */
export interface SurfaceState {
  surfaceId: string;
  catalogId: string;
  rootId: string;
  components: Map<string, ComponentNode>;
  dataModel: Map<string, DataEntry>;
  styles: Record<string, unknown>;
}

/** A component after its props have been resolved against the data model. */
export interface ResolvedComponent {
  id: string;
  type: string;
  resolvedProps: Record<string, unknown>;
}
