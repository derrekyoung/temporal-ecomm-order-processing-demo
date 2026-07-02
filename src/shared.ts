// Shared types between the client, worker, and workflow code.
// Kept dependency-free so the workflow bundle stays small and deterministic.

// Failure injection is driven by order input, never randomness, so every
// demo scenario is reproducible on the first try.
export type SimulateMode = 'none' | 'flaky-inventory' | 'shipment-failure';

export interface OrderItem {
  sku: string;
  quantity: number;
}

export interface Order {
  id: string;
  items: OrderItem[];
  amount: number; // USD
  simulate: SimulateMode;
}

export type OrderStatus =
  | 'RECEIVED'
  | 'PAYMENT_AUTHORIZED'
  | 'INVENTORY_RESERVED'
  | 'SHIPPED'
  | 'PAYMENT_CAPTURED'
  | 'COMPLETED'
  | 'COMPENSATING'
  | 'FAILED_COMPENSATED';

export const TASK_QUEUE = 'orders';
