// Shared types between the client, worker, and workflow code.

// Failure injection is driven by order input, never randomness, so every
// demo scenario is reproducible on the first try.
export type SimulateMode = 'none' | 'flaky-inventory' | 'shipment-failure';

// Order item
export interface OrderItem {
  sku: string;
  quantity: number;
}

// Order shape
export interface Order {
  id: string;
  items: OrderItem[];
  amount: number; // USD
  simulate: SimulateMode;
}

// Order status types
export type OrderStatus =
  | 'RECEIVED'
  | 'PAYMENT_AUTHORIZED'
  | 'INVENTORY_RESERVED'
  | 'SHIPPED'
  | 'PAYMENT_CAPTURED'
  | 'COMPLETED'
  | 'COMPENSATING'
  | 'FAILED_COMPENSATED';

// Task queue name as a constant to be shared between client and worker
export const TASK_QUEUE = 'orders';
