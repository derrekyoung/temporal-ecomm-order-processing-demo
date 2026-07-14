// Workflow code must be deterministic: no I/O, no clocks, no randomness.
// Anything that touches the outside world is delegated to an activity, so
// Temporal can replay this function from history and arrive at the exact
// same state after a crash or worker restart.
import { proxyActivities, defineQuery, setHandler, workflowInfo } from '@temporalio/workflow';
import type * as activities from './activities';
import type { Order, OrderStatus } from './shared';

// Query: lets anyone (CLI, support tooling, a dashboard) ask a live or
// completed order "where are you?" without touching the workflow's state.
export const getStatus = defineQuery<OrderStatus>('getStatus');

const {
  authorizePayment,
  reserveInventory,
  createShipment,
  capturePayment,
  sendConfirmation,
  releaseInventory,
  voidPaymentAuthorization,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 seconds',
  // The retry policy is server-side config, not workflow code: transient
  // failures (flaky-inventory) are absorbed here with ZERO lines of
  // try/catch. 1s initial + 2x backoff makes each retry watchable live in
  // worker logs and the Web UI; maximumAttempts caps the demo so a truly
  // broken service fails the activity instead of retrying forever
  // (production would typically retry longer, or indefinitely).
  retry: {
    initialInterval: '1 second',
    backoffCoefficient: 2,
    maximumAttempts: 5,
  },
});

export async function orderWorkflow(order: Order): Promise<OrderStatus> {
  let status: OrderStatus = 'RECEIVED';
  setHandler(getStatus, () => status);

  // Compensation stack: after each successful step, push its undo. If a later
  // step fails permanently, pop in REVERSE order — undo the most recent work
  // first, like unwinding nested transactions (releasing inventory can matter
  // to other orders immediately; voiding the auth is the final "no harm done").
  // No Saga class, no framework — a plain array is the whole mechanism.
  const compensations: Array<() => Promise<unknown>> = [];

  // The idempotency key is derived from the workflow id INSIDE the workflow:
  // it's deterministic, so it survives activity retries, worker crashes, and
  // history replay — the payment service sees the same key every time, which
  // is what makes "retry aggressively" safe for money movement.
  const idempotencyKey = `pay-${workflowInfo().workflowId}`;

  // TEMPORAL VERSIONING
  // If this pipeline changed in production (e.g. a fraud check inserted here),
  // we'd gate the new code with patched('fraud-check') so orders started
  // before the deploy still replay their old histories deterministically.

  try {
    const authId = await authorizePayment(order, idempotencyKey);
    status = 'PAYMENT_AUTHORIZED';
    compensations.push(() => voidPaymentAuthorization(order, authId));

    const reservationId = await reserveInventory(order);
    status = 'INVENTORY_RESERVED';
    compensations.push(() => releaseInventory(order, reservationId));

    await createShipment(order);
    status = 'SHIPPED';

    await capturePayment(order, authId, idempotencyKey);
    status = 'PAYMENT_CAPTURED';

    await sendConfirmation(order);
    status = 'COMPLETED';
    return status;
  } catch {
    // Only PERMANENT failures land here: transient errors are absorbed by the
    // retry policy above and non-retryable ApplicationFailures skip it.
    // Production notes: compensations are activities, so each undo below gets
    // the same retry policy as forward steps, and each is idempotent — the two
    // requirements for a safe saga. (Production would also inspect the error
    // type before deciding to compensate vs. fail loudly.)
    status = 'COMPENSATING';
    for (const undo of compensations.reverse()) {
      await undo();
    }
    // The workflow COMPLETES (with a business status), it doesn't fail: the
    // saga did its job — no duplicate charge, no stranded reservation.
    status = 'FAILED_COMPENSATED';
    return status;
  }
}
