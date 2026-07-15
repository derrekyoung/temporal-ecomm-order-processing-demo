// Workflow code must be deterministic: no I/O, no clocks, no randomness.
// Anything that touches the outside world is delegated to an activity, so
// Temporal can replay this function from history and arrive at the exact
// same state after a crash or worker restart.
import { proxyActivities, defineQuery, setHandler, workflowInfo } from '@temporalio/workflow';
import type * as activities from './activities';
import type { Order, OrderStatus } from './shared';

// Query: lets anyone (CLI, support tooling, a dashboard) ask a live or
// completed order "where are you?" without mutating the workflow's state.
export const getStatus = defineQuery<OrderStatus>('getStatus');

const {
  authorizePayment,
  reserveInventory,
  createShipment,
  capturePayment,
  sendConfirmation,
  releaseInventory,
  voidPaymentAuthorization,
  cancelShipment,
  refundPayment,
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
  // first, like unwinding nested transactions (cancel the shipment, then refund
  // or void the payment, then release inventory... each newest-first). Crucially
  // the undo must MATCH what actually happened: an authorized-but-not-captured
  // payment is voided, a captured one is refunded — different operations, so
  // each forward step registers its own correct reversal. No Saga class, no
  // framework — a plain array is the whole mechanism.
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
  // Or we could use Workflow Versioning if the changes are substantial or we
  // want a cleaner break between old and new code.

  try {
    // 1) Authorize payment to make sure that this customer can fulfill the order.
    const authId = await authorizePayment(order, idempotencyKey);
    status = 'PAYMENT_AUTHORIZED';
    compensations.push(() => voidPaymentAuthorization(order, authId));

    // 2) Reserve inventory for the order.
    const reservationId = await reserveInventory(order);
    status = 'INVENTORY_RESERVED';
    compensations.push(() => releaseInventory(order, reservationId));

    // 3) Create a shipment for the order.
    const trackingId = await createShipment(order);
    status = 'SHIPMENT_CREATED';
    compensations.push(() => cancelShipment(order, trackingId));

    // 4) Capture payment for the order and charge them. Once money has MOVED,
    // the undo is a refund, not a void.
    await capturePayment(order, authId, idempotencyKey);
    status = 'PAYMENT_CAPTURED';

    // A refund is a SEPARATE money movement from the capture, so it gets its
    // own idempotency key — reusing the `pay-` key would let the gateway dedupe
    // the refund against the original charge and silently drop it. Still derived
    // from the workflow id, so it's deterministic and stable across retries and
    // replay: the refund can be retried aggressively without ever paying out twice.
    const refundKey = `refund-${workflowInfo().workflowId}`;
    compensations.push(() => refundPayment(order, authId, refundKey));

    // 5) Send a final confirmation to the customer. This is the one step that
    // must NOT trigger the saga: the order is already paid and shipped, and a
    // failed notification is no reason to refund and cancel a good order. We
    // absorb a permanent failure here (log and move on) so the customer keeps
    // their shipment. Worst case they don't get an email, which support can
    // resend out of band. Everything before this point is transactional; this
    // is best-effort.
    try {
      await sendConfirmation(order);
    } catch {
      console.log(`[notification] confirmation failed for order ${order.id} — order still COMPLETED, notify out of band`);
    }
    status = 'COMPLETED';

    return status;
  } catch {
    // What actually reaches this catch: a non-retryable ApplicationFailure
    // (fails on attempt 1, like the shipment rejection), OR a transient error
    // that exhausted maximumAttempts — the retry policy absorbs transient
    // failures only up to the cap, not forever. (Cancellation would land here
    // too, but this bare catch can't compensate a cancelled workflow: scheduling
    // an activity from a cancelled scope throws immediately, so the unwind would
    // die on its first undo. Production wraps it in CancellationScope.nonCancellable
    // — deliberately out of scope for this demo.)
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
