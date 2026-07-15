// CLI entry point, a thin wrapper around the Temporal client:
//   npm run order -- <scenario>            start an order workflow
import { Client, Connection } from '@temporalio/client';
import { orderWorkflow } from './workflows';
import { TASK_QUEUE, type Order } from './shared';

// Every scenario is a fixed order payload — failure injection rides along on
// the `simulate` field, so demos are deterministic and repeatable.
const SCENARIOS: Record<string, Order> = {
  'happy': {
    id: 'O-1001',
    items: [{ sku: 'SKU-001', quantity: 2 }],
    amount: 99.98,
    simulate: 'none',
  },
  'flaky-inventory': {
    id: 'O-2001',
    items: [{ sku: 'SKU-002', quantity: 1 }],
    amount: 149.5,
    simulate: 'flaky-inventory',
  },
  'shipment-failure': {
    id: 'O-3001',
    items: [{ sku: 'SKU-003', quantity: 3 }],
    amount: 249.99,
    simulate: 'shipment-failure',
  },
};

async function main() {
  const [mode, ...args] = process.argv.slice(2);
  // Explicit IPv4 for the same reason as worker.ts: macOS "localhost" may
  // resolve to ::1, but the dev server binds 127.0.0.1.
  const connection = await Connection.connect({ address: '127.0.0.1:7233' });
  const client = new Client({ connection });

  // Get the scenario from the arguments
  const scenario = args[0];

  // Get the order from the scenario
  const order = scenario ? SCENARIOS[scenario] : undefined;
  if (!order) {
    console.error(`Usage: npm run order -- <${Object.keys(SCENARIOS).join('|')}>`);
    process.exit(1);
  }

  // Generate a unique workflow ID
  const workflowId = `order-${scenario}-${Date.now()}`;

  // Start the workflow
  const handle = await client.workflow.start(orderWorkflow, {
    taskQueue: TASK_QUEUE,
    workflowId,
    args: [order],
  });
  console.log(`Started ${workflowId} (watch it at http://localhost:8233)`);

  const result = await handle.result();
  console.log(`Workflow finished: ${result}`);

  await connection.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
