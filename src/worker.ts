// The worker hosts both workflow and activity code and long-polls the task
// queue. It is stateless: kill it mid-order and a restarted worker resumes
// every in-flight workflow from history — that's the durability demo.
import { NativeConnection, Worker } from '@temporalio/worker';
import * as activities from './activities';
import { TASK_QUEUE } from './shared';

async function run() {
  // The `temporal server start-dev` server. Explicit IPv4 address because
  // "localhost" can resolve to ::1 on macOS while the dev server binds 127.0.0.1.
  const connection = await NativeConnection.connect({ address: '127.0.0.1:7233' });

  const worker = await Worker.create({
    connection,
    taskQueue: TASK_QUEUE,
    // Workflow code is bundled separately (sandboxed for determinism);
    // activities are plain functions registered directly.
    workflowsPath: require.resolve('./workflows'),
    activities,
  });

  console.log(`[worker] listening on task queue "${TASK_QUEUE}" — ctrl-c to stop`);
  await worker.run();
  await connection.close();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
