import { LogicalReplicationService, Wal2JsonPlugin } from 'pg-logical-replication';
import Queue from 'queue';
import * as pg from 'pg';

const slotName = 'test_slot_wal2json';

const connection = {
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'postgres',
  port: 5432,
};

const q = new Queue({ concurrency: 1, autostart: true });
let stopping = false;

const service = new LogicalReplicationService(
  /**
   * node-postgres Client options for connection
   * https://github.com/DefinitelyTyped/DefinitelyTyped/blob/master/types/pg/index.d.ts#L16
   */
  connection,
  /**
   * Logical replication service config
   * https://github.com/kibae/pg-logical-replication/blob/main/src/logical-replication-service.ts#L9
   */
  {
    acknowledge: {
      auto: true,
      timeoutSeconds: 10
    }
  }
)

// `TestDecodingPlugin` for test_decoding and `ProtocolBuffersPlugin` for decoderbufs are also available.
const plugin = new Wal2JsonPlugin({
  /**
   * Plugin options for wal2json
   * https://github.com/kibae/pg-logical-replication/blob/main/src/output-plugins/wal2json/wal2json-plugin-options.type.ts
   */
  //...
  includeTypes: false,
  addTables: 'public.outbox',
  actions: 'insert',
  includeTimestamp: true,
});

async function addItem(table: string, id: string) {
  const client = new pg.Client(connection);
  await client.connect();

  const text = `INSERT INTO ${table} (aggregate_id, aggregate_type, payload) VALUES ($1, $2, $3)`;
  const values = [id, 'user', { name: 'John Doe', table }];

  await client.query(text, values);

  await client.end();
}

async function demoData() {
  await addItem('outbox', 'START');
  await new Promise((resolve) => setTimeout(resolve, 2000));

  let i = 0;
  while (true) {
    await addItem('outbox', (++i).toString());
    await addItem('junk', (++i).toString());
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

function exponentialBackoff(i: number, rate = 1): Promise<number> {
  const incremented = i + Math.pow(2, Math.random() * rate);
  return new Promise((resolve) => setTimeout(() => resolve(incremented), incremented));
}


async function complete() {
  if (stopping) {
    return;
  }
  stopping = true;

  service.stop()

  let i = 0;
  while (!service.isStop() || q.length) {
    i = await exponentialBackoff(i);
  }

  process.exit(0);
}

/**
 * Wal2Json.Output
 * https://github.com/kibae/pg-logical-replication/blob/ts-main/src/output-plugins/wal2json/wal2json-plugin-output.type.ts
 */
service.on('data', (lsn, log: any) => {
  // Do something what you want.
  const inserts = log.change.filter((change: any) => change.kind === 'insert')

  if (!inserts.length) {
    return;
  }

  q.push(() => new Promise(async (resolve) => {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    console.log('Processed:', log.change);
    resolve(null);
  }));

  if (new Date(log.timestamp).getTime() > Date.now() - 1000 * 60) {
    complete();
  }
});

// Start subscribing to data change events.
(async function proc() {
  demoData(); // No Await to run in parallel

  service.subscribe(plugin, slotName)
    .catch((e) => {
      console.error(e);
    })
    .then(() => {
      setTimeout(proc, 100);
    });

  await Promise.resolve(setTimeout(() => process.exit(1), 900000));
})();
