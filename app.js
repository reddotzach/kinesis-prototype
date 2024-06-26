const pg = require('pg');
const { LogicalReplicationService, Wal2JsonPlugin } = require('pg-logical-replication');

const slotName = 'test_slot_wal2json';

const connection = {
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'postgres',
  port: 5432,
};

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
});

/**
 * Wal2Json.Output
 * https://github.com/kibae/pg-logical-replication/blob/ts-main/src/output-plugins/wal2json/wal2json-plugin-output.type.ts
 */
service.on('data', (lsn, log) => {
  // Do something what you want.
  // log.change.filter((change) => change.kind === 'insert').length;
  console.log({ lsn, log: JSON.stringify(log, null, 2) })
});

async function addItem() {
  const client = new pg.Client(connection);
  await client.connect();

  const text = 'INSERT INTO outbox (aggregate_id, aggregate_type, payload) VALUES ($1, $2, $3)';
  const values = ['1', 'user', { name: 'John Doe' }];

  await client.query(text, values);

  await client.end();
}

// Start subscribing to data change events.
(async function proc() {
  service.subscribe(plugin, slotName)
    .catch((e) => {
      console.error(e);
    })
    .then(() => {
      setTimeout(proc, 100);
    });

  while (true) {
    await addItem();
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
})();
