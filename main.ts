import { aql, Database } from 'arangojs';
import { setTimeout } from 'node:timers/promises';
import dotenv from 'dotenv';

dotenv.config();

const database = new Database({
    url: process.env.DATABASE_URL ?? 'http://localhost:8529',
    auth: process.env.AUTH_TOKEN
        ? {
              token: process.env.AUTH_TOKEN,
          }
        : {
              username: process.env.DATABASE_USERNAME ?? 'root',
              password: process.env.DATABASE_PASSWORD ?? '',
          },
    databaseName: process.env.DATABASE_NAME ?? 'test',
});

const collection = database.collection('test');
const VIEW_NAME = 'test_view';
const INITIAL_COUNT = 1_000_000;

async function setup() {
    if (await collection.exists()) {
        await collection.drop();
    }
    await collection.create();
    await collection.ensureIndex({ type: 'persistent', fields: ['gauge'] });

    const view = database.view(VIEW_NAME);
    if (await view.exists()) {
        await view.drop();
    }

    await view.create({
        type: 'arangosearch',
        links: {
            test: {
                fields: {
                    field1: {
                        analyzers: ['identity'],
                    },
                    field2: {
                        analyzers: ['identity'],
                    },
                    field3: {
                        analyzers: ['identity'],
                    },
                    field4: {
                        analyzers: ['identity'],
                    },
                    field5: {
                        analyzers: ['identity'],
                    },
                    category: {
                        analyzers: ['identity'],
                    },
                },
            },
        },
        commitIntervalMsec: 1000,
        consolidationIntervalMsec: 1000,
        consolidationPolicy: {
            type: 'tier',
            segmentsMax: 3_000_000,
        },
        /*consolidationPolicy: {
            type: 'tier',
            segmentsMin: 3,
            segmentsBytesFloor: 500000
        }*/
    });

    await reInitData();
}

async function reInitData() {
    await collection.truncate();
    console.log(`Inserting ${INITIAL_COUNT} initial documents...`);
    for (let i = 0; i < 1000; i++) {
        await insert(1000);
        await setTimeout(10);
    }
    console.log('Done.');
}

async function insert(count = 1) {
    await database.query(aql`
        FOR index in RANGE(1, ${count})
        INSERT { field1: RANDOM_TOKEN(60), field2: RANDOM_TOKEN(60), field3: RANDOM_TOKEN(60), field4: RANDOM_TOKEN(60), field5: RANDOM_TOKEN(60), category: FLOOR(RAND() * 8), gauge: RAND() } IN ${collection}
    `);
}

async function remove(count = 1) {
    // remove random slice of documents
    // the "gauge" field is evenly distributed between 0 and 1
    // there is a regular (persistent) index on gauge, so we can cheaply do this
    const targetGauge = Math.random();
    await database.query(aql`
        FOR doc IN ${collection} FILTER doc.gauge > ${targetGauge} SORT doc.gauge ASC LIMIT 0, ${count} REMOVE doc IN ${collection}
    `);
}

async function run() {
    let totalInserts = 0;
    let totalRemovals = 0;
    while (true) {
        const inserts = Math.round(10 ** (Math.random() * 2));
        const removals = Math.round(10 ** (Math.random() * 2));
        const delay = Math.round(10 ** (Math.random() * 4));
        await insert(inserts);
        await remove(inserts);
        totalInserts += inserts;
        totalRemovals += removals;
        console.log(
            `Inserted ${String(inserts).padStart(4)} documents, removed ${String(removals).padStart(
                4,
            )} docs (${totalInserts} inserts in total, ${totalRemovals} removals in total), waiting ${delay} ms`,
        );
        await setTimeout(delay);
    }
}

async function main() {
    await setup();
    await run();
}

main().catch((err) => console.error(err));
