import { aql, Database } from 'arangojs';
import { setTimeout } from 'node:timers/promises';
import dotenv from 'dotenv';

dotenv.config({ path: '../.env' });

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
                },
            },
        },
        commitIntervalMsec: 1000,
        consolidationIntervalMsec: 1000,
    });

    console.log(`Inserting ${INITIAL_COUNT} initial documents...`);
    await insert(1_000_000);
    console.log('Done.');
}

async function insert(count = 1) {
    await database.query(aql`
        FOR index in RANGE(1, ${count})
        INSERT { field1: RANDOM_TOKEN(60), field2: RANDOM_TOKEN(60), field3: RANDOM_TOKEN(60), field4: RANDOM_TOKEN(60), field5: RANDOM_TOKEN(60) } IN ${collection}
    `);
}

async function run() {
    let counter = 0;
    let lastLogTime = performance.now();
    while (true) {
        await insert(1);
        await setTimeout(5000);
        counter++;
        if (performance.now() - lastLogTime > 1000) {
            console.log(`Inserted ${counter} documents`);
            lastLogTime = performance.now();
        }
    }
}

async function main() {
    //await setup();
    await run();
}

main().catch((err) => console.error(err));
