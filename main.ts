import { aql, Database } from 'arangojs';
import { setTimeout } from 'node:timers/promises';
import dotenv from 'dotenv';
import { ArangoSearchViewLinkOptions, ArangoSearchViewProperties, View } from 'arangojs/views';
import { runComparisons } from './lib/compare-runner.js';
import { BenchmarkConfig } from './lib/async-bench.js';

dotenv.config();

const COLLECTION_NAME = 'test';

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

const collection = database.collection(COLLECTION_NAME);
const view1 = database.view(COLLECTION_NAME + '_view');
const view2 = database.view(COLLECTION_NAME + '_view2');
const views = [view1, view2];

const links: Record<string, Omit<ArangoSearchViewLinkOptions, 'nested'>> = {
    [COLLECTION_NAME]: {
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
};

async function setup() {
    if (await collection.exists()) {
        await collection.drop();
    }
    await collection.create();
    await collection.ensureIndex({ type: 'persistent', fields: ['gauge'] });

    if (await view1.exists()) {
        await view1.drop();
    }
    await view1.create({
        type: 'arangosearch',
        links,
        commitIntervalMsec: 1000,
        consolidationIntervalMsec: 1000,
        consolidationPolicy: {
            type: 'tier',
            segmentsBytesMax: 3_000_000,
        },
    });

    if (await view2.exists()) {
        await view2.drop();
    }
    await view2.create({
        type: 'arangosearch',
        links,
        commitIntervalMsec: 1000,
        consolidationIntervalMsec: 1000,
        consolidationPolicy: {
            type: 'tier',
            segmentsMin: 3,
        },
    });

    await reInitData();
}

async function reInitData() {
    // reduce commit interval so the resulting layout of segments is closer to
    // how it would be if the inserts were not done in bulk
    let oldProps = new Map<View, ArangoSearchViewProperties>();
    for (const view of views) {
        const props = (await view.properties()) as ArangoSearchViewProperties;
        oldProps.set(view, props);
        await view.updateProperties({
            commitIntervalMsec: 10,
        });
    }

    try {
        await collection.truncate();
        const batches = 10000;
        const countPerPatch = 1000;
        console.log(
            `Inserting ${batches * countPerPatch} initial documents in ${batches} batches...`,
        );
        for (let i = 0; i < batches; i++) {
            await insert(countPerPatch);
            await setTimeout(10);
        }
    } finally {
        for (const view of views) {
            const props = oldProps.get(view);
            await view.updateProperties({
                commitIntervalMsec: props!.commitIntervalMsec,
            });
        }
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

async function testQueryPerformance(fn: PerfQueryFn) {
    const options: Partial<BenchmarkConfig> = {
        maxTime: 3, // times 3 because of 3 cycles
        initialCount: 10,
    };
    await runComparisons([
        {
            name: fn.name + ', view1 (segmentsBytesMax), no parallelism',
            fn: () => fn({ view: view1, parallelism: 1 }),
            ...options,
        },
        {
            name: fn.name + ', view1 (segmentsBytesMax), parallelism = 16',
            fn: () => fn({ view: view1, parallelism: 16 }),
            ...options,
        },
        {
            name: fn.name + ', view2 (segmentsMin), no parallelism',
            fn: () => fn({ view: view2, parallelism: 1 }),
            ...options,
        },
        {
            name: fn.name + ', view2 (segmentsMin), parallelism = 16',
            fn: () => fn({ view: view2, parallelism: 16 }),
            ...options,
        },
    ]);
}

interface PerfQueryOptions {
    readonly view: View;
    readonly parallelism: number;
}

type PerfQueryFn = (options: PerfQueryOptions) => Promise<number>;

async function perfQueryCount({ view, parallelism }: PerfQueryOptions) {
    const res = await database.query(
        aql`for a in ${view} search a.category == 0 options { parallelism: ${parallelism} } collect with count into c return c`,
        { profile: true },
    );
    return (res.extra.profile as any).executing;
}

async function perfQueryFind({ view, parallelism }: PerfQueryOptions) {
    const targetGauge = Math.random() * 0.9; // don't go up to 1 because we might not find any then
    const result = await database.query(aql`
        FOR doc IN ${collection} FILTER doc.gauge > ${targetGauge} SORT doc.gauge ASC LIMIT 0, 1 RETURN doc
    `);
    const docs = await result.all();
    if (!docs.length) {
        throw new Error(`Did not find doc with gauge ${targetGauge}`);
    }
    const targetField1 = docs[0].field1;
    const res = await database.query(
        aql`for a in ${view} search a.field1 == ${targetField1} options { parallelism: ${parallelism} } return a.field1`,
        { profile: true },
    );
    const foundItems = await res.all();
    if (!foundItems.length) {
        throw new Error(`arangosearch failed to find doc with field1 == ${targetField1}`);
    }
    if (foundItems[0] !== targetField1) {
        throw new Error(
            `arangosearch found doc with field1 == ${foundItems[0]} but should have been field1 = ${targetField1}`,
        );
    }
    return (res.extra.profile as any).executing;
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
    //await reInitData();
    await run();

    //await testQueryPerformance(perfQueryCount);
    //await testQueryPerformance(perfQueryFind);
}

main().catch((err) => console.error(err));
