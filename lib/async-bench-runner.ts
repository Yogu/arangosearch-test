import { benchmark, BenchmarkFactories, time } from './async-bench.js';
import chalk from 'chalk';

const SHOW_CYCLE_INFO = false;

function formatTimings({
    meanTime,
    relativeMarginOfError,
}: {
    meanTime: number;
    relativeMarginOfError: number;
}) {
    return `${(meanTime * 1000).toFixed(3)}ms (±${(relativeMarginOfError * 100).toFixed(2)}%)`;
}

function formatElapsedTime({ elapsedTime, setUpTime }: { elapsedTime: number; setUpTime: number }) {
    return `${elapsedTime.toFixed()}s elapsed (${(
        (setUpTime / elapsedTime) *
        100
    ).toFixed()}% for setup)`;
}

interface BenchmarkSuiteResult {
    hasErrors: boolean;
}

export async function runBenchmarks(factories: BenchmarkFactories): Promise<BenchmarkSuiteResult> {
    const startTime = time();
    console.log('');
    console.log('Running benchmark suite');
    let index = 1;
    let erroredCount = 0;
    for (const factory of factories) {
        const config = factory();
        console.log('');
        console.log(chalk.yellow(chalk.bold(`[${index} / ${factories.length}] ${config.name}...`)));
        try {
            const result = await benchmark(config, {
                onCycleDone: (cycle) => {
                    if (SHOW_CYCLE_INFO) {
                        console.log(
                            chalk.grey(
                                `  Cycle ${cycle.index + 1}: ${cycle.iterationCount} iterations, ` +
                                    `current estimate: ${formatTimings(
                                        cycle.timingsSoFar,
                                    )} per iteration, ` +
                                    `${formatElapsedTime(cycle)}`,
                            ),
                        );
                    }
                },
            });
            console.log(chalk.green(`  ${formatTimings(result)}`) + ` per iteration`);
            console.log(
                `  ${formatElapsedTime(result)} for ${result.iterationCount} iterations in ${
                    result.cycles
                } cycles`,
            );
        } catch (err: any) {
            console.error(err.message, err.stack);
            erroredCount++;
        }
        index++;
    }

    const elapsed = time() - startTime;
    const elapsedMinutes = Math.floor(elapsed / 60);
    const elapsedSeconds = Math.floor(elapsed % 60);
    console.log('');
    console.log(chalk.bold(`Done.`));
    console.log(
        chalk.bold(
            `Executed ${factories.length} benchmarks in ${elapsedMinutes} minutes, ${elapsedSeconds} seconds`,
        ),
    );
    if (erroredCount) {
        console.log(chalk.red(chalk.bold(`${erroredCount} benchmarks reported an error.`)));
    }
    console.log('');
    return {
        hasErrors: erroredCount > 0,
    };
}
