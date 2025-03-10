import { BenchmarkConfig, time } from './async-bench.ts';
import { runComparison } from './compare.ts';
import chalk from 'chalk';

const SHOW_CYCLE_INFO = true;

function formatMs(seconds: number) {
    return `${(seconds * 1000).toFixed(3)}ms`;
}

function formatPercent(fraction: number) {
    return `${(fraction * 100).toFixed(2)}%`;
}

function formatTimings({
    meanTime,
    relativeMarginOfError,
}: {
    meanTime: number;
    relativeMarginOfError: number;
}) {
    return `${formatMs(meanTime)} (±${formatPercent(relativeMarginOfError)})`;
}

function formatElapsedTime({ elapsedTime, setUpTime }: { elapsedTime: number; setUpTime: number }) {
    return `${elapsedTime.toFixed()}s elapsed (${((setUpTime / elapsedTime) * 100).toFixed()}% test overhead)`;
}

function formatOverhead(x: {
    overheadMin: number;
    relativeOverheadMin: number;
    overheadMax: number;
    relativeOverheadMax: number;
}) {
    return `${formatMs(x.overheadMin)} \u2013 ${formatMs(x.overheadMax)} (${formatPercent(x.relativeOverheadMin)} \u2013 ${formatPercent(x.relativeOverheadMax)})`;
}

interface BenchmarkSuiteResult {
    hasErrors: boolean;
}

export async function runComparisons(benchmarks: BenchmarkConfig[]): Promise<BenchmarkSuiteResult> {
    const startTime = time();
    console.log('');
    console.log('Running comparison suite');
    let index = 1;
    let erroredCount = 0;

    const result = await runComparison(benchmarks, {
        onCycleDone: (cycle) => {
            if (SHOW_CYCLE_INFO) {
                console.log(
                    chalk.grey(
                        `  Cycle ${cycle.index + 1} of ${cycle.name}: ${cycle.iterationCount} iterations, ` +
                            `current estimate: ${formatTimings(cycle.timingsSoFar)} per iteration, ` +
                            `${formatElapsedTime(cycle)}`,
                    ),
                );
            }
        },
    });

    for (const candidate of result.candidates) {
        console.log('');
        console.log(
            chalk.yellow.bold(`[${index} / ${benchmarks.length}] ${candidate.config.name}...`),
        );
        console.log(chalk.green(`  ${formatTimings(candidate.benchmark)}`) + ` per iteration`);
        console.log(
            `  ${formatElapsedTime(candidate.benchmark)} for ${candidate.benchmark.iterationCount} iterations in ${candidate.benchmark.cycles} cycles`,
        );
        if (candidate.isFastest) {
            console.log(chalk.green.bgBlack(`  Fastest result.`));
        } else {
            console.log(
                chalk.yellow.bgBlack(`  Slower than fastest by ${formatOverhead(candidate)}`),
            );
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
            `Executed ${benchmarks.length} benchmarks in ${elapsedMinutes} minutes, ${elapsedSeconds} seconds`,
        ),
    );
    if (erroredCount) {
        console.log(chalk.red.bold(`${erroredCount} benchmarks reported an error.`));
    }
    console.log('');
    return {
        hasErrors: erroredCount > 0,
    };
}
