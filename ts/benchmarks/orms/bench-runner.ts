/**
 * Comprehensive ORM Benchmark Runner
 * Runs all three ORMs and generates a detailed comparison report
 */

import { exec } from "child_process";
import { promisify } from "util";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const execAsync = promisify(exec);

// ============================================================================
// Types
// ============================================================================

interface BenchmarkResult {
  name: string;
  mean: number;
  variance: number;
  rme: number;
  samples: number;
  min: number;
  max: number;
}

interface ORMResults {
  orm: string;
  results: BenchmarkResult[];
  totalTime: number;
}

// ============================================================================
// Results Parsing
// ============================================================================

function extractResults(output: string, orm: string): BenchmarkResult[] {
  const results: BenchmarkResult[] = [];
  const lines = output.split("\n");

  let inResults = false;
  for (const line of lines) {
    if (line.includes("📊") || line.includes("Results:")) {
      inResults = true;
      continue;
    }

    if (!inResults || line.includes("═") || line === "") continue;

    // Parse: "Name                       mean(ms)     (rme%)"
    const match = line.match(/^(.+?)\s+([\d.]+)ms\s+\(([\d.]+)%\)$/);
    if (match) {
      results.push({
        name: match[1].trim(),
        mean: parseFloat(match[2]),
        variance: 0,
        rme: parseFloat(match[3]),
        samples: 0,
        min: 0,
        max: 0,
      });
    }
  }

  return results;
}

// ============================================================================
// Comparison Report
// ============================================================================

function generateComparisonReport(
  allResults: ORMResults[]
): string {
  const report: string[] = [];

  report.push("\n");
  report.push("╔════════════════════════════════════════════════════════════════════════╗");
  report.push("║                  ORM BENCHMARK COMPARISON REPORT                       ║");
  report.push("║                   Neutron vs Drizzle vs Prisma                        ║");
  report.push("╚════════════════════════════════════════════════════════════════════════╝");
  report.push("");

  // Get unique benchmark names
  const benchmarkNames = Array.from(
    new Set(
      allResults
        .flatMap((r) => r.results)
        .map((r) => r.name)
    )
  );

  // Summary table
  report.push("📊 PERFORMANCE SUMMARY");
  report.push("─".repeat(90));
  report.push(
    "Benchmark".padEnd(35) +
      "Neutron".padEnd(20) +
      "Drizzle".padEnd(20) +
      "Prisma".padEnd(15)
  );
  report.push("─".repeat(90));

  const ormMap = new Map(allResults.map((r) => [r.orm, r.results]));

  for (const benchmark of benchmarkNames) {
    const neutronResult = ormMap.get("Neutron")?.find((r) => r.name === benchmark);
    const drizzleResult = ormMap.get("Drizzle")?.find((r) => r.name === benchmark);
    const prismaResult = ormMap.get("Prisma")?.find((r) => r.name === benchmark);

    const neutronStr = neutronResult ? `${neutronResult.mean.toFixed(4)}ms` : "N/A";
    const drizzleStr = drizzleResult ? `${drizzleResult.mean.toFixed(4)}ms` : "N/A";
    const prismaStr = prismaResult ? `${prismaResult.mean.toFixed(4)}ms` : "N/A";

    report.push(
      benchmark.padEnd(35) +
        neutronStr.padEnd(20) +
        drizzleStr.padEnd(20) +
        prismaStr.padEnd(15)
    );
  }

  report.push("─".repeat(90));

  // Winner analysis
  report.push("");
  report.push("🏆 WINNERS BY CATEGORY");
  report.push("─".repeat(90));

  for (const benchmark of benchmarkNames) {
    const results = [
      { orm: "Neutron", result: ormMap.get("Neutron")?.find((r) => r.name === benchmark) },
      { orm: "Drizzle", result: ormMap.get("Drizzle")?.find((r) => r.name === benchmark) },
      { orm: "Prisma", result: ormMap.get("Prisma")?.find((r) => r.name === benchmark) },
    ].filter((r) => r.result);

    if (results.length > 1) {
      const fastest = results.reduce((a, b) =>
        (a.result?.mean ?? Infinity) < (b.result?.mean ?? Infinity) ? a : b
      );

      const margin = results.length > 1
        ? ((results[1].result?.mean ?? 0) / (fastest.result?.mean ?? 1) - 1) * 100
        : 0;

      report.push(
        `${benchmark.padEnd(35)} 🥇 ${fastest.orm.padEnd(10)} ${margin.toFixed(1)}% faster`
      );
    }
  }

  // Aggregate analysis
  report.push("");
  report.push("📈 AGGREGATE STATISTICS");
  report.push("─".repeat(90));

  for (const orm of allResults) {
    const avgLatency = orm.results.reduce((sum, r) => sum + r.mean, 0) / orm.results.length;
    const slowest = Math.max(...orm.results.map((r) => r.mean));
    const fastest = Math.min(...orm.results.map((r) => r.mean));

    report.push(`${orm.orm.padEnd(15)}`);
    report.push(`  Average Latency: ${avgLatency.toFixed(4)}ms`);
    report.push(`  Fastest Query:   ${fastest.toFixed(4)}ms`);
    report.push(`  Slowest Query:   ${slowest.toFixed(4)}ms`);
    report.push(`  Range:           ${(slowest - fastest).toFixed(4)}ms`);
    report.push("");
  }

  // Key insights
  report.push("");
  report.push("💡 KEY INSIGHTS");
  report.push("─".repeat(90));

  const neutronResults = ormMap.get("Neutron") || [];
  const drizzleResults = ormMap.get("Drizzle") || [];
  const prismaResults = ormMap.get("Prisma") || [];

  // Simple queries
  const simpleQuery = neutronResults.find((r) => r.name === "Simple SELECT by ID");
  if (simpleQuery) {
    const drizzleSimple = drizzleResults.find((r) => r.name === "Simple SELECT by ID");
    const prismaSimple = prismaResults.find((r) => r.name === "Simple SELECT by ID");

    report.push("• SIMPLE QUERIES (SELECT by ID):");
    report.push(`  Neutron: ${simpleQuery.mean.toFixed(4)}ms`);
    report.push(`  Drizzle: ${drizzleSimple?.mean.toFixed(4) || "N/A"}ms`);
    report.push(`  Prisma:  ${prismaSimple?.mean.toFixed(4) || "N/A"}ms`);
    report.push("");
  }

  // Complex queries
  const complexQuery = neutronResults.find((r) => r.name.includes("tags and comments"));
  if (complexQuery) {
    const drizzleComplex = drizzleResults.find((r) => r.name.includes("tags and comments"));
    const prismaComplex = prismaResults.find((r) => r.name.includes("tags and comments"));

    report.push("• COMPLEX QUERIES (Multi-table JOINs):");
    report.push(`  Neutron: ${complexQuery.mean.toFixed(4)}ms`);
    report.push(`  Drizzle: ${drizzleComplex?.mean.toFixed(4) || "N/A"}ms`);
    report.push(`  Prisma:  ${prismaComplex?.mean.toFixed(4) || "N/A"}ms`);
    report.push("");
  }

  // Writes
  const insertUser = neutronResults.find((r) => r.name === "INSERT user");
  if (insertUser) {
    const drizzleInsert = drizzleResults.find((r) => r.name === "INSERT user");
    const prismaInsert = prismaResults.find((r) => r.name === "INSERT user");

    report.push("• WRITE OPERATIONS (INSERT):");
    report.push(`  Neutron: ${insertUser.mean.toFixed(4)}ms`);
    report.push(`  Drizzle: ${drizzleInsert?.mean.toFixed(4) || "N/A"}ms`);
    report.push(`  Prisma:  ${prismaInsert?.mean.toFixed(4) || "N/A"}ms`);
    report.push("");
  }

  // Neutron multi-model advantage
  const kvCache = neutronResults.find((r) => r.name.includes("KV cache"));
  if (kvCache) {
    report.push("• NEUTRON MULTI-MODEL ADVANTAGE:");
    report.push(`  KV + SQL combo operation: ${kvCache.mean.toFixed(4)}ms`);
    report.push("  ✓ Unique to Neutron (Drizzle/Prisma require separate cache layer)");
    report.push("");
  }

  report.push("═".repeat(90));
  report.push("");

  return report.join("\n");
}

// ============================================================================
// Main Runner
// ============================================================================

async function runBenchmarks() {
  console.log("\n🚀 Starting ORM Benchmark Suite...\n");

  const allResults: ORMResults[] = [];

  // Run Neutron benchmark
  console.log("⏱️  Testing Neutron ORM...");
  try {
    const { stdout: neutronOutput } = await execAsync(`node --loader tsx ./neutron-bench.ts`, {
      cwd: __dirname,
      timeout: 60000,
    });
    const neutronResults = extractResults(neutronOutput, "Neutron");
    allResults.push({
      orm: "Neutron",
      results: neutronResults,
      totalTime: 0,
    });
    console.log(`✓ Neutron benchmark complete (${neutronResults.length} tests)\n`);
  } catch (error) {
    console.error("✗ Neutron benchmark failed:", error);
  }

  // Run Drizzle benchmark
  console.log("⏱️  Testing Drizzle ORM...");
  try {
    const { stdout: drizzleOutput } = await execAsync(`node --loader tsx ./drizzle-bench.ts`, {
      cwd: __dirname,
      timeout: 60000,
    });
    const drizzleResults = extractResults(drizzleOutput, "Drizzle");
    allResults.push({
      orm: "Drizzle",
      results: drizzleResults,
      totalTime: 0,
    });
    console.log(`✓ Drizzle benchmark complete (${drizzleResults.length} tests)\n`);
  } catch (error) {
    console.error("✗ Drizzle benchmark failed:", error);
  }

  // Run Prisma benchmark
  console.log("⏱️  Testing Prisma ORM...");
  try {
    const { stdout: prismaOutput } = await execAsync(`node --loader tsx ./prisma-bench.ts`, {
      cwd: __dirname,
      timeout: 60000,
    });
    const prismaResults = extractResults(prismaOutput, "Prisma");
    allResults.push({
      orm: "Prisma",
      results: prismaResults,
      totalTime: 0,
    });
    console.log(`✓ Prisma benchmark complete (${prismaResults.length} tests)\n`);
  } catch (error) {
    console.error("✗ Prisma benchmark failed:", error);
  }

  // Generate comparison report
  const report = generateComparisonReport(allResults);
  console.log(report);

  // Save report to file
  const reportPath = path.join(__dirname, "benchmark-report.txt");
  fs.writeFileSync(reportPath, report);
  console.log(`📄 Full report saved to: ${reportPath}\n`);
}

runBenchmarks().catch(console.error);
