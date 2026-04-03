import { NextResponse } from "next/server";

import { readDashboardEnv } from "@/lib/isolation";

export async function GET() {
  const env = readDashboardEnv();

  return NextResponse.json(
    {
      status: "ok",
      mode: "read_only",
      projectRef: env.projectRef,
      forbiddenHint: env.forbiddenHint,
    },
    {
      status: 200,
      headers: {
        "cache-control": "no-store",
      },
    }
  );
}
