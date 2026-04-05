import { createClient } from "@supabase/supabase-js";

import { readDashboardEnv } from "@/lib/isolation";

export function createOpsbotSupabaseClient() {
  const env = readDashboardEnv();

  return createClient(env.supabaseUrl, env.supabaseAnonKey, {
    auth: {
      autoRefreshToken: false,
      persistSession: false,
    },
    global: {
      headers: {
        "x-opsbot-readonly": "true",
      },
    },
  });
}
