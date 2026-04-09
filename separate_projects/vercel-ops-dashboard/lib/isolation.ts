export type DashboardEnv = {
  supabaseUrl: string;
  supabaseAnonKey: string;
  projectRef: string;
  forbiddenHint: string;
};

function required(name: string): string {
  const value = process.env[name];
  if (!value || value.trim().length === 0) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value.trim();
}

function containsForbidden(value: string, forbiddenHint: string): boolean {
  return value.toLowerCase().includes(forbiddenHint.toLowerCase());
}

export function readDashboardEnv(): DashboardEnv {
  const env: DashboardEnv = {
    supabaseUrl: required("OPSBOT_SUPABASE_URL"),
    supabaseAnonKey: required("OPSBOT_SUPABASE_ANON_KEY"),
    projectRef: required("OPSBOT_SUPABASE_PROJECT_REF"),
    forbiddenHint: (process.env.OPSBOT_FORBIDDEN_PROJECT_HINT ?? "legacy_external_project").trim(),
  };

  if (env.forbiddenHint.length > 0) {
    if (containsForbidden(env.supabaseUrl, env.forbiddenHint)) {
      throw new Error(
        `Isolation guardrail: OPSBOT_SUPABASE_URL appears to reference '${env.forbiddenHint}'. ` +
          "Use a new, separate project."
      );
    }
    if (containsForbidden(env.projectRef, env.forbiddenHint)) {
      throw new Error(
        `Isolation guardrail: OPSBOT_SUPABASE_PROJECT_REF appears to reference '${env.forbiddenHint}'. ` +
          "Use a new, separate project."
      );
    }
  }

  return env;
}
