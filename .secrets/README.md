# Local Secrets

Store local credentials and private keys in this directory only.

- Keep files here out of version control (`.gitignore` excludes everything except this README).
- Use strict permissions:
  - `chmod 700 .secrets`
  - `chmod 600 .secrets/*`
- Point `KALSHI_PRIVATE_KEY_PATH` in `data/research/account_onboarding.local.env` to your key file here.

Do not paste API tokens or passwords into committed files.
