---
"bielu-persistentqueues": patch
---

Publish NuGet packages through nuget.org trusted publishing from this repository's own workflow (the previous stable release failed because the OIDC token was minted inside the shared template), skip beta releases for pushes that change no shipped code, and align repository tooling with Bielu.AspNetCore.AsyncApi: `.editorconfig`, shared `coverlet.runsettings`, `AGENTS.md`, and a daily ClearlyDefined harvest.
