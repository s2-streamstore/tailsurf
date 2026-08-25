---
"@tailsurf/protocol": minor
"@tailsurf/client": minor
---

Add the required `web_origin` field to stream creation and link creation responses. `createLink` now returns a `CreateLinkResponse` carrying `webOrigin` alongside the credential, and `CreateStreamResponse` exposes `webOrigin`. Clients present stream links against the server-supplied origin, so no separate web-URL configuration is needed. Requires a server that returns `web_origin` when minting link credentials.
