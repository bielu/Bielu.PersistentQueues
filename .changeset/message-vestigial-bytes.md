---
"bielu-persistentqueues": patch
---

`Message.WithBytes(...)` is now `[Obsolete]`, and the `bytes` parameter of the `Message` constructor is documented as ignored. `Message` stopped storing raw serialized bytes in an earlier upstream change, so both have been no-ops since then. They stay for 1.x binary compatibility and will be removed in the next major version.
