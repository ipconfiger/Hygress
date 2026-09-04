# Rollback (design §11 / DoD 6)

The pristine Higress s6 run scripts live in the official GPUStack image at
`/etc/s6-overlay/s6-rc.d/{pilot,controller,gateway}/run`. To keep a rollback switch,
copy them into this repo at image-build time under `s6-rc.d.dist/` (they are not shipped
in this repo because they are GPUStack image content, not Hygress content).

Rollback procedure:
1. Restore `s6-rc.d.dist/{pilot,controller,gateway}/run` over the surgery scripts.
2. Restore the original `supercronic/run` (re-add the `readinessCheck "Higress Pilot" 15010`).
3. Keep Hygress installed but unused (or remove the image layer).
Rebuild the image. This returns the embedded Higress trio exactly as upstream ships it.
