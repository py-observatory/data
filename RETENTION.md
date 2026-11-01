# Retention

- Create-once assets are never replaced or deleted.
- Only stale `swap-next-`/`swap-prev-` copies in a `-current` release are ever deleted.
- Any other deletion needs a written owner decision, recorded in this file.
- No asset is ever uploaded with `--clobber`; an asset in a `-current` release is replaced only through its `swap-next-`/`swap-prev-` names.
