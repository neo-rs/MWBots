## DiscumBot folder

This folder is just a **launcher wrapper** so you can run DiscumBot from a clean, dedicated directory:

- Run: `run_discumbot.bat`
- Or: `python discumbot.py`

The actual implementation runs from `neonxt/bots/discumbot.py`.

### Config files used

DiscumBot loads config from the nearest `config/` folder it can find (searching upward):

- `config/tokens-api.env`
- `config/settings.env`
- `config/channel_map.json`

So you do **not** need to duplicate config into this folder unless you want DiscumBot to be fully self-contained.

