from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

# NOTE: These are vendored from `neonxt/core/global_filters.py` so MWDataManagerBot
# can be standalone (no `neonxt.*` imports).

# Amazon
AMAZON_LINK_PATTERN = re.compile(
    r"(https?:\/\/(?:www\.)?(?:amazon\.[a-z.]{2,10}|amzn\.to|a\.co)\/[^\s]+|\bB0[A-Z0-9]{8}\b)",
    re.IGNORECASE,
)
AMAZON_ASIN_PATTERN = re.compile(r"\bB0[A-Z0-9]{8}\b", re.IGNORECASE)

# Timestamp / upcoming
TIMESTAMP_PATTERN = re.compile(
    r"(<t:\d+:[a-zA-Z]>|\b(coming\s+soon|goes\s+live|go\s+live)\b|"
    r"\b(drops?|releasing?|launches?)\s+(on|at)\b|\bpre[- ]?order\b|"
    r"\b(available|starts?)\s+(at|on)\b|\bup\s*next\b|\b(in|within)\s+\d+\s*(minutes?|mins?|hours?|hrs?|days?)\b|"
    r"\btomorrow\b|\bnext\s+(week|month|friday|monday|tuesday|wednesday|thursday|saturday|sunday)\b|"
    r"\b(release|movie\s+release|launch)\s+date\b|\bwhen:\s*|\b\d{1,2}:\d{2}\s*(am|pm)\b|"
    r"\b\d{1,2}\/\d{1,2}\/\d{2,4}\b|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{2,4}\b)",
    re.IGNORECASE,
)

# In-store categories
SEASONAL_PATTERN = re.compile(
    r"\b("
    r"christmas|xmas|holiday\s*time|holiday|seasonal|"
    r"halloween|thanksgiving|easter|"
    r"valentine['s]?\s*day|mother['s]?\s*day|father['s]?\s*day|"
    r"new\s*year|black\s*friday|cyber\s*monday|memorial\s*day|independence\s*day|labor\s*day|"
    r"st\.?\s*patrick['s]?\s*day|st\s*patty|st\s*paddy|"
    r"chinese\s*new\s*year|lunar\s*new\s*year|"
    r"presidents?\s*day|presidents?\s*weekend|"
    r"veterans?\s*day|veterans?\s*weekend|"
    r"columbus\s*day|indigenous\s*peoples?\s*day|"
    r"juneteenth|"
    r"fourth\s*of\s*july|4th\s*of\s*july|july\s*4th|"
    r"new\s*years?\s*eve|new\s*years?\s*day|"
    r"groundhog\s*day|"
    r"super\s*bowl|superbowl|"
    r"mardi\s*gras|fat\s*tuesday|"
    r"ash\s*wednesday|"
    r"good\s*friday|"
    r"passover|pesach|"
    r"ramadan|eid|"
    r"hanukkah|chanukah|"
    r"kwanzaa|"
    r"graduation|grad\s*season|"
    r"back\s*to\s*school|bts|"
    r"summer\s*solstice|winter\s*solstice|"
    r"spring\s*break|summer\s*break|"
    r"gingerbread|nutcracker|ornament|winter|snowman|"
    r"pumpkin|spooky|ghost|witch|"
    r"turkey|pilgrim|"
    r"bunny|egg\s*hunt|"
    r"heart|cupid|"
    r"shamrock|leprechaun|"
    r"fireworks|patriotic|"
    r"red\s*white\s*blue|"
    r"green\s*beer|"
    r"dragon|lantern|"
    r"graduation\s*cap|diploma"
    r")\b",
    re.IGNORECASE,
)
SNEAKERS_PATTERN = re.compile(
    r"\b("
    r"nike|jordan|air\s*jordan|aj\d+|"
    r"yeezy|yeezys?|"
    r"adidas|adidas\s*originals|"
    r"reebok|"
    r"puma|"
    r"new\s*balance|nb\d+|"
    r"crocs|crocband|"
    r"vans|vans\s*old\s*skool|"
    r"converse|chuck\s*taylor|all\s*star|"
    r"asics|"
    r"brooks|"
    r"saucony|"
    r"skechers|"
    r"under\s*armour|ua\s*shoes?|"
    r"fila|"
    r"new\s*balance|"
    r"onitsuka\s*tiger|"
    r"veja|"
    r"allbirds|"
    r"hoka|hoka\s*one\s*one|"
    r"on\s*running|on\s*cloud|"
    r"salomon|"
    r"merrell|"
    r"keen|"
    r"timberland|"
    r"dr\.?\s*martens?|doc\s*martens?|"
    r"clarks?|"
    r"ecco|"
    r"birkenstock|"
    r"ugg|"
    r"steve\s*madden|"
    r"keds|"
    r"k-swiss|"
    r"diadora|"
    r"ellesse|"
    r"lacoste|"
    r"toms?|"
    r"tretorn|"
    r"sneakers?|sneaks?|"
    r"shoes?|"
    r"kicks?|"
    r"footwear|"
    r"athletic\s*shoes?|"
    r"running\s*shoes?|"
    r"basketball\s*shoes?|"
    r"tennis\s*shoes?|"
    # Avoid matching TCG "Elite Trainer Box" via bare \btrainer(s)?\b.
    r"(?:running|basketball|tennis)\s+trainers?\b|"
    r"jordans?|"
    r"dunks?|dunk\s*low|dunk\s*high|"
    r"air\s*max|airmax|"
    r"air\s*force|airforce|"
    r"blazer|"
    r"cortez|"
    r"af1|"
    r"sb\s*dunk|"
    r"yeezy\s*\d+|"
    r"boost|"
    r"ultraboost|"
    r"stan\s*smith|"
    r"superstar|"
    r"samba|"
    r"gazelle|"
    r"forum"
    r")\b",
    re.IGNORECASE,
)

# In-store: Nike/Adidas/etc. appear on apparel; gate INSTORE_SNEAKERS behind explicit footwear signals
# unless the post is clearly not footwear (tights/shorts/running suit/Dri-FIT apparel).
_INSTORE_APPAREL_SUPPRESS_SNEAKERS_PATTERN = re.compile(
    r"\b("
    r"dri[-\s]?fit|"
    r"tech\s+fleece|"
    r"(?:running|athletic|gym|yoga|bike|compression)\s+(?:tights?|shorts?|pants|leggings?|top|tank|singlet|suit|jacket|hoodie|bra|shirt|tee)|"
    r"\b(?:tights?|leggings?)\b|"
    r"\bjoggers?\b|"
    r"\bsweatpants?\b|"
    r"\b(?:running|athletic|gym)\s+shorts?\b|"
    r"\bshorts\b|"
    r"\bsports?\s+bras?\b|"
    r"\bsleeveless\b|"
    r"\brunning\s+suit\b|"
    r"\b(?:track|bomber|denim|varsity)\s+jacket\b|"
    r"\btrack\s+jacket\b|"
    r"\bhoodie\b|"
    r"\b(?:full[-\s]?zip|zip[-\s]?up)\b|"
    r"\bvest\b|"
    r"\bwork\s+vest\b|"
    r"\bpadded\b|"
    r"\bspeedsuit\b|"
    r"\bwindrunner\b|"
    r"\b(?:polo|henley|crewneck|pullover|windbreaker)\b"
    r")\b",
    re.IGNORECASE,
)
# Collectibles / toys: "Retro" SKUs and brand lines often still hit athletic brand tokens in SNEAKERS_PATTERN.
_INSTORE_COLLECTIBLE_TOY_SUPPRESS_SNEAKERS_PATTERN = re.compile(
    r"\b("
    r"action\s+figure|action\s+figures|"
    r"marvel\s+legends|star\s+wars|gi\s*joe|g\.?\s*i\.?\s*joe|"
    r"super\s*7|hasbro|mcfarlane|mattel|funko|"
    r"retro\s+collection|vintage\s+collection|"
    r"legends\s+series|classified\s+series|"
    r"multipack|"
    r"\bfigs?\b|\bfigures?\b"
    r")\b",
    re.IGNORECASE,
)
_INSTORE_SPORTS_PROTECTIVE_SUPPRESS_SNEAKERS_PATTERN = re.compile(
    r"\b("
    r"catcher|catcher'?s?|catchers|"
    r"chest\s+guard|chest\s+protector|converge|"
    r"batting\s+helmet|shoulder\s+pads|shin\s+guards|"
    r"football\s+helmet|hockey\s+helmet|lacrosse\s+helmet"
    r")\b",
    re.IGNORECASE,
)
# Shoe-shaped signals that should beat generic brand + apparel/toy ambiguity.
_INSTORE_STRONG_FOOTWEAR_ONLY_INTENT_PATTERN = re.compile(
    r"\b("
    r"sneakers?|sneaks?|kicks?\b|"
    r"\bshoes?\b|"
    r"footwear|"
    r"cleats?|"
    r"\bslides?\b|"
    r"\bdunks?\b(?:\s+(?:low|high))?|"
    r"\baf1\b|"
    r"\byeezy\b|"
    r"air\s*max|air\s*force|air\s*jordan|"
    r"sb\s*dunks?\b|"
    r"retro\s*jordans?\b|"
    r"jordan\s*\d+|aj\s*\d+|\baj\d+\b|"
    r"(?:running|basketball|tennis)\s+shoes?\b|"
    r"(?:running|basketball|tennis)\s+trainers?\b"
    r")\b",
    re.IGNORECASE,
)
_INSTORE_EXPLICIT_FOOTWEAR_INTENT_PATTERN = re.compile(
    r"\b("
    r"sneakers?|sneaks?|kicks?\b|"
    r"\bshoes?\b|"
    r"footwear|"
    r"cleats?|"
    r"\bslides?\b|"
    r"\bdunks?\b|"
    r"\baf1\b|"
    r"\bjordans?\b|"
    r"\byeezy\b|"
    r"air\s*max|air\s*force|air\s*jordan|"
    r"sb\s*dunks?\b|"
    r"retro\s*jordans?\b|"
    r"jordan\s*\d+|aj\s*\d+|\baj\d+\b|"
    r"(?:running|basketball|tennis)\s+shoes?\b|"
    r"(?:running|basketball|tennis)\s+trainers?\b"
    r")\b",
    re.IGNORECASE,
)


def instore_apparel_suppresses_sneakers_bucket(text: str) -> bool:
    return bool(_INSTORE_APPAREL_SUPPRESS_SNEAKERS_PATTERN.search(text or ""))


def instore_collectible_toy_suppresses_sneakers_bucket(text: str) -> bool:
    return bool(_INSTORE_COLLECTIBLE_TOY_SUPPRESS_SNEAKERS_PATTERN.search(text or ""))


def instore_sports_protective_suppresses_sneakers_bucket(text: str) -> bool:
    return bool(_INSTORE_SPORTS_PROTECTIVE_SUPPRESS_SNEAKERS_PATTERN.search(text or ""))


def instore_strong_footwear_only_intent(text: str) -> bool:
    return bool(_INSTORE_STRONG_FOOTWEAR_ONLY_INTENT_PATTERN.search(text or ""))


def instore_explicit_footwear_intent(text: str) -> bool:
    return bool(_INSTORE_EXPLICIT_FOOTWEAR_INTENT_PATTERN.search(text or ""))


def instore_sneakers_bucket_active(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw or not SNEAKERS_PATTERN.search(raw):
        return False
    if instore_collectible_toy_suppresses_sneakers_bucket(raw):
        return False
    if instore_sports_protective_suppresses_sneakers_bucket(raw):
        return False
    if instore_apparel_suppresses_sneakers_bucket(raw):
        return bool(instore_strong_footwear_only_intent(raw))
    if instore_explicit_footwear_intent(raw):
        return True
    return True


CARDS_PATTERN = re.compile(
    r"\b("
    # Pokemon - brand and set names
    r"pok[eé]mon|pokemon|"
    r"surging\s*sparks|prismatic\s*evolutions?|"
    r"temporal\s*forces?|twilight\s*masquerade|paldean\s*fates?|"
    r"obsidian\s*flames?|scarlet\s*&\s*violet|sv\d+|"
    r"crown\s*zenith|brilliant\s*stars?|astral\s*radiance|"
    r"lost\s*origin|silver\s*tempest|"
    r"pok[eé]mon\s*card|pokemon\s*card|pok[eé]mon\s*booster|pokemon\s*booster|"
    r"pok[eé]mon\s*pack|pokemon\s*pack|pok[eé]mon\s*box|pokemon\s*box|"
    r"pok[eé]mon\s*etb|pokemon\s*etb|elite\s*trainer\s*box|"
    # Magic The Gathering - brand and set names
    r"magic\s*the\s*gathering|mtg|"
    r"march\s*of\s*the\s*machine|march\s*of\s*the\s*machine\s*the\s*aftermath|"
    r"all\s*will\s*be\s*one|phyrexia|"
    r"the\s*bros\s*war|brothers\s*war|"
    r"dominaria\s*united|unfinity|unstable|unhinged|"
    r"double\s*masters|commander\s*masters?|modern\s*masters?|"
    r"time\s*spiral\s*remastered|time\s*spiral|"
    r"kaldheim|zendikar\s*rising|theros\s*beyond\s*death|"
    r"throne\s*of\s*eldraine|war\s*of\s*the\s*spark|"
    r"guilds\s*of\s*ravnica|ravnica\s*allegiance|"
    # MTG-style core codes (M21, M22, …). Use at most 2 digits so ".m570." eBay tokens do not match.
    r"core\s*set\s*\d+|m\d{1,2}\b|"
    r"commander\s*\d+|c\d+|"
    r"jumpstart|jump\s*start|"
    r"modern\s*horizons?|mh\d+|"
    r"mystery\s*booster|"
    r"mtg\s*booster|mtg\s*pack|mtg\s*box|mtg\s*bundle|"
    r"magic\s*booster|magic\s*pack|magic\s*box|magic\s*bundle|"
    r"booster\s*box|draft\s*box|set\s*box|collector\s*box|"
    r"collector\s*booster|draft\s*booster|set\s*booster|"
    # Sports Cards - brands and types
    r"topps\s*chrome|topps\s*finest|topps\s*heritage|topps\s*archives?|"
    r"topps\s*stadium\s*club|topps\s*gallery|topps\s*gold\s*label|"
    r"topps\s*flagship|topps\s*series\s*\d+|topps\s*update|"
    r"topps\s*bowman|bowman\s*chrome|bowman\s*draft|"
    r"topps\s*pro\s*debut|topps\s*prospect|"
    r"panini\s*prizm|panini\s*select|panini\s*optic|panini\s*donruss|"
    r"panini\s*contenders?|panini\s*chronicles?|panini\s*playbook|"
    r"panini\s*immaculate|panini\s*flawless|panini\s*national\s*treasures?|"
    r"panini\s*one\s*and\s*one|panini\s*recon|panini\s*spectra|"
    r"upper\s*deck|ud\s*series\s*\d+|"
    r"fleer|fleer\s*ultra|fleer\s*tradition|"
    r"donruss|donruss\s*optic|donruss\s*elite|"
    r"score|score\s*football|"
    r"leaf|leaf\s*metal|"
    r"nba\s*cards?|nfl\s*cards?|mlb\s*cards?|nhl\s*cards?|"
    r"baseball\s*cards?|football\s*cards?|basketball\s*cards?|hockey\s*cards?|"
    r"rookie\s*card|rc\s*card|rookie\s*auto|"
    r"autograph|auto\s*card|signed\s*card|"
    r"patch\s*card|relic\s*card|memorabilia\s*card|"
    r"refractor|chrome\s*refractor|"
    # Other Trading Cards
    r"yu[\s-]?gi[\s-]?oh|yugioh|ygo|"
    r"one\s*piece\s*card|one\s*piece\s*tcg|"
    r"dragon\s*ball\s*super\s*card|dragon\s*ball\s*tcg|"
    r"flesh\s*and\s*blood|fab\s*tcg|"
    r"lorcana|disney\s*lorcana|"
    r"digimon\s*card|digimon\s*tcg|"
    r"cardfight\s*!!\s*vanguard|vanguard|"
    r"weiss\s*schwarz|weiss\s*schwartz|"
    r"final\s*fantasy\s*tcg|fftcg|"
    # Generic terms
    r"sports?\s*cards?|trading\s*cards?|tcg|ccg|"
    r"trading\s*card\s*game|collectible\s*card\s*game|"
    r"booster\s*pack|booster\s*box|booster\s*case|"
    r"trading\s*card\s*pack|trading\s*card\s*box|"
    r"card\s*pack|card\s*box|card\s*set|"
    r"collector\s*card|collectible\s*card|"
    r"hobby\s*box|retail\s*box|"
    r"case\s*break|break|"
    r"graded\s*card|psa\s*\d+|bgs\s*\d+|cgc\s*\d+|"
    r"slab|slabbed"
    r")\b",
    re.IGNORECASE,
)

# Theatre
THEATRE_STORE_PATTERN = re.compile(
    "|".join(
        [
            r"cinemark(?:\s*(?:theatres?|theaters?|cinemas?))?",
            r"regal(?:\s*(?:cinemas?|theatres?|theaters?))?",
            r"amc\s*(?:theatres?|theaters?)?",
            r"alamo\s*drafthouse",
            r"harkins\s*theatres?",
            r"megaplex\s*theatres?",
            r"marcus\s*theatres?",
            r"showcase\s*cinemas?",
            r"cinepolis",
            r"cineplex",
            r"galaxy\s*theatres?",
            r"b\s*&\s*b\s*theatres?",
            r"movie\s+tavern",
            r"landmark\s*theatres?",
            r"carmike\s*cinemas?",
            r"cobb\s*theatres?",
            r"emagine\s*entertainment",
            r"flix\s*brewhouse",
            r"ipic\s*theatres?",
            r"studio\s*movie\s*grill",
            r"cinema\s*de\s*lux",
            r"angelika\s*film\s*center",
            r"arc light\s*cinemas?",
            r"arclight",
            r"cinerama",
            r"imax\s*theatre",
            r"dolby\s*cinema",
            r"movie\s*theater",
            r"movie\s*theatre",
            r"cinema",
            r"drive[- ]?in",
            r"drive[- ]?thru",
        ]
    ),
    re.IGNORECASE,
)
THEATRE_MERCH_PATTERN = re.compile(
    r"\b(popcorn\s+(?:bucket|tin|tub)|collectible\s+combo|souvenir\s+(?:cups?|tubs?)|movie\s+merch)\b",
    re.IGNORECASE,
)
THEATRE_CONTEXT_PATTERN = re.compile(
    r"\b(theatres?|theaters?|cinema|movie\s+release|movie\s+premiere)\b",
    re.IGNORECASE,
)

# Global triggers (FULL_SEND removed; PRICE_ERROR is the canonical glitch/price-error trigger)
PRICE_ERROR_PATTERN = re.compile(
    r"\b(bugged|wrong\s+price|accidental\s+drop|underpriced|checkout\s+working|error\s+price|"
    r"price\s+error|"
    # "messed up" alone is too noisy (process/signup/admin text). Require a commerce/price adjacency.
    r"(?:price|checkout|cart|listing|sku)\s+messed\s+up|messed\s+up\s+(?:price|checkout|cart|listing)|"
    r"mispriced|glitched\s+price|stack(?:ed|ing)\s+glitch|glitch(?:ed)?)\b",
    re.IGNORECASE,
)

# URL / money / %-off signals so we do not route (or treat as "complete") monitor stubs that only
# carry a glitch headline + "…" + footer before MESSAGE_UPDATE fills the body.
_DEAL_SUBSTANCE_SIGNAL_PATTERN = re.compile(
    r"(https?://|\bwww\.|\bamzn\.to\b|\ba\.co/|\bamazon\.(com|ca|co\.uk|de|fr|es|it|in|com\.mx|com\.au|co\.jp)\b|"
    r"/dp/|\bb0[a-z0-9]{8}\b|"
    r"[$£€]\s*\d|\d+\s*[$£€]|"
    r"\d{1,3}(?:,\d{3})+(?:\.\d{2})?\b|\d+\.\d{2}\b|"
    r"\d+%\s*(?:off|discount)|\b\d{2,}%\b)",
    re.IGNORECASE,
)
_DEAL_SUBSTANCE_STRIP_TRAILING_MONITOR_BOILERPLATE = re.compile(r"(?is)\n\s*---+\s*\n[\s\S]*$")
# Footer line is often "... From: Bot | By: Bot" (ellipsis before From) — not "\nfrom:" flush.
_DEAL_SUBSTANCE_STRIP_TRAILING_BYLINE = re.compile(
    r"(?is)\n[^\n]*\bfrom\s*:\s*.+\|\s*by\s*:[^\n]*\s*$",
)
# Same footer squeezed onto the headline line: "Title ... From: x | By: y"
_DEAL_SUBSTANCE_STRIP_INLINE_BYLINE = re.compile(
    r"(?i)\s+\.{1,}\s*from\s*:\s*.+\|\s*by\s*:.+$",
)


def deal_substance_core_text(blob: str) -> str:
    """Strip common monitor trailer (--- / From: …) for length checks only."""
    s = (blob or "").strip()
    if not s:
        return ""
    s = _DEAL_SUBSTANCE_STRIP_TRAILING_MONITOR_BOILERPLATE.sub("", s)
    s = _DEAL_SUBSTANCE_STRIP_TRAILING_BYLINE.sub("", s)
    s = _DEAL_SUBSTANCE_STRIP_INLINE_BYLINE.sub("", s)
    return s.strip()


def deal_substance_core_len(blob: str) -> int:
    return len(re.sub(r"\s+", " ", deal_substance_core_text(blob)).strip())


def blob_has_deal_substance_signals(blob: str) -> bool:
    return bool(_DEAL_SUBSTANCE_SIGNAL_PATTERN.search(blob or ""))


def passes_deal_substance_gate(blob: str, *, min_core_chars: int) -> bool:
    """
    True when the flattened body looks like a real lead (link / money / %-off) or is long enough
    that a text-only glitch write-up is plausible. Used for PRICE_ERROR routing and short-embed hydration.
    """
    if not (blob or "").strip():
        return False
    if blob_has_deal_substance_signals(blob):
        return True
    try:
        mc = int(min_core_chars)
    except Exception:
        mc = 52
    if mc < 12:
        mc = 12
    return deal_substance_core_len(blob) >= mc


def passes_price_error_routing_gate(
    blob: str, *, min_core_chars: int, require_deal_substance_signals: bool
) -> bool:
    """
    PRICE_ERROR destination gate: same as passes_deal_substance_gate, plus optional requirement that the blob
    contain commerce signals (URL, $ / £ / € amounts, %-off, ASIN-ish tokens). When required, blocks meme posts
    that only mention phrases like \"price error\" in prose with no deal substance.
    """
    if not passes_deal_substance_gate(blob, min_core_chars=min_core_chars):
        return False
    if require_deal_substance_signals and not blob_has_deal_substance_signals(blob):
        return False
    return True


# AFFILIATED_LINKS suppressions (channel is noisy if it accepts flip templates / fandom drops / stub edits).
_AFFILIATE_FLIP_FIELDS_PATTERN = re.compile(
    r"(?im)^\s*(when|where|retail|resell)\b",
    re.IGNORECASE,
)
_AFFILIATE_QUICK_LINKS_PATTERN = re.compile(r"(?im)^\s*quick\s+links?\b", re.IGNORECASE)
# Pokemon: use substring match so pokemoncenter.com / URLs match (word-boundary \bpokemon\b misses those).
_AFFILIATE_POKEMON_SUBSTRING_PATTERN = re.compile(r"pokemon", re.IGNORECASE)
# TCG stock-update style posts (ETB / booster / blister + SKU/available-stock/restock language) are not generic
# affiliate drops and should not route to AFFILIATED_LINKS even when "pokemon" is not present.
_AFFILIATE_TCG_GENERIC_TERMS_PATTERN = re.compile(
    r"\b("
    r"elite\s+trainer\s+box|"
    r"\betb\b|"
    r"booster\s*(?:bundle|box|pack)?|"
    r"3-?\s*booster\s+blister|"
    r"\bblister\b|"
    r"trading\s+card\s+game|"
    r"\btcg\b|"
    r"\bcard\s+game\b"
    r")\b",
    re.IGNORECASE,
)
_AFFILIATE_TCG_STOCK_UPDATE_TERMS_PATTERN = re.compile(
    r"\b("
    r"stock\s+update|"
    r"available\s+stock|"
    r"\brestock\b|"
    r"\bdrop\b|"
    r"\bsku\s*[:#]"
    r")\b",
    re.IGNORECASE,
)
# One Piece (TCG/anime): titles + StockX-style URLs use `One+Piece` / `one-piece` — not generic affiliated drops.
_AFFILIATE_ONE_PIECE_SUBSTRING_PATTERN = re.compile(r"(?i)one[\s+\-_]*piece")
_AFFILIATE_COMICS_PATTERN = re.compile(
    r"\b(marvel|capcom|dc|dcu|mcu|x-?men|avengers|spider-?man|batman|superman|wolverine|daredevil|punisher|"
    r"joker|harley\s+quinn|deadpool|iron\s+man|captain\s+america|thor|hulk)\b",
    re.IGNORECASE,
)
_AFFILIATE_COMICS_CONTEXT_PATTERN = re.compile(
    r"\b(variant|cover|foc|ratio|print(?:ing)?|issue\s*#?\s*\d+|#\s*\d{1,4})\b",
    re.IGNORECASE,
)


def _affiliate_text_strip_markdown_noise(blob: str) -> str:
    """So **Retail:** $12 embed fields match price-grid guards."""
    s = str(blob or "")
    s = re.sub(r"[\*_`]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def affiliate_should_suppress_affiliated_links(blob: str, *, min_core_chars: int = 80) -> str:
    """
    Return a non-empty suppression reason when the message should NOT route to AFFILIATED_LINKS.
    This keeps AFFILIATED_LINKS focused on simple non-Amazon product leads, not flip-templates or fandom drops.
    """
    raw = str(blob or "").strip()
    if not raw:
        return "empty"
    core_len = deal_substance_core_len(raw)
    # Very short stubs are usually incomplete edits / placeholder embeds.
    if core_len < 25:
        return "thin_stub_message"
    # Only apply the configurable length gate when the message *looks* truncated (ellipsis / monitor bylines),
    # otherwise short "link + one sentence" leads are allowed.
    try:
        mc = int(min_core_chars or 0)
    except Exception:
        mc = 0
    if mc > 0 and core_len < mc:
        if ("..." in raw) or re.search(r"(?i)\bfrom\s*:", raw) or re.search(r"(?i)\bby\s*:", raw):
            return "thin_truncated_message"
    # Monitor footer stubs (From/By) without any pricing context are usually incomplete/low-signal.
    has_footer = bool(re.search(r"(?i)\bfrom\s*:", raw) and re.search(r"(?i)\bby\s*:", raw))
    has_price_token = bool(re.search(r"[$£€]\s*\d|\d+\s*[$£€]|\b\d+%\b", raw))
    if has_footer and (not has_price_token) and core_len < 140:
        return "footer_stub_no_price"

    # Truncated / placeholder URLs are not "affiliate links" (e.g. https://www/...).
    # Also: Discord message jump links by themselves should not route to AFFILIATED_LINKS.
    try:
        urls = re.findall(r"https?://[^\s<>\"]+", raw, flags=re.IGNORECASE)
    except Exception:
        urls = []
    if urls:
        non_discord_urls: List[str] = []
        for u in urls:
            ul = str(u or "").strip()
            if not ul:
                continue
            if "..." in ul or ul.endswith("/") and "www/..." in ul.lower():
                return "truncated_url"
            try:
                p = urlparse(ul)
                host = (p.netloc or "").strip().lower()
            except Exception:
                host = ""
            # bad placeholders like https://www/... or host with no dot
            if host in ("www", "www.") or (host and "." not in host):
                return "truncated_url"
            if "discord.com" in host or "discordapp.com" in host:
                continue
            non_discord_urls.append(ul)
        if (not non_discord_urls) and re.search(r"(?i)discord(?:app)?\.com/channels/\d+/\d+/\d+", raw):
            return "discord_message_link_only"

    # Deal-monitor price grids: "Retail: $…" / "Resale: $…" (and flip-spelled "Resell:") — not generic affiliate drops.
    guard_plain = _affiliate_text_strip_markdown_noise(raw)
    if re.search(r"(?i)retail\s*:\s*\$", guard_plain):
        return "affiliate_retail_dollar_line"
    if re.search(r"(?i)(?:resale|resell)\s*:\s*\$", guard_plain):
        return "affiliate_resale_dollar_line"

    # Pokemon anywhere (body, mention, pokemoncenter.com, TCG titles).
    if _AFFILIATE_POKEMON_SUBSTRING_PATTERN.search(raw):
        return "pokemon_content"

    # Generic TCG restock/stock-update posts (ETB/booster/SKU/available stock) are not AFFILIATED_LINKS.
    if _AFFILIATE_TCG_GENERIC_TERMS_PATTERN.search(raw) and _AFFILIATE_TCG_STOCK_UPDATE_TERMS_PATTERN.search(raw):
        return "tcg_stock_update"

    # One Piece franchise / TCG (Crunchyroll Divine/Zephyr monitors, StockX search URLs, etc.).
    if _AFFILIATE_ONE_PIECE_SUBSTRING_PATTERN.search(raw):
        return "one_piece_content"

    # Flip template fields: When/Where/Retail/Resell (+ quick links or ebay) => not AFFILIATED_LINKS.
    has_when = bool(re.search(r"(?im)^\s*when\b", raw))
    has_where = bool(re.search(r"(?im)^\s*where\b", raw))
    has_retail = bool(re.search(r"(?im)^\s*retail\b", raw))
    has_resell = bool(re.search(r"(?im)^\s*resell\b", raw))
    if has_when and has_where and has_retail and has_resell:
        return "flip_template_fields"
    if has_retail and has_resell and (bool(_AFFILIATE_QUICK_LINKS_PATTERN.search(raw)) or "ebay" in raw.lower()):
        return "flip_template_retail_resell"

    # Comics / hero-villain fandom posts: only suppress when comic-like context exists, to avoid blocking "Washington DC".
    if _AFFILIATE_COMICS_PATTERN.search(raw) and _AFFILIATE_COMICS_CONTEXT_PATTERN.search(raw):
        return "comics_fandom_content"

    return ""


CLEARANCE_PATTERN = re.compile(
    r"\b(clearance|markdown|70%\s*off|80%\s*off|90%\s*off|penny\s*deal|shelf\s*pull)\b",
    re.IGNORECASE,
)
PROFITABLE_FLIP_PATTERN = re.compile(
    r"\b(200%|300%|400%|500%|\d{3,}%|3x|4x|5x|\d+x\s*retail|high\s*roi|exceptional\s*margin|great\s*flip|easy\s*money|quick\s*flip)\b",
    re.IGNORECASE,
)
# Woot leads often include Amazon affiliate tracking (amzn.to links). `STORE_DOMAINS` maps woot.com -> "woot"
# so `_is_amazon_primary` / affiliated routing can treat Woot as the primary merchant (not Amazon).
WOOT_DEALS_PATTERN = re.compile(r"\bwoot\s*deals\b", re.IGNORECASE)

# Amazon profitable flip indicators (Keepa-style: avg 30, % drop, etc.)
AMAZON_PROFITABLE_INDICATOR_PATTERN = re.compile(
    r"\b(avg\s*30|average\s*30|avg\s*365|\d+%\s*drop|\d+%\s*off|amazon\s*sold|flip\s*alert)\b",
    re.IGNORECASE,
)

# Divine / "AMZ Price Errors" monitor — rigid templates; route to AMAZON not AMAZON_PROFITABLE_LEADS.
_AMZ_PE_ERRORS_FOOTER_PATTERN = re.compile(r"by:\s*amz\s*price\s*errors\b", re.IGNORECASE)
_AMZ_PE_FLIP_LINE_PATTERN = re.compile(r"amazon\s+to\s+ebay\s+flip", re.IGNORECASE)
_AMZ_PE_WAREHOUSE_ALERT_PATTERN = re.compile(r"like-new\s*\([^)]*warehouse[^)]*\)\s*alert", re.IGNORECASE)
_AMZ_PE_PRICE_AMAZON_SOLD_PATTERN = re.compile(
    r"price:\s*\$[\d,]+(?:\.\d{2})?\s*\(\s*amazon\s*sold\s*\)",
    re.IGNORECASE,
)
_AMZ_PE_AVG30_LINE_PATTERN = re.compile(r"average\s*30\s*:", re.IGNORECASE)
_AMZ_PE_AMAZON_SOLD_LABEL_PATTERN = re.compile(r"amazon\s*sold\s*:", re.IGNORECASE)
_AMZ_PE_EBAY_AVG_PATTERN = re.compile(r"ebay\s*avg", re.IGNORECASE)


def is_amz_price_errors_monitor_blob(text: str) -> bool:
    """
    True for templated Discord alerts from Divine / AMZ Price Errors (footer, flip lines, warehouse alerts).
    These hit AMAZON_PROFITABLE_INDICATOR_PATTERN (e.g. amazon sold, average 30, % drop) but are high-volume
    monitor spam relative to curated profitable leads.
    When True, PRICE_ERROR / glitch routing is skipped (classifier + global_triggers) so these posts are not
    treated as generic “price error” leads.
    """
    if not text or not str(text).strip():
        return False
    s = str(text)
    sl = s.lower()
    if _AMZ_PE_ERRORS_FOOTER_PATTERN.search(sl):
        return True
    if _AMZ_PE_FLIP_LINE_PATTERN.search(sl):
        return True
    if _AMZ_PE_WAREHOUSE_ALERT_PATTERN.search(sl):
        return True
    if _AMZ_PE_PRICE_AMAZON_SOLD_PATTERN.search(sl) and _AMZ_PE_AVG30_LINE_PATTERN.search(sl):
        return True
    if (
        _AMZ_PE_AMAZON_SOLD_LABEL_PATTERN.search(sl)
        and _AMZ_PE_EBAY_AVG_PATTERN.search(sl)
        and (re.search(r"amazon\s+to\s+ebay", sl) or re.search(r"difference\s*!", sl))
    ):
        return True
    return False


_RINGINTHEDEALS_HOST_PATTERN = re.compile(r"ringinthedeals\.com", re.IGNORECASE)
# FLIPFLUENCE-style: "Take 67% Off Product Name!"
_TAKE_PCT_OFF_HEADLINE_PATTERN = re.compile(r"take\s+\d{1,3}\s*%\s*off\b", re.IGNORECASE)
_REG_PAREN_PRICE_PATTERN = re.compile(r"\(\s*Reg\s*\$", re.IGNORECASE)
_SUBSCRIBE_SAVE_PATTERN = re.compile(r"subscribe\s*&\s*save", re.IGNORECASE)
# Common FlipFluence/RingInTheDeals template emoji scaffolding.
_FLIPFLUENCE_TEMPLATE_EMOJI_PATTERN = re.compile(r"[🔔🏷️✅]", re.UNICODE)
_FLIPFLUENCE_REROUTER_BYLINE_PATTERN = re.compile(
    r"from:\s*flipfluence\s*\|\s*by:\s*rerouter\s*\|\s*flipfluence\b",
    re.IGNORECASE,
)


def is_ringinthedeals_flipfluence_deal_blob(text: str) -> bool:
    """
    Templated deals from ringinthedeals.com / FLIPFLUENCE (Take N% Off, Reg $..., /deal/ links).
    High-volume affiliate feed — use regular AMAZON bucket, not AMAZON_PROFITABLE_LEADS.
    """
    if not text or not str(text).strip():
        return False
    raw = str(text)
    sl = raw.lower()
    has_ring = bool(_RINGINTHEDEALS_HOST_PATTERN.search(sl))
    # Some variants may not include the ringinthedeals URL in the same blob chunk (e.g. content-only snapshot),
    # but still carry the distinctive template scaffolding + "Reg $" pricing line.
    template_scaffold = bool(_SUBSCRIBE_SAVE_PATTERN.search(raw) and _REG_PAREN_PRICE_PATTERN.search(raw))
    if not has_ring and not template_scaffold:
        return False
    if template_scaffold and _FLIPFLUENCE_TEMPLATE_EMOJI_PATTERN.search(raw):
        return True
    if _TAKE_PCT_OFF_HEADLINE_PATTERN.search(raw):
        return True
    if "flipfluence" in sl:
        return True
    if "/deal/" in sl and (_REG_PAREN_PRICE_PATTERN.search(raw) or re.search(r"\$\d", raw)):
        return True
    return False


def is_flipfluence_rerouter_product_card_blob(text: str) -> bool:
    """
    FlipFluence "product card" posts (often Amazon DP links + long product description + rerouter byline).
    These are high-volume informational cards and should not qualify as CONVERSATIONAL_DEALS.
    """
    if not text or not str(text).strip():
        return False
    raw = str(text)
    sl = raw.lower()
    if "flipfluence" not in sl:
        return False
    if not _FLIPFLUENCE_REROUTER_BYLINE_PATTERN.search(sl):
        return False
    # Typical shape includes at least one direct product URL (often Amazon) + long description.
    if "http" not in sl:
        return True
    if "/dp/" in sl or "amazon." in sl or "amzn.to" in sl or "a.co/" in sl:
        return True
    return True


def is_divine_helper_price_monitor_blob(text: str) -> bool:
    """
    Templated Discord alerts from Divine Helper v2 (rule N prefix, New/Old Price, tool links row).
    High-volume price-monitor traffic — route to generic AMAZON, not AMAZON_PROFITABLE_LEADS.
    """
    if not text or not str(text).strip():
        return False
    raw = str(text)
    sl = raw.lower()
    if "divine helper v2" in sl:
        return True
    # Structured embed shape: New Price + Old Price + Discount + FBA + KEEPA/SAS row (distinct from hand-typed leads).
    has_price_grid = (
        "new price" in sl
        and "old price" in sl
        and "discount" in sl
        and "seller" in sl
        and re.search(r"\bfba\b", sl)
        and "keepa" in sl
        and "sas" in sl
        and "ebay" in sl
        and "google" in sl
    )
    if not has_price_grid:
        return False
    if re.search(r"(?m)^\s*\(\s*rule\s+\d+\s*\)", raw):
        return True
    if "promotion" in sl and "false" in sl and "business required" in sl:
        return True
    return False


def is_flipflip_restock_monitor_blob(text: str) -> bool:
    """
    FlipFlip stock/restock monitor embeds (Walmart Item Restocked, Offer ID, FlipFlip Monitors footer).
    """
    if not text or not str(text).strip():
        return False
    sl = str(text).lower()
    if "flipflip monitors" in sl:
        return True
    if re.search(r"walmart\s*-\s*item\s+restocked", sl):
        return True
    if "offer id" in sl and "order limit" in sl and re.search(r"\bsku\b", sl):
        if "flipflip" in sl or "lightningmobile" in sl or "selleramp" in sl:
            return True
    return False


def should_skip_amazon_profitable_leads_monitor_blob(text: str) -> bool:
    """True when message should not use the profitable-leads bucket (spam/monitor templates)."""
    return bool(
        is_amz_price_errors_monitor_blob(text)
        or is_ringinthedeals_flipfluence_deal_blob(text)
        or is_divine_helper_price_monitor_blob(text)
        or is_flipflip_restock_monitor_blob(text)
    )


def allow_amz_deals_despite_complicated_monitor(text: str) -> bool:
    """
    Conversational / affiliate-bridge posts that use monitor-ish scaffolding (New Deal Found!, $x.xx teasers)
    but should still be eligible for the AMZ_DEALS bucket (not generic AMAZON / profitable-leads).
    """
    sl = str(text or "").lower()
    if "flipfluence" in sl:
        return True
    if "pricedoffers.com" in sl:
        return True
    if "deal soldier" in sl:
        return True
    if re.search(r"miablogs\.us/", sl) and "paid-ad" in sl:
        return True
    return False


def is_amz_deals_affiliate_bridge_blob(text: str) -> bool:
    """
    Affiliate-bridge / feed-shaped Amazon promos that should bucket as AMZ_DEALS (conversational deals),
    not AMAZON_PROFITABLE_LEADS and not AFFILIATED_LINKS.

    Examples: FlipFluence + amazon.com/dp, pricedoffers.com + clip/checkout copy, miablogs paid-ad ASIN links.
    """
    if not text or not str(text).strip():
        return False
    raw = str(text)
    sl = raw.lower()
    has_amazonish = bool(
        re.search(r"\bamazon\b", sl)
        or re.search(r"amazon\.[a-z.]{2,16}/", sl)
        or re.search(r"amzn\.to|a\.co/", sl)
        or re.search(r"(?:/|\b)dp/\s*B0[A-Z0-9]{8}\b", raw, re.IGNORECASE)
        or re.search(r"\bB0[A-Z0-9]{8}\b", raw, re.IGNORECASE)
    )
    if "flipfluence" in sl and (has_amazonish or _AMZ_COMPLICATED_NEW_DEAL_FOUND_PATTERN.search(sl)):
        return bool(CONVERSATIONAL_DEALS_STRICT_SIGNAL_PATTERN.search(raw))
    if "pricedoffers.com" in sl and (
        "$" in raw
        or has_amazonish
        or "deal soldier" in sl
        or re.search(r"\bclip\b", sl)
    ):
        return bool(CONVERSATIONAL_DEALS_STRICT_SIGNAL_PATTERN.search(raw))
    if re.search(r"miablogs\.us/[^\s]*paid-ad", sl, re.IGNORECASE) and re.search(r"\bB0[A-Z0-9]{8}\b", raw, re.IGNORECASE):
        return bool(CONVERSATIONAL_DEALS_STRICT_SIGNAL_PATTERN.search(raw))
    # RingInTheDeals / FLIPFLUENCE templated deal blasts are high-volume feed posts; they should NOT
    # force routing into the conversational deals bucket.
    return False


# Noisy deal-monitor banners / price-teaser layouts — do not use "simple" profitable-leads exception.
_AMZ_COMPLICATED_CHECK_PRICE_PATTERN = re.compile(r"check\s+your\s+price", re.IGNORECASE)
_AMZ_COMPLICATED_NEW_DEAL_FOUND_PATTERN = re.compile(r"\bnew\s+deal\s+found\b", re.IGNORECASE)
_AMZ_COMPLICATED_BEFORE_REG_PATTERN = re.compile(r"before\s+reg(?:ular|\.?)\b", re.IGNORECASE)
# Placeholder / teaser prices like "$7.xx" or "xx $7.xx"
_AMZ_COMPLICATED_XX_PRICE_PATTERN = re.compile(
    r"(?:\$\s*\d*\.xx\b|\bxx\s*\$?\s*\d|\d+\s*\$?\s*\d+\.xx\b)",
    re.IGNORECASE,
)


def has_placeholder_teaser_price(text: str) -> bool:
    """
    True when the content includes teaser / placeholder pricing like "$7.xx".

    These are monitor/template artifacts and should not qualify as CONVERSATIONAL_DEALS.
    """
    try:
        return bool(_AMZ_COMPLICATED_XX_PRICE_PATTERN.search(str(text or "")))
    except Exception:
        return False


def is_amazon_deal_complicated_monitor_blob(text: str) -> bool:
    """
    True for loud monitor-style deal posts (headers, teaser prices). These stay on the generic
    AMAZON bucket even if they mention Amazon; they are excluded from the simple profitable-leads path.
    """
    if not text or not str(text).strip():
        return False
    sl = str(text).lower()
    if _AMZ_COMPLICATED_NEW_DEAL_FOUND_PATTERN.search(sl):
        return True
    if _AMZ_COMPLICATED_CHECK_PRICE_PATTERN.search(sl):
        return True
    if _AMZ_COMPLICATED_BEFORE_REG_PATTERN.search(sl):
        return True
    if _AMZ_COMPLICATED_XX_PRICE_PATTERN.search(sl):
        return True
    return False


def is_simple_amazon_profitable_lead_blob(text: str) -> bool:
    """
    Plain Amazon deal copy (price + short context + link) without Keepa-style % / avg-30 triggers.
    Used to route curated-style alerts to AMAZON_PROFITABLE_LEADS when not complicated-monitor shaped.
    """
    if not text or not str(text).strip():
        return False
    raw = str(text)
    sl = raw.lower()
    if not re.search(r"\$[\d,]+(?:\.\d{2})?\b", raw):
        return False
    amazonish = bool(
        re.search(r"\bamazon\b", sl)
        or re.search(r"amzn\.to|a\.co/", sl)
        or re.search(r"amazon\.[a-z.]{2,12}/", sl)
        or re.search(r"\bB0[A-Z0-9]{8}\b", raw, re.IGNORECASE)
    )
    if not amazonish:
        return False
    if is_amazon_deal_complicated_monitor_blob(raw):
        return False
    if is_amz_price_errors_monitor_blob(raw):
        return False
    if is_ringinthedeals_flipfluence_deal_blob(raw):
        return False
    if is_divine_helper_price_monitor_blob(raw):
        return False
    if is_flipflip_restock_monitor_blob(raw):
        return False
    simple_signals = [
        r"on\s+sale\s+from\s+\$[\d,]+(?:\.\d{2})?\s+down\s+to\s+\$",
        r"\bfrom\s+\$[\d,]+(?:\.\d{2})?\s+down\s+to\s+\$",
        r"sold\s+by\s+amazon\s*",
        r"no\s+risk\s+preorder",
        r"these\s+are\s+normally",
        r"normally\s+\$[\d,]+(?:\.\d{2})?",
        r"\$[\d,]+(?:\.\d{2})?[^\n]{0,140}\bon\s+amazon\b",
        r"\bon\s+amazon\b[^\n]{0,140}\$[\d,]+(?:\.\d{2})?",
        r"clip\s+(?:the\s+)?(?:\d+\s*%?\s*off\s+)?coupon",
        r"sub\s*&\s*save|subscribe\s*&\s*save",
        r"(?:target|walmart|costco|kroger)\s+sells.{0,100}\$",
        r"same\s+(?:bottle|item|pack|product|sku|size)\s+for\s+\$",
        r"other\s+retailers.{0,120}\$[\d,]+(?:\.\d{2})?",
        r"\$[\d,]+(?:\.\d{2})?.{0,120}other\s+retailers",
    ]
    return any(re.search(p, sl) for p in simple_signals)


# Conversational deal templates (Amazon-focused phrases).
# NOTE: The bucket is `CONVERSATIONAL_DEALS` (not Amazon-only), but these phrases are Amazon-centric.
CONVERSATIONAL_DEALS_AMAZON_PHRASE_PATTERN = re.compile(
    r"\b("
    r"use\s+code\s+at\s+checkout"
    r"|use\s+promo\s+code"
    r"|apply\s+promo\s*code"
    r"|promo\s+stack"
    r"|with\s+code\b"
    r"|subscribe\s*&\s*save"
    r"|must\s+subscribe\s*&\s*save"
    r"|clip\s+(?:the\s+)?\d+\s*%\s*off\s+coupon"
    r"|clip\s+(?:the\s+)?coupon"
    r"|clip\s+\$\s*[\d,]+(?:\.\d{2})?\s+coupon"
    r"|originally\s+listed\s+on\s+amazon"
    r"|this\s+amazon\s+seller\s+is\s+glitching"
    r"|lowest\s+ever\s+price"
    r"|promo\s+drops"
    r"|lowest\s+ever\s+on\s+amazon"
    r"|free\s+at\s+checkout"
    r"|buy\s+on\s+amazon"
    r"|shipped\s+and\s+sold\s+by\s+amazon"
    r")\b",
    re.IGNORECASE,
)

# Strict subset used to keep CONVERSATIONAL_DEALS tight.
# Excludes weak phrases that appear in product-page embeds and monitor cards (e.g. shipped/sold, buy on Amazon).
CONVERSATIONAL_DEALS_STRICT_SIGNAL_PATTERN = re.compile(
    r"\b("
    r"use\s+code\s+at\s+checkout"
    r"|use\s+promo\s+code"
    r"|apply\s+promo\s*code"
    r"|promo\s+stack"
    r"|with\s+code\b"
    r"|subscribe\s*&\s*save"
    r"|must\s+subscribe\s*&\s*save"
    r"|clip\s+(?:the\s+)?\d+\s*%\s*off\s+coupon"
    r"|clip\s+(?:the\s+)?coupon"
    r"|clip\s+\$\s*[\d,]+(?:\.\d{2})?\s+coupon"
    r"|free\s+at\s+checkout"
    r")\b",
    re.IGNORECASE,
)

# Chatty grocery / delivery / in-app price glitches (CONVERSATIONAL_DEALS bucket).
# Matches templates that often have no product URL in the embed body.
CONVERSATIONAL_DEALS_RETAIL_PATTERN = re.compile(
    r"\b("
    r"glitch(?:ing|ed)?\s+on\s+instacart"
    r"|ringing\s+up\s+for"
    r"|check\s+all\s+stores\s+near\s+you"
    r"|walmart\s+on\s+instacart"
    r"|target\s+on\s+instacart"
    r"|instacart\s*[—-]\s*(?:check|see|try)"
    r")\b",
    re.IGNORECASE,
)

# Affiliate wrapper domains that should never qualify for CONVERSATIONAL_DEALS.
# (User request: exclude JoyLink links from CONVERSATIONAL_DEALS.)
CONVERSATIONAL_DEALS_BLOCKED_DOMAIN_PATTERN = re.compile(r"https?://(?:www\.)?joylink\.io\b", re.IGNORECASE)

# Store lists (subset; mirrors global_filters)
MAJOR_STORES = [
    "walmart",
    "target",
    "amazon",
    "best buy",
    "costco",
    "sam's club",
    "home depot",
    "lowe's",
    "lowes",
    "gamestop",
    "nike",
    "adidas",
    "foot locker",
    "finish line",
    "macy's",
    "nordstrom",
    "sephora",
    "ulta",
    "kroger",
    "heb",
    "meijer",
    "publix",
    "menards",
    "dick's sporting goods",
    "academy sports",
    "cabela's",
    "bass pro",
    "trader joe's",
    "trader joes",
    "jack in the box",
    "whole foods",
    "sprouts",
    "scheels",
    "fleet farm",
    "bj's wholesale",
    "rite aid",
    "walgreens",
    "cvs",
    "hobby lobby",
    "pottery barn",
    "disney store",
    "disneyland",
    "disney world",
    "disney parks",
    "walt disney world",
    "disney",
    "build-a-bear",
    "mattel creations",
    "starbucks",
    "dunkin",
    "dunkin donuts",
    "popeyes",
    "kohl's",
    "kohls",
    "jcpennys",
    "jc penney",
    "jcpenney",
    "belk",
    "dillard's",
    "dillards",
    "neiman marcus",
    "saks fifth avenue",
    "saks",
    "bloomingdale's",
    "bloomingdales",
    "barneys",
    "barney's",
    "lord & taylor",
    "nordstrom rack",
    "saks off 5th",
    "tj maxx",
    "marshalls",
    "homegoods",
    "bed bath & beyond",
    "bed bath and beyond",
    "wayfair",
    "overstock",
    "zappos",
    "dsw",
    "designer shoe warehouse",
    "famous footwear",
    "shoes for crews",
    "journeys",
    "journey's",
    "champs sports",
    "champs",
    "eastbay",
    "jimmy jazz",
    "sneakerhead",
    "stockx",
    "goat",
    "stadium goods",
    "flight club",
    "barnes & noble",
    "barnes and noble",
    "books-a-million",
    "half price books",
    "booksamillion",
    "petco",
    "petsmart",
    "pet supplies plus",
    "petland",
    "chewy",
    "auto zone",
    "autozone",
    "advance auto parts",
    "oreilly auto parts",
    "oreilly",
    "o'reilly",
    "pep boys",
    "napa auto parts",
    "napa",
    "michaels",
    "joann",
    "joann fabrics",
    "hobby lobby",
    "ac moore",
    "a.c. moore",
    "pat catan's",
    "michael's",
    "office depot",
    "office max",
    "staples",
    "best buy",
    "micro center",
    "fry's electronics",
    "b&h photo",
    "bh photo",
    "adorama",
    "crutchfield",
    "newegg",
    "tiger direct",
    "radioshack",
    "radio shack",
    "at&t",
    "att",
    "verizon",
    "t-mobile",
    "tmobile",
    "sprint",
    "us cellular",
    "cricket",
    "metro pcs",
    "metro by t-mobile",
    "boost mobile",
    "straight talk",
    "total wireless",
    "mint mobile",
    "visible",
    "google fi",
    "xfinity",
    "comcast",
    "spectrum",
    "optimum",
    "cox",
    "frontier",
    "centurylink",
    "lumen",
    "dish network",
    "directv",
    "hulu",
    "netflix",
    "disney plus",
    "disney+",
    "hbo max",
    "hbo",
    "paramount plus",
    "paramount+",
    "peacock",
    "apple tv",
    "amazon prime",
    "prime video",
    "espn plus",
    "espn+",
    "youtube tv",
    "sling tv",
    "fubo",
    "philo",
    "tubi",
    "pluto tv",
    "roku",
    "fire tv",
    "chromecast",
    "apple tv",
    "nvidia shield",
    "sony playstation",
    "playstation",
    "ps5",
    "ps4",
    "xbox",
    "xbox series x",
    "xbox series s",
    "nintendo switch",
    "nintendo",
    "steam",
    "epic games",
    "ubisoft",
    "ea",
    "electronic arts",
    "activision",
    "blizzard",
    "rockstar",
    "take-two",
    "2k",
    "warner bros",
    "warner brothers",
    "universal",
    "paramount",
    "sony pictures",
    "disney",
    "marvel",
    "dc comics",
    "hasbro",
    "mattel",
    "lego",
    "mega construx",
    "funko",
    "hot topic",
    "boxlunch",
    "spencer's",
    "spencers",
    "fye",
    "for your entertainment",
    "thinkgeek",
    "gamestop",
    "eb games",
    "gamefly",
    "redbox",
    "blockbuster",
    "family video",
    "movie trading company",
    "half price books",
    "2nd & charles",
    "books-a-million",
    "barnes & noble",
    "barnes and noble",
    "indigo",
    "chapters",
    "coles",
    "chapters.indigo",
    "indigo books",
    "indigo.ca",
    "indigo.com",
    "indigo",
    "chapters",
    "coles",
    "smith books",
    "bookstore",
    "book store",
    "comic book store",
    "comic shop",
    "comics",
    "comic books",
    "graphic novels",
    "manga",
    "anime",
    "collectibles",
    "trading cards",
    "sports cards",
    "pokemon cards",
    "magic the gathering",
    "mtg",
    "yu-gi-oh",
    "yugioh",
    "dragon ball",
    "one piece",
    "naruto",
    "attack on titan",
    "demon slayer",
    "my hero academia",
    "jujutsu kaisen",
    "chainsaw man",
    "spy x family",
    "tokyo ghoul",
    "death note",
    "fullmetal alchemist",
    "cowboy bebop",
    "evangelion",
    "ghost in the shell",
    "akira",
    "studio ghibli",
    "ghibli",
    "totoro",
    "spirited away",
    "howl's moving castle",
    "princess mononoke",
    "kiki's delivery service",
    "ponyo",
    "the boy and the heron",
    "marnie",
    "arrietty",
    "the wind rises",
    "the tale of princess kaguya",
    "when marnie was there",
    "the secret world of arrietty",
    "from up on poppy hill",
    "the cat returns",
    "whisper of the heart",
    "my neighbor totoro",
    "castle in the sky",
    "nausicaa",
    "laputa",
    "grave of the fireflies",
    "only yesterday",
    "ocean waves",
    "pom poko",
    "my neighbors the yamadas",
    "the yamadas",
    "tales from earthsea",
    "earthsea",
    "the borrower arrietty",
    "arrietty",
    "the secret world of arrietty",
    "from up on poppy hill",
    "the cat returns",
    "whisper of the heart",
    "my neighbor totoro",
    "castle in the sky",
    "laputa",
    "nausicaa",
    "grave of the fireflies",
    "only yesterday",
    "ocean waves",
    "pom poko",
    "my neighbors the yamadas",
    "the yamadas",
    "tales from earthsea",
    "earthsea",
    "the borrower arrietty",
    "arrietty",
    "the secret world of arrietty",
    "from up on poppy hill",
    "the cat returns",
    "whisper of the heart",
    "my neighbor totoro",
    "castle in the sky",
    "laputa",
    "nausicaa",
    "grave of the fireflies",
    "only yesterday",
    "ocean waves",
    "pom poko",
    "my neighbors the yamadas",
    "the yamadas",
    "tales from earthsea",
    "earthsea",
]
DISCOUNTED_STORES = [
    "burlington",
    "burlington coat factory",
    "marshalls",
    "tj maxx",
    "ross",
    "ross dress for less",
    "five below",
    "dollar tree",
    "dollar general",
    "family dollar",
    "aldi",
    "lidl",
    "homegoods",
    "ollie's",
    "ollies",
    "big lots",
    "burke's outlet",
    "dd's discounts",
    "dd's",
    "dds",
    "city trends",
    "rainbow shops",
    "roses",
    "grocery outlet",
    "daiso",
    "popshelf",
    "tuesday morning",
    "gabriel brothers",
    "gabes",
    "gordmans",
    "steve & barry's",
    "steve and barry's",
    "stein mart",
    "syms",
    "filene's basement",
    "liquidation world",
    "nordstrom rack",
    "saks off 5th",
    "neiman marcus last call",
    "last call",
    "century 21",
    "century21",
    "marshalls",
    "homegoods",
    "sierra trading post",
    "sierra",
    "oak tree",
    "oaktree",
    "gabriel brothers",
    "gabes",
    "gordmans",
    "steve & barry's",
    "steve and barry's",
    "stein mart",
    "sym's",
    "syms",
    "filene's basement",
    "liquidation world",
    "nordstrom rack",
    "saks off 5th",
    "neiman marcus last call",
    "last call",
    "century 21",
    "century21",
    "marshalls",
    "homegoods",
    "sierra trading post",
    "sierra",
    "oak tree",
    "oaktree",
]

def _store_token_to_pattern(token: str) -> str:
    """
    Build a safer store-token regex that avoids substring false-positives.
    Example: token "ea" should not match inside "average".
    """
    tok = str(token or "").strip()
    if not tok:
        return ""
    esc = re.escape(tok)
    # Prevent matching inside other words (letters/digits/underscore).
    return rf"(?<!\w){esc}(?!\w)"


MAJOR_STORE_PATTERN = re.compile(
    "|".join([p for p in (_store_token_to_pattern(s) for s in MAJOR_STORES) if p]),
    re.IGNORECASE,
)
DISCOUNTED_STORE_PATTERN = re.compile(
    "|".join([p for p in (_store_token_to_pattern(s) for s in DISCOUNTED_STORES) if p]),
    re.IGNORECASE,
)
ALL_STORE_PATTERN = re.compile(
    "|".join([p for p in (_store_token_to_pattern(s) for s in (MAJOR_STORES + DISCOUNTED_STORES)) if p]),
    re.IGNORECASE,
)

# Domain mapping (used for affiliated link / store-domain routing)
STORE_DOMAINS = {
    "amazon.com": "amazon",
    "amzn.to": "amazon",
    "a.co": "amazon",
    "woot.com": "woot",
    "walmart.com": "walmart",
    "target.com": "target",
    "bestbuy.com": "best buy",
    "costco.com": "costco",
    "homedepot.com": "home depot",
    "lowes.com": "lowe's",
    "gamestop.com": "gamestop",
}

# Label/header detection (used for in-store style messages)
LABEL_PATTERN = re.compile(r"(?im)^\s*(retail|resell|where|location|store)\s*[:\-]\s*.+$")

INSTORE_KEYWORDS = [
    "in store",
    "instore",
    "bopis",
    "pickup",
    "brick and mortar",
    "brick-and-mortar",
]

# Discord structural mentions / emoji / timestamp tokens (API wire format).
_DISCORD_STRUCTURAL_TOKEN_RE = re.compile(
    r"<@!?[0-9]{15,22}>|<@&[0-9]{15,22}>|<#[0-9]{15,22}>"
    r"|<a?:[A-Za-z0-9_]{1,32}:[0-9]{15,22}>"
    r"|<t:[0-9]{6,22}:[RrDdFfTt]>",
    re.IGNORECASE,
)


def is_mention_format_noise_blob(text_blob: str, attachments: Optional[List[Dict[str, Any]]] = None) -> bool:
    """
    True when the message is effectively only Discord pings / mention-shaped noise.

    Used to avoid DEFAULT / UNCLASSIFIED spam for posts like role pings (`<@&...>`) or
    pasted `@RoleName` lines with no deal payload (no URLs, no $ prices, no ASINs).
    """
    raw = str(text_blob or "").strip()
    if not raw:
        return False
    low = raw.lower()
    if "http://" in low or "https://" in low:
        return False
    if re.search(r"\$[\d,]", raw):
        return False
    if re.search(r"\bB0[A-Z0-9]{8}\b", raw, re.IGNORECASE):
        return False

    for a in attachments or []:
        if not isinstance(a, dict):
            continue
        u = str(a.get("url") or a.get("proxy_url") or "").strip().lower()
        if u.startswith("http://") or u.startswith("https://"):
            return False

    rem = _DISCORD_STRUCTURAL_TOKEN_RE.sub(" ", raw)
    rem = re.sub(r"(?i)@everyone\b|@here\b", " ", rem)
    rem = re.sub(r"\s+", " ", rem).strip()
    if not rem:
        return bool(re.search(r"<[@#]", raw) or re.search(r"(?i)@everyone|@here\b", raw))

    def _literal_at_ping_lines_only(s: str) -> bool:
        s2 = str(s or "").strip()
        if not s2:
            return False
        for line in s2.splitlines():
            t = line.strip()
            if not t:
                continue
            if not t.startswith("@"):
                return False
        return True

    if _literal_at_ping_lines_only(rem):
        return True
    return False

