from __future__ import annotations

import re

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
    r"trainers?|"
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
    r"forum|"
    r"retro|"
    r"collab|collaboration"
    r")\b",
    re.IGNORECASE,
)
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
    r"price\s+error|messed\s+up|mispriced|glitched\s+price|stack(?:ed|ing)\s+glitch|glitch(?:ed)?)\b",
    re.IGNORECASE,
)
CLEARANCE_PATTERN = re.compile(
    r"\b(clearance|markdown|70%\s*off|80%\s*off|90%\s*off|penny\s*deal|shelf\s*pull)\b",
    re.IGNORECASE,
)
PROFITABLE_FLIP_PATTERN = re.compile(
    r"\b(200%|300%|400%|500%|\d{3,}%|3x|4x|5x|\d+x\s*retail|high\s*roi|exceptional\s*margin|great\s*flip|easy\s*money|quick\s*flip)\b",
    re.IGNORECASE,
)
# Woot leads are often wrapped with Amazon affiliate tracking (amzn.to links),
# so we treat them as a separate primary store class.
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


def is_ringinthedeals_flipfluence_deal_blob(text: str) -> bool:
    """
    Templated deals from ringinthedeals.com / FLIPFLUENCE (Take N% Off, Reg $..., /deal/ links).
    High-volume affiliate feed — use regular AMAZON bucket, not AMAZON_PROFITABLE_LEADS.
    """
    if not text or not str(text).strip():
        return False
    raw = str(text)
    sl = raw.lower()
    if not _RINGINTHEDEALS_HOST_PATTERN.search(sl):
        return False
    if _TAKE_PCT_OFF_HEADLINE_PATTERN.search(raw):
        return True
    if "flipfluence" in sl:
        return True
    if "/deal/" in sl and (_REG_PAREN_PRICE_PATTERN.search(raw) or re.search(r"\$\d", raw)):
        return True
    return False


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


# Noisy deal-monitor banners / price-teaser layouts — do not use "simple" profitable-leads exception.
_AMZ_COMPLICATED_CHECK_PRICE_PATTERN = re.compile(r"check\s+your\s+price", re.IGNORECASE)
_AMZ_COMPLICATED_NEW_DEAL_FOUND_PATTERN = re.compile(r"\bnew\s+deal\s+found\b", re.IGNORECASE)
_AMZ_COMPLICATED_BEFORE_REG_PATTERN = re.compile(r"before\s+reg(?:ular|\.?)\b", re.IGNORECASE)
# Placeholder / teaser prices like "$7.xx" or "xx $7.xx"
_AMZ_COMPLICATED_XX_PRICE_PATTERN = re.compile(
    r"(?:\$\s*\d*\.xx\b|\bxx\s*\$?\s*\d|\d+\s*\$?\s*\d+\.xx\b)",
    re.IGNORECASE,
)


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


# Conversational Amazon deal templates (often missing explicit amazon.com/amzn.to links).
# Based on your historical `logs/Datalogs/Amazon.json`, common phrases include:
# - "Use Promo Code"
# - "Free at checkout. Clip the coupon."
# - "Buy on Amazon"
# - "shipped and sold by amazon"
AMAZON_CONVERSATIONAL_DEAL_PATTERN = re.compile(
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
    r"|promo\s+drops"
    r"|lowest\s+ever\s+on\s+amazon"
    r"|free\s+at\s+checkout"
    r"|buy\s+on\s+amazon"
    r"|shipped\s+and\s+sold\s+by\s+amazon"
    r")\b",
    re.IGNORECASE,
)

# Chatty grocery / delivery / in-app price glitches (AMZ_DEALS bucket in settings — legacy name).
# Matches templates that often have no product URL in the embed body.
RETAIL_CONVERSATIONAL_DEAL_PATTERN = re.compile(
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

