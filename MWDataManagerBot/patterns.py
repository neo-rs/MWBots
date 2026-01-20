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
    r"gingerbread|nutcracker|ornament|winter|snowman"
    r")\b",
    re.IGNORECASE,
)
SNEAKERS_PATTERN = re.compile(
    r"\b(nike|jordan|yeezy|adidas|reebok|puma|new\s*balance|crocs|vans|converse|asics|brooks|saucony|skechers|sneakers?|shoes?|kicks|footwear)\b",
    re.IGNORECASE,
)
CARDS_PATTERN = re.compile(
    r"\b(pok[eé]mon|topps\s*chrome|panini|magic\s*the\s*gathering|mtg|yu[\s-]?gi[\s-]?oh|sports?\s*cards?|nba\s*cards?|nfl\s*cards?|mlb\s*cards?|trading\s*cards?|tcg|ccg|pokemon)\b",
    re.IGNORECASE,
)

# Theatre
THEATRE_STORE_PATTERN = re.compile(
    "|".join(
        [
            r"cinemark",
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

# Global triggers
FULL_SEND_PATTERN = re.compile(
    r"\b(price\s+error|glitch|instant\s+sellout|live\s+early|went\s+live\s+again|surprise\s+drop|surprise\s+release)\b",
    re.IGNORECASE,
)
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
    "build-a-bear",
    "mattel creations",
    "starbucks",
    "dunkin",
]
DISCOUNTED_STORES = [
    "burlington",
    "marshalls",
    "tj maxx",
    "ross",
    "five below",
    "dollar tree",
    "dollar general",
    "aldi",
    "lidl",
    "homegoods",
    "ollie's",
    "big lots",
    "burke's outlet",
    "dd's discounts",
    "city trends",
    "rainbow shops",
    "roses",
    "grocery outlet",
    "daiso",
    "popshelf",
]

MAJOR_STORE_PATTERN = re.compile("|".join([re.escape(s) for s in MAJOR_STORES]), re.IGNORECASE)
DISCOUNTED_STORE_PATTERN = re.compile("|".join([re.escape(s) for s in DISCOUNTED_STORES]), re.IGNORECASE)
ALL_STORE_PATTERN = re.compile("|".join([re.escape(s) for s in (MAJOR_STORES + DISCOUNTED_STORES)]), re.IGNORECASE)

# Domain mapping (used for affiliated link / store-domain routing)
STORE_DOMAINS = {
    "amazon.com": "amazon",
    "amzn.to": "amazon",
    "a.co": "amazon",
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

