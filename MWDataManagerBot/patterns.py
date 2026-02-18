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
    r"core\s*set\s*\d+|m\d+|"
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

