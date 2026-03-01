"""
WeddingLeadIntel AI - Lead Engine v2
India weddings only. 10-180 days window.
"""

import re
import asyncio
import logging
import json
import os
from datetime import datetime
from apify_client import ApifyClient
import anthropic

logger = logging.getLogger(__name__)

CITY_ALIASES = {
    "kochin": "kochi", "cochin": "kochi", "kochi kerala": "kochi",
    "bombay": "mumbai", "madras": "chennai", "calcutta": "kolkata",
    "bangalore": "bengaluru", "blr": "bengaluru",
    "trivandrum": "thiruvananthapuram", "tvm": "thiruvananthapuram",
    "trichur": "thrissur", "calicut": "kozhikode",
    "delhi": "new delhi", "ncr": "new delhi",
    "palghat": "palakkad", "quilon": "kollam",
    "alleppey": "alappuzha", "tanjore": "thanjavur",
    "mysore": "mysuru", "baroda": "vadodara",
    "poona": "pune", "simla": "shimla",
    "pondicherry": "puducherry", "gurgaon": "gurugram",
    "vizag": "visakhapatnam", "hyd": "hyderabad",
}

INDIAN_CITIES = [
    "mumbai", "delhi", "new delhi", "bengaluru", "hyderabad", "ahmedabad",
    "chennai", "kolkata", "surat", "pune", "jaipur", "lucknow", "kanpur",
    "nagpur", "indore", "thane", "bhopal", "visakhapatnam", "patna",
    "vadodara", "ghaziabad", "ludhiana", "agra", "nashik", "faridabad",
    "meerut", "rajkot", "varanasi", "srinagar", "aurangabad", "dhanbad",
    "amritsar", "prayagraj", "ranchi", "howrah", "coimbatore", "jabalpur",
    "gwalior", "vijayawada", "jodhpur", "madurai", "raipur",
    "kochi", "thrissur", "kozhikode", "thiruvananthapuram", "kollam",
    "alappuzha", "palakkad", "kannur", "malappuram", "ernakulam",
    "chandigarh", "mysuru", "hubli", "mangalore", "belgaum",
    "tirunelveli", "tiruchirapalli", "salem", "vellore", "erode",
    "tiruppur", "thoothukudi", "thanjavur", "dindigul",
    "goa", "panaji", "margao", "udaipur", "kota", "ajmer", "bikaner",
    "shimla", "dharamshala", "dehradun", "haridwar", "rishikesh",
    "bhubaneswar", "cuttack", "guwahati", "imphal", "shillong",
    "noida", "gurugram", "greater noida", "dwarka",
    "kerala", "karnataka", "tamilnadu", "tamil nadu", "rajasthan",
    "gujarat", "maharashtra", "punjab", "haryana", "bihar", "india",
]

CITY_TO_STATE = {
    "kochi": "Kerala", "thrissur": "Kerala", "kozhikode": "Kerala",
    "thiruvananthapuram": "Kerala", "kollam": "Kerala", "alappuzha": "Kerala",
    "palakkad": "Kerala", "kannur": "Kerala", "malappuram": "Kerala",
    "ernakulam": "Kerala",
    "mumbai": "Maharashtra", "pune": "Maharashtra", "nagpur": "Maharashtra",
    "nashik": "Maharashtra", "aurangabad": "Maharashtra", "thane": "Maharashtra",
    "chennai": "Tamil Nadu", "coimbatore": "Tamil Nadu", "madurai": "Tamil Nadu",
    "salem": "Tamil Nadu", "tirunelveli": "Tamil Nadu", "erode": "Tamil Nadu",
    "bengaluru": "Karnataka", "mysuru": "Karnataka", "hubli": "Karnataka",
    "mangalore": "Karnataka",
    "hyderabad": "Telangana", "visakhapatnam": "Andhra Pradesh",
    "vijayawada": "Andhra Pradesh",
    "kolkata": "West Bengal", "howrah": "West Bengal",
    "new delhi": "Delhi", "noida": "Uttar Pradesh", "gurugram": "Haryana",
    "jaipur": "Rajasthan", "udaipur": "Rajasthan", "jodhpur": "Rajasthan",
    "ahmedabad": "Gujarat", "surat": "Gujarat", "vadodara": "Gujarat",
    "lucknow": "Uttar Pradesh", "kanpur": "Uttar Pradesh", "agra": "Uttar Pradesh",
    "varanasi": "Uttar Pradesh", "prayagraj": "Uttar Pradesh",
    "patna": "Bihar", "bhopal": "Madhya Pradesh", "indore": "Madhya Pradesh",
    "chandigarh": "Punjab", "amritsar": "Punjab", "ludhiana": "Punjab",
    "guwahati": "Assam", "bhubaneswar": "Odisha", "raipur": "Chhattisgarh",
    "ranchi": "Jharkhand", "goa": "Goa", "panaji": "Goa",
    "shimla": "Himachal Pradesh", "dehradun": "Uttarakhand",
}

FOREIGN_CITIES = [
    "dubai", "london", "new york", "toronto", "sydney", "singapore",
    "los angeles", "chicago", "houston", "melbourne", "perth",
    "kuala lumpur", "bangkok", "hong kong", "tokyo", "paris",
    "berlin", "amsterdam", "rome", "madrid", "nyc", "usa",
    "canada", "uk", "australia", "uae", "qatar", "europe",
]

VENDOR_KEYWORDS = [
    "weddingplanner", "wedding_planner", "bridalmakeup", "bridal_makeup",
    "makeupartist", "makeup_artist", "weddingvenue", "wedding_venue",
    "weddingdecor", "wedding_decor", "mehendiartist", "mehendi_artist",
    "weddingflorist", "weddingcatering", "bridalstore", "weddingband",
    "weddingdj", "weddingcake", "weddingcard", "bridalwear",
    "weddingmandap", "weddingchoreographer", "bridalmua",
]

REJECT_KEYWORDS = [
    "photographer", "videographer", "cinematographer", "studio",
    "photoandvideo", "weddingphotog",
]

MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def is_indian_city(city: str) -> bool:
    city_lower = city.lower().strip()
    for fc in FOREIGN_CITIES:
        if fc in city_lower:
            return False
    canonical = CITY_ALIASES.get(city_lower, city_lower)
    if canonical in INDIAN_CITIES:
        return True
    for ic in INDIAN_CITIES:
        if ic in city_lower:
            return True
    return True


def normalize_city(city: str) -> dict:
    city_clean = city.lower().strip()
    canonical = CITY_ALIASES.get(city_clean, city_clean)
    state = CITY_TO_STATE.get(canonical, "India")
    south = ["Kerala", "Tamil Nadu", "Karnataka", "Andhra Pradesh", "Telangana", "Goa"]
    north = ["Punjab", "Haryana", "Uttar Pradesh", "Rajasthan", "Delhi", "Chandigarh"]
    east = ["West Bengal", "Odisha", "Assam", "Jharkhand", "Bihar"]
    west = ["Maharashtra", "Gujarat"]
    if state in south:
        region = "south"
    elif state in north:
        region = "north"
    elif state in east:
        region = "east"
    elif state in west:
        region = "west"
    else:
        region = "central"
    culture_map = {
        "Kerala": "christian_hindu_muslim",
        "Punjab": "sikh",
        "Rajasthan": "hindu_destination",
        "Goa": "christian_destination",
        "Tamil Nadu": "hindu_tamil",
        "West Bengal": "hindu_bengali",
        "Gujarat": "hindu_jain",
        "Maharashtra": "hindu_marathi",
        "Karnataka": "hindu_kannada",
        "Andhra Pradesh": "hindu_telugu",
        "Telangana": "hindu_telugu",
    }
    culture = culture_map.get(state, "hindu")
    display = f"{canonical.title()}, {state}" if state != "India" else canonical.title()
    return {
        "city": canonical,
        "state": state,
        "region": region,
        "culture": culture,
        "display": display,
    }


def get_vendor_accounts(city: str, state: str) -> list:
    b = city.replace(" ", "").lower()
    s = state.replace(" ", "").lower()
    patterns = [
        f"bridalmakeup{b}", f"makeupartist{b}", f"bridalmua{b}", f"muabride{b}",
        f"{b}bridalmakeup", f"{b}makeupartist", f"{b}bridalmua",
        f"weddingplanner{b}", f"{b}weddingplanner", f"weddingplanning{b}",
        f"{b}events", f"{b}weddingco", f"{b}weddinghouse",
        f"mehendiartist{b}", f"mehendi{b}", f"{b}mehendi",
        f"hennaartist{b}", f"{b}henna", f"mehendidesign{b}",
        f"weddingdecor{b}", f"{b}weddingdecor", f"eventdecor{b}",
        f"{b}decorator", f"weddingdecorator{b}", f"florist{b}", f"{b}florist",
        f"weddingvenue{b}", f"{b}venue", f"{b}banquet",
        f"{b}palace", f"{b}resorts", f"{b}gardens",
        f"bridalstore{b}", f"{b}bridalstore", f"bridalwear{b}",
        f"{b}lehenga", f"weddinglehenga{b}", f"{b}bridal",
        f"catering{b}", f"{b}catering", f"weddingcatering{b}", f"{b}caterers",
        f"bridalmakeup{s}", f"weddingplanner{s}", f"mehendi{s}",
        f"weddingdecor{s}", f"bridalstore{s}",
    ]
    seen = set()
    unique = []
    for v in patterns:
        if v not in seen and len(v) > 5:
            seen.add(v)
            unique.append(v)
    return unique[:50]


def generate_hashtags(city_data: dict) -> list:
    city = city_data["city"]
    state = city_data["state"]
    culture = city_data["culture"]
    region = city_data["region"]
    b = city.lower().replace(" ", "")
    s = state.lower().replace(" ", "")

    tier1 = [
        f"#{b}bride", f"#{b}bride2025", f"#{b}bride2026",
        f"#{b}groom", f"#{b}groom2025", f"#{b}engaged",
        f"#{b}engagement", f"#{b}wedding2025", f"#{b}wedding2026",
        f"#savethedate{b}", f"#bridetobe{b}", f"#futuremrs{b}",
        f"#{b}bridetobe", f"#{b}weddingsoon", f"#soontobe{b}",
    ]
    tier2 = [
        f"#{b}wedding", f"#{b}mehendi", f"#{b}haldi", f"#{b}sangeet",
        f"#{b}reception2025", f"#{b}nikah2025", f"#{b}weddingday",
        f"#{b}weddingprep", f"#{b}bridalshopping", f"#{b}bridalmakeup",
        f"#{b}weddingvenue", f"#{b}bridal", f"#{b}destinationwedding",
        f"#{s}wedding2025", f"#{s}bride2025", f"#{s}wedding2026",
        f"#{b}weddingseason", f"#{b}groomtobe",
    ]
    intent = [
        "#weddingcountdown", "#weddingsoon", "#bigdaysoon",
        "#gettingmarried2025", "#gettingmarried2026",
        "#bridetobe2025", "#bridetobe2026", "#groomtobe2025",
        "#shesaidyes", "#hesaidyes", "#justengaged", "#newlyengaged",
        "#savethedate2025", "#savethedate2026",
        "#indianwedding2025", "#indianwedding2026",
        "#indianbride2025", "#indianbride2026",
        "#weddingseason2025", "#indianbride", "#indianwedding",
        "#weddingvibes", "#bridegoals", "#weddingplanning",
        "#weddingday2025", "#weddingday2026",
    ]
    regional = []
    if region == "north" or "sikh" in culture:
        regional += [
            f"#shaadi{b}", f"#baraat{b}", f"#dulhan{b}",
            "#shaadi2025", "#shaadi2026", "#dulhan2025",
            "#punjabiwedding", "#sikhwedding", "#anandkaraj",
            "#dulhandiaries", "#shaadiseason", "#punjabibride",
        ]
    if state == "Kerala" or "christian" in culture:
        regional += [
            "#keralawedding", "#keralawedding2025", "#keralabride",
            "#keralabride2025", "#christianweddingkerala",
            "#muslimweddingkerala", "#hinduweddingkerala",
            "#malayaliwedding", "#keralaengagement",
            "#malabarwedding", "#keralachristianwedding",
        ]
    if state == "Tamil Nadu" or "tamil" in culture:
        regional += [
            f"#kalyanam{b}", "#tamilwedding", "#tamilwedding2025",
            "#tamilbride", "#tamilgroom", "#southindianbride",
            "#tamilengagement", "#tamilnaduwedding",
        ]
    if state == "West Bengal" or "bengali" in culture:
        regional += [
            f"#biye{b}", "#bengaliwedding", "#bengaliwedding2025",
            "#bengalibride", "#bengaligroom", "#kolkatabride",
        ]
    if state in ["Andhra Pradesh", "Telangana"] or "telugu" in culture:
        regional += [
            "#teluguwedding2025", "#telugubride",
            "#pellikuthuru", "#muhurtham", "#hyderabadbride",
        ]
    if state == "Gujarat" or "jain" in culture:
        regional += [
            "#gujaratiwedding", f"#lagna{b}",
            "#gujaratibride", "#ahmedabadbride",
        ]
    if state == "Maharashtra" or "marathi" in culture:
        regional += [
            "#marathiwedding", "#marathibride",
            "#punebride", "#lagnasohala",
        ]
    if state in ["Rajasthan", "Goa"] or "destination" in culture:
        regional += [
            "#rajasthaniwedding", "#palacewedding",
            "#destinationweddingindia", "#jaipurwedding",
            "#udaipurwedding", "#goawedding",
        ]
    if state == "Karnataka" or "kannada" in culture:
        regional += [
            "#kannadawedding", "#karnatakawedding",
            "#bangalorewedding", "#bangalorebride",
        ]
    if "muslim" in culture:
        regional += [f"#nikah{b}", "#indiannikah", "#muslimweddingindia"]
    if "christian" in culture:
        regional += ["#christianweddingindia", "#churchweddingindia"]

    all_tags = tier1 + tier2 + intent + regional
    seen = set()
    unique = []
    for tag in all_tags:
        t = tag.lower()
        if t not in seen:
            seen.add(t)
            unique.append(tag)
    return unique[:150]


def is_vendor(username: str, bio: str = "") -> bool:
    text = (username + " " + bio).lower()
    if any(k in text for k in REJECT_KEYWORDS):
        return True
    return sum(1 for k in VENDOR_KEYWORDS if k in text) >= 1


def detect_timeline(caption: str):
    if not caption:
        return None
    today = datetime.now()
    text = re.sub(r'[^\x00-\x7F]+', ' ', caption.lower())

    m = re.search(r'(\d+)\s*(?:days?|sleeps?)\s*(?:to go|left|away|until|till|more)', text)
    if m:
        d = int(m.group(1))
        if 10 <= d <= 180:
            return d

    m = re.search(r't-?(\d+)', text)
    if m:
        d = int(m.group(1))
        if 10 <= d <= 180:
            return d

    m = re.search(r'(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})', text)
    if m:
        try:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if y < 100:
                y += 2000
            if 1 <= d <= 31 and 1 <= mo <= 12:
                diff = (datetime(y, mo, d) - today).days
                if 10 <= diff <= 180:
                    return diff
        except Exception:
            pass

    m = re.search(
        r'(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)'
        r'[a-z]*[\s,]+202[567]', text
    )
    if m:
        mn = MONTH_MAP.get(m.group(1).lower()[:3])
        if mn:
            year = 2026 if "2026" in text else 2027 if "2027" in text else 2025
            try:
                diff = (datetime(year, mn, 15) - today).days
                if 10 <= diff <= 180:
                    return diff
            except Exception:
                pass

    if re.search(r'next\s+month|coming\s+month', text):
        return 30
    if re.search(r'this\s+month', text):
        return 15
    return None


def normalize_uname(u: str) -> str:
    u = u.lower().strip().lstrip('@')
    u = re.sub(r'[._\-]', '', u)
    u = re.sub(r'\d+$', '', u)
    return u


def deduplicate(profiles: list) -> list:
    seen = {}
    unique = []
    for p in profiles:
        key = normalize_uname(p.get('username', ''))
        if not key:
            continue
        if key not in seen:
            seen[key] = len(unique)
            unique.append(p)
        else:
            idx = seen[key]
            unique[idx]['multi_source'] = True
            unique[idx]['priority'] = max(unique[idx].get('priority', 4) - 1, 1)
    unique.sort(key=lambda x: (
        not x.get('multi_source', False),
        x.get('priority', 4),
        x.get('days_until_wedding', 999) or 999,
    ))
    return unique


async def safe_apify_run(client: ApifyClient, actor_id: str, input_data: dict) -> list:
    for attempt in range(3):
        try:
            loop = asyncio.get_event_loop()
            run = await loop.run_in_executor(
                None, lambda: client.actor(actor_id).call(run_input=input_data)
            )
            items = []
            if run and run.get('defaultDatasetId'):
                for item in client.dataset(run['defaultDatasetId']).iterate_items():
                    items.append(item)
            logger.info(f"✅ {actor_id}: {len(items)} items")
            return items
        except Exception as e:
            err = str(e)
            logger.warning(f"⚠️ {actor_id} attempt {attempt + 1}: {err}")
            if "403" in err or "429" in err:
                await asyncio.sleep((attempt + 1) * 30)
            if attempt == 2:
                return []
    return []


def extract_profiles_from_posts(
        posts: list, city: str, state: str,
        source_type: str = "hashtag") -> list:
    profiles = []
    for post in posts:
        caption = post.get('caption', '') or ''
        post_url = post.get('url', '') or ''
        if not post_url.startswith('http'):
            sc = post.get('shortCode', '')
            post_url = f"https://instagram.com/p/{sc}" if sc else ''
        likes = post.get('likesCount', 0) or 0
        comments_c = post.get('commentsCount', 0) or 0
        timestamp = str(post.get('timestamp', '') or '')
        days = detect_timeline(caption)
        base_profile = {
            'post_caption': caption[:200],
            'post_url': post_url,
            'post_date': timestamp,
            'detected_city': city,
            'state': state,
            'multi_source': False,
            'likes_count': likes,
            'comments_count': comments_c,
            'days_until_wedding': days,
            'source_type': source_type,
            'follower_count': None,
            'is_private': None,
        }
        for user in (post.get('taggedUsers', []) or []):
            uname = user.get('username', '') if isinstance(user, dict) else str(user)
            uname = uname.strip().lstrip('@')
            if uname and not is_vendor(uname):
                profiles.append({
                    **base_profile,
                    'username': uname,
                    'priority': 1,
                    'tagged_by_vendor': True,
                })
        mentions = post.get('mentions', []) or re.findall(r'@(\w+)', caption)
        for uname in mentions:
            uname = uname.strip().lstrip('@')
            if uname and not is_vendor(uname):
                profiles.append({
                    **base_profile,
                    'username': uname,
                    'priority': 2,
                    'tagged_by_vendor': False,
                })
        author = post.get('ownerUsername', '') or ''
        if not author:
            a = post.get('author', {})
            author = a.get('username', '') if isinstance(a, dict) else str(a)
        if author.strip() and not is_vendor(author.strip()):
            profiles.append({
                **base_profile,
                'username': author.strip(),
                'priority': 3,
                'tagged_by_vendor': False,
            })
    return profiles


def filter_with_claude(profiles: list, city: str, ac) -> list:
    kept = []
    for i in range(0, min(len(profiles), 150), 10):
        batch = profiles[i:i + 10]
        batch_data = [{
            'username': p['username'],
            'caption': p['post_caption'],
            'days_until_wedding': p['days_until_wedding'],
            'tagged_by_vendor': p['tagged_by_vendor'],
            'priority': p['priority'],
            'source_type': p['source_type'],
            'multi_source': p['multi_source'],
        } for p in batch]

        prompt = f"""You are a lead analyst for an Indian wedding photographer.
City: {city}, India. Return JSON array only.

Profiles:
{json.dumps(batch_data, indent=2)}

For each profile return:
{{"username":"...","keep":true/false,"label":"bride/groom/unknown",
"wedding_month":"April 2025" or null,"days_estimate":number or null,
"confidence":0-100,"confidence_tier":"high/medium/low",
"is_private":true/false/null,"wedding_in_india":true/false,
"rejection_reason":null or "reason"}}

KEEP: real person, India wedding, 10-180 days, confidence>=75
REJECT: business, vendor, outside India, past wedding, fake, confidence<75
BOOST: tagged_by_vendor+15, multi_source+10, priority1+10
ONLY JSON array. No other text."""

        try:
            response = ac.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = response.content[0].text.strip()
            raw = re.sub(r'^```json?\s*', '', raw)
            raw = re.sub(r'\s*```$', '', raw)
            results = json.loads(raw)
            for result in results:
                if result.get('keep') and result.get('wedding_in_india', True):
                    for p in batch:
                        if p['username'] == result['username']:
                            p.update({
                                'confidence': result.get('confidence', 75),
                                'confidence_tier': result.get('confidence_tier', 'medium'),
                                'label': result.get('label', 'unknown'),
                                'wedding_month': result.get('wedding_month'),
                                'days_estimate': result.get('days_estimate'),
                                'is_private': result.get('is_private'),
                            })
                            kept.append(p)
                            break
        except Exception as e:
            logger.error(f"Claude error: {e}")
            for p in batch:
                if p.get('priority') == 1 and p.get('tagged_by_vendor'):
                    p.update({
                        'confidence': 75,
                        'confidence_tier': 'medium',
                        'label': 'unknown',
                        'wedding_month': None,
                        'days_estimate': p.get('days_until_wedding'),
                        'is_private': None,
                    })
                    kept.append(p)
    return kept


async def discover_leads(location: str) -> list:
    apify_token = os.environ.get('APIFY_API_TOKEN')
    if not apify_token:
        raise ValueError("APIFY_API_TOKEN not set!")
    apify_client = ApifyClient(apify_token)
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')
    ac = anthropic.Anthropic(api_key=anthropic_key) if anthropic_key else anthropic.Anthropic()

    city_data = normalize_city(location)
    city = city_data['city']
    state = city_data['state']
    hashtags = generate_hashtags(city_data)
    vendors = get_vendor_accounts(city, state)

    tier1 = [h.lstrip('#') for h in hashtags[:15]]
    tier2 = [h.lstrip('#') for h in hashtags[15:30]]
    tier3 = [h.lstrip('#') for h in hashtags[30:45]]

    all_profiles = []

    logger.info(f"[1/5] Tier1 hashtags for {city}...")
    p1 = extract_profiles_from_posts(
        await safe_apify_run(apify_client, "apify/instagram-scraper", {
            "searchType": "hashtag",
            "searchLimit": 30,
            "searchQueries": tier1,
        }), city, state, "hashtag")
    all_profiles.extend(p1)
    logger.info(f"Source 1: {len(p1)} profiles")
    await asyncio.sleep(30)

    logger.info(f"[2/5] Tier2 hashtags for {city}...")
    p2 = extract_profiles_from_posts(
        await safe_apify_run(apify_client, "apify/instagram-scraper", {
            "searchType": "hashtag",
            "searchLimit": 25,
            "searchQueries": tier2,
        }), city, state, "hashtag")
    all_profiles.extend(p2)
    logger.info(f"Source 2: {len(p2)} profiles")
    await asyncio.sleep(30)

    logger.info(f"[3/5] Tier3 hashtags for {city}...")
    p3 = extract_profiles_from_posts(
        await safe_apify_run(apify_client, "apify/instagram-scraper", {
            "searchType": "hashtag",
            "searchLimit": 20,
            "searchQueries": tier3,
        }), city, state, "hashtag")
    all_profiles.extend(p3)
    logger.info(f"Source 3: {len(p3)} profiles")
    await asyncio.sleep(30)

    logger.info(f"[4/5] Vendor batch 1 for {city}...")
    p4 = extract_profiles_from_posts(
        await safe_apify_run(apify_client, "apify/instagram-tagged-scraper", {
            "usernames": vendors[:25],
            "resultsLimit": 50,
        }), city, state, "vendor_tag")
    for p in p4:
        p['tagged_by_vendor'] = True
        p['priority'] = 1
    all_profiles.extend(p4)
    logger.info(f"Source 4: {len(p4)} profiles")
    await asyncio.sleep(30)

    logger.info(f"[5/5] Vendor batch 2 for {city}...")
    p5 = extract_profiles_from_posts(
        await safe_apify_run(apify_client, "apify/instagram-tagged-scraper", {
            "usernames": vendors[25:50],
            "resultsLimit": 50,
        }), city, state, "vendor_tag")
    for p in p5:
        p['tagged_by_vendor'] = True
        p['priority'] = 1
    all_profiles.extend(p5)
    logger.info(f"Source 5: {len(p5)} profiles")

    logger.info(f"Total raw: {len(all_profiles)}")
    unique = deduplicate(all_profiles)
    logger.info(f"After dedup: {len(unique)}")
    filtered = filter_with_claude(unique, city, ac)
    logger.info(f"After Claude: {len(filtered)}")

    filtered.sort(key=lambda x: (
        not x.get('multi_source', False),
        0 if x.get('confidence_tier') == 'high'
        else 1 if x.get('confidence_tier') == 'medium' else 2,
        x.get('days_estimate', 999) or 999,
    ))
    return filtered[:50]
