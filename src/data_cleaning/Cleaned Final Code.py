import json
import os
import re
from typing import Dict, Tuple, List
from dateutil.parser import parse
import matplotlib.pyplot as plt

# Input and output directories
json_dir_path = r"C:\Users\20243314\OneDrive - TU Eindhoven\Desktop\Quartile 4\DBL Data Challenge\Dataset"
output_dir_path = r"C:\Users\20243314\OneDrive - TU Eindhoven\Desktop\Quartile 4\DBL Data Challenge\Cleaned Dataset"
plots_dir_path = os.path.join(output_dir_path, "plots")

os.makedirs(output_dir_path, exist_ok=True)
os.makedirs(plots_dir_path, exist_ok=True)

# Configurations for weird-account detection
CONFIG = {
    "WEIRD_CRITERIA": {
        "min_followers": 20,
        "max_age_days": 30,
        "score_threshold": 1.0
    },
    "min_text_length_for_langdetect": 30
}

# Language tolerance for specific airlines
AIRLINE_LANG_TOLERANCE = {
    "klm": "nl",
    "lufthansa": "de",
    "british_airways": "en",
    "airfrance": "fr"
}

# --- Spam filtering patterns ---
complaint_patterns = [
    r"\bscam\b",
    r"\bscammed?\b",
    r"\bovercharg(?:ed|e)?\b",
    r"\b(?:change fee|refund|compensate|insurance|fake ticket|rip[\s-]?off|fraud)\b"
]
promo_patterns = [
    r"\b(?:save|get)\s+\d{1,2}%\b",
    r"\bdeal\b",
    r"\bsale\b",
    r"\bbook\b"
]
short_url_re = re.compile(r"https?://(?:t\.co|bit\.ly|tinyurl\.com)/", re.IGNORECASE)
complaint_regex = [re.compile(p, re.IGNORECASE) for p in complaint_patterns]
promo_regex = [re.compile(p, re.IGNORECASE) for p in promo_patterns]

def is_complaint(text: str) -> bool:
    return any(r.search(text) for r in complaint_regex)

def is_promotional_spam(text: str) -> bool:
    return any(r.search(text) for r in promo_regex) and bool(short_url_re.search(text))

def extract_relevant_info(tweet: Dict) -> Dict:
    urls = tweet.get('entities', {}).get('urls', [])
    cleaned_urls = [{'url': u.get('url'), 'display_url': u.get('display_url')} for u in urls]

    return {
        'created_at': tweet.get('created_at'),
        'id': tweet.get('id'),
        'text': tweet.get('text') or tweet.get('extended_tweet', {}).get('full_text'),
        'lang': tweet.get('lang'),
        'retweet_count': tweet.get('retweet_count'),
        'favorite_count': tweet.get('favorite_count'),
        'in_reply_to_status_id': tweet.get('in_reply_to_status_id'),
        'in_reply_to_user_id': tweet.get('in_reply_to_user_id'),
        'in_reply_to_screen_name': tweet.get('in_reply_to_screen_name'),
        'is_quote_status': tweet.get('is_quote_status'),
        'quote_count': tweet.get('quote_count'),
        'reply_count': tweet.get('reply_count'),
        'place': tweet.get('place'),
        'favorited': tweet.get('favorited'),
        'retweeted': tweet.get('retweeted'),
        'user': {
            'id': tweet.get('user', {}).get('id'),
            'screen_name': tweet.get('user', {}).get('screen_name'),
            'name': tweet.get('user', {}).get('name'),
            'followers_count': tweet.get('user', {}).get('followers_count'),
            'friends_count': tweet.get('user', {}).get('friends_count'),
            'favourites_count': tweet.get('user', {}).get('favourites_count'),
            'statuses_count': tweet.get('user', {}).get('statuses_count'),
            'verified': tweet.get('user', {}).get('verified'),
            'location': tweet.get('user', {}).get('location'),
            'time_zone': tweet.get('user', {}).get('time_zone'),
            'created_at': tweet.get('user', {}).get('created_at')
        },
        'entities': {
            'hashtags': tweet.get('entities', {}).get('hashtags'),
            'user_mentions': tweet.get('entities', {}).get('user_mentions'),
            'urls': cleaned_urls,
            'symbols': tweet.get('entities', {}).get('symbols'),
        }
    }

def is_weird_account(user: Dict, tweet_created_at: str, tweet: Dict) -> Tuple[bool, List[str]]:
    if not user or not tweet:
        return True, ["missing_user_info_or_tweet"]

    score, reasons = 0.0, []
    followers = user.get('followers_count', 0)
    friends = user.get('friends_count', 0)
    statuses = user.get('statuses_count', 0)
    screen_name = user.get('screen_name', '')
    verified = user.get('verified', False)
    description = user.get('description', '')
    profile_image = user.get('profile_image_url', '')

    if followers < CONFIG["WEIRD_CRITERIA"]['min_followers'] and statuses > 5000:
        score += 0.8; reasons.append("low_followers_high_activity")
    if followers < CONFIG["WEIRD_CRITERIA"]['min_followers'] and friends > 500:
        score += 0.8 if not verified else 0.2; reasons.append("spammy_follow_ratio")
    if statuses < 10:
        score += 0.5; reasons.append("very_low_tweet_count")
    if not description:
        score += 0.5; reasons.append("empty_profile_description")
    if profile_image and 'default_profile' in profile_image.lower():
        score += 0.5; reasons.append("default_profile_image")
    if not verified:
        score += 0.2; reasons.append("not_verified")

    created_at = user.get('created_at', '')
    if created_at and tweet_created_at:
        try:
            user_created = parse(created_at, fuzzy=False)
            tweet_created = parse(tweet_created_at, fuzzy=False)
            age_days = max((tweet_created - user_created).days, 1)
            if age_days < CONFIG["WEIRD_CRITERIA"]['max_age_days'] and statuses > 500 and followers < CONFIG["WEIRD_CRITERIA"]['min_followers']:
                score += 0.8; reasons.append("new_active_no_followers")
            if statuses / age_days > 100:
                score += 0.8; reasons.append("high_tweet_frequency")
        except (ValueError, TypeError):
            reasons.append("skipped_age_check_invalid_date")

    text = (tweet.get('text') or '').lower()
    tweet_lang = tweet.get('lang', '')
    for airline, lang_code in AIRLINE_LANG_TOLERANCE.items():
        if airline in text and tweet_lang == lang_code:
            score -= 0.2; reasons.append(f"{airline}_native_lang_tolerance")

    threshold = CONFIG["WEIRD_CRITERIA"]['score_threshold']
    if 'lufthansa' in text or tweet_lang == 'de':
        threshold = 1.0
    if score >= threshold:
        reasons.append(f"score={score:.2f}")
        return True, reasons
    return False, []

def plot_summary_stats(stats: Dict[str, int]):
    categories = list(stats.keys())
    values = [stats[k] for k in categories]

    plt.figure(figsize=(10, 6))
    plt.bar(categories, values, edgecolor='black')
    plt.title("Tweet Processing Summary")
    plt.xlabel("Category")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    output_path = os.path.join(plots_dir_path, 'summary_bar_chart.png')
    plt.savefig(output_path)
    plt.close()
    print(f"Saved summary plot to {output_path}")

def main():
    agg_stats = {
        'total_lines': 0,
        'processed': 0,
        'weird_accounts': 0,
        'tweets_saved': 0,
        'spam_dropped': 0
    }

    for filename in os.listdir(json_dir_path):
        if not filename.endswith('.json'):
            continue
        print(f"Processing file: {filename}")
        json_file = os.path.join(json_dir_path, filename)
        out_file = os.path.join(output_dir_path, f"cleaned_{filename}")

        cleaned = []
        with open(json_file, 'r', encoding='utf-8') as infile:
            for line in infile:
                agg_stats['total_lines'] += 1
                try:
                    tweet = json.loads(line)
                except json.JSONDecodeError:
                    continue

                agg_stats['processed'] += 1
                text = tweet.get('text') or tweet.get('extended_tweet', {}).get('full_text', '')

                if not is_complaint(text) and is_promotional_spam(text):
                    agg_stats['spam_dropped'] += 1
                    continue

                info = extract_relevant_info(tweet)
                weird, reasons = is_weird_account(tweet.get('user', {}), tweet.get('created_at'), tweet)
                info['weird_account'] = weird
                info['weird_reasons'] = reasons if weird else []
                if weird:
                    agg_stats['weird_accounts'] += 1
                else:
                    cleaned.append(info)
                    agg_stats['tweets_saved'] += 1

        with open(out_file, 'w', encoding='utf-8') as outfile:
            json.dump(cleaned, outfile, ensure_ascii=False, indent=2)

    plot_summary_stats(agg_stats)

if __name__ == '__main__':
    main()

