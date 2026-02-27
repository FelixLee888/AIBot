#!/usr/bin/env python3
"""Chief Fafa recipe pipeline.

Given a recipe URL, this script:
1) Crawls and extracts recipe details + image URL.
2) Builds a multi-format content pack (web copy, Facebook, Instagram, YouTube).
3) Attempts to create a Google Docs note with the generated content.

It always prints a report, even when one or more integrations fail.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urljoin, urlparse

import requests

REQUEST_TIMEOUT = 25
OPENAI_REQUEST_TIMEOUT = 55
OPENAI_MAX_RETRIES = 2
OPENAI_RETRY_BACKOFF_SEC = 1.5
USER_AGENT = "ChiefFafaBot/1.0 (+https://t.me/ChiefFafaBot)"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
OPENAI_TRANSCRIPTIONS_URL = "https://api.openai.com/v1/audio/transcriptions"
DOCS_API_CREATE_URL = "https://docs.googleapis.com/v1/documents"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_DRIVE_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id"
GOOGLE_DRIVE_API_BASE = "https://www.googleapis.com/drive/v3/files"
MAX_EMBED_IMAGE_BYTES = 8 * 1024 * 1024
DOCS_SUPPORTED_IMAGE_MIME = {"image/jpeg", "image/png", "image/gif"}
TRANSCRIPT_MAX_CHARS = 24000
VIDEO_TEXT_MAX_CHARS = 24000
TRANSCRIPT_MODEL = "gpt-4o-mini-transcribe"
DOC_IMAGE_MARKER = "[[CHIEF_FAFA_IMAGE_HERE]]"
URL_PATTERN = re.compile(r"https?://[^\s<>\"]+", flags=re.IGNORECASE)

VIDEO_URL_HINTS = [
    "youtube.com",
    "youtu.be",
    "vimeo.com",
    "tiktok.com",
    "instagram.com",
    "facebook.com",
    "threads.com",
    "x.com",
    "twitter.com",
    "bilibili.com",
]

ENQUIRY_KEYWORDS = [
    "find recipe",
    "search recipe",
    "saved recipe",
    "previous recipe",
    "old recipe",
    "look up recipe",
    "what did i save",
    "history",
    "enquiry",
    "query",
    "lookup",
    "搜尋",
    "搜索",
    "查詢",
    "查找",
    "之前",
    "以前",
    "已存",
    "食譜",
    "食谱",
    "料理",
    "レシピ",
    "조회",
    "검색",
    "레시피",
]

ENQUIRY_STOPWORDS = {
    "a",
    "an",
    "the",
    "to",
    "for",
    "of",
    "in",
    "on",
    "and",
    "or",
    "is",
    "are",
    "my",
    "me",
    "please",
    "recipe",
    "recipes",
    "find",
    "search",
    "saved",
    "previous",
    "show",
    "look",
    "up",
}

INGREDIENT_HEADING_KEYWORDS = [
    "ingredients",
    "ingredient",
    "材料",
    "材料一覧",
    "材料リスト",
    "食材リスト",
    "食材",
    "配料",
    "調味",
    "调味",
    "醬汁",
    "酱汁",
    "재료",
    "양념",
    "소스",
]

METHOD_HEADING_KEYWORDS = [
    "method",
    "methods",
    "instruction",
    "instructions",
    "direction",
    "directions",
    "steps",
    "preparation",
    "做法",
    "作法",
    "步驟",
    "步骤",
    "料理步驟",
    "料理步骤",
    "作法與步驟",
    "作り方",
    "手順",
    "工程",
    "下準備",
    "レシピ",
    "만드는 법",
    "조리법",
    "조리 방법",
    "조리순서",
    "방법",
    "순서",
]

STOP_SECTION_KEYWORDS = [
    "notes",
    "note",
    "tips",
    "tip",
    "nutrition",
    "video",
    "影片",
    "视频",
    "貼士",
    "贴士",
    "備註",
    "备注",
    "心得",
    "コツ",
    "メモ",
    "ポイント",
    "팁",
    "주의",
    "비고",
]

ENV_FALLBACK_FILES = [
    Path(__file__).resolve().parent.parent / ".env",
    Path("/home/felixlee/.openclaw/.env"),
    Path("/home/felixlee/Desktop/chief-fafa/.env"),
    Path.home() / ".openclaw/.env",
    Path.home() / "Desktop/chief-fafa/.env",
]


def read_env_value(name: str, default: str = "") -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value

    for env_file in ENV_FALLBACK_FILES:
        try:
            if not env_file.exists():
                continue
        except OSError:
            continue
        try:
            for raw_line in env_file.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, raw_val = line.split("=", 1)
                if key.strip() != name:
                    continue
                cleaned = raw_val.strip().strip("'\"")
                if cleaned:
                    return cleaned
        except Exception:
            continue

    return default


def env_flag(name: str, default: bool = False) -> bool:
    raw = read_env_value(name, "1" if default else "0").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def is_fast_mode_enabled() -> bool:
    return env_flag("CHIEF_FAFA_FAST_MODE", False)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def decode_html_text(text: str) -> str:
    return normalize_space(unescape(text or ""))


def normalize_multiline_text(text: str) -> str:
    raw = unescape(text or "")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    lines = [normalize_space(x) for x in raw.split("\n")]
    kept = [x for x in lines if x]
    return "\n".join(kept).strip()


def unique_clean_lines(items: Iterable[str], max_items: int = 220) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        line = decode_html_text(item)
        line = re.sub(r"^[\s\-*•●▪■★☆※\d\.\)\(、:：]+", "", line).strip()
        if len(line) < 2:
            continue
        if line.lower().startswith(("http://", "https://")):
            continue
        key = line.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(line)
        if len(out) >= max_items:
            break
    return out


def split_ingredient_candidates(text: str) -> List[str]:
    raw = unescape(text or "")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    raw = raw.replace("\u00a0", " ").replace("\u3000", " ")
    raw = re.sub(r"\s*\*\s*", "\n", raw)
    raw = re.sub(r"[•●▪■★☆※]\s*", "\n", raw)
    parts = [normalize_space(p) for p in raw.split("\n") if normalize_space(p)]
    return parts


def split_step_candidates(text: str) -> List[str]:
    raw = unescape(text or "")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")
    raw = raw.replace("\u00a0", " ").replace("\u3000", " ")
    chunks: List[str] = []
    for part in raw.split("\n"):
        part = normalize_space(part)
        if not part:
            continue
        numbered = re.split(r"(?=(?:^|\s)(?:\d{1,2}|[一二三四五六七八九十])[\.、\):：])", part)
        if len(numbered) > 1:
            for item in numbered:
                item = normalize_space(item)
                if item:
                    chunks.append(item)
        else:
            chunks.append(part)
    return chunks


def html_to_text_lines(html: str) -> List[str]:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|li|h[1-6]|div|tr|ul|ol|section|article|header|footer)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\u00a0", " ")
    lines = [normalize_space(x) for x in text.splitlines()]
    return [x for x in lines if x]


def is_heading_line(line: str, keywords: List[str]) -> bool:
    low = line.casefold()
    if len(line) > 90:
        return False
    return any(k in low for k in keywords)


def heading_keywords_pattern(keywords: List[str]) -> str:
    ordered = sorted({k for k in keywords if k}, key=len, reverse=True)
    if not ordered:
        return r"(?:)"
    return "(?:" + "|".join(re.escape(k) for k in ordered) + ")"


def is_heading_start_line(line: str, keywords: List[str]) -> bool:
    patt = heading_keywords_pattern(keywords)
    return bool(
        re.search(
            rf"^\s*(?:[-*•●▪■★☆※]\s*)?{patt}(?:\s*[\(\[（【].{{0,40}}?[\)\]）】])?\s*(?:[:：]|$)",
            line,
            flags=re.IGNORECASE,
        )
    )


def heading_inline_tail(line: str, keywords: List[str]) -> str:
    patt = heading_keywords_pattern(keywords)
    m = re.search(
        rf"^\s*(?:[-*•●▪■★☆※]\s*)?{patt}(?:\s*[\(\[（【].{{0,40}}?[\)\]）】])?\s*(?:[:：]|[-—–])?\s*(.*)$",
        line,
        flags=re.IGNORECASE,
    )
    if not m:
        return ""
    tail = normalize_space(m.group(1) or "")
    if not tail:
        return ""
    # Treat pure heading aliases as empty tails.
    if re.fullmatch(r"[\(\[（【].{0,40}[\)\]）】]", tail):
        return ""
    return tail


def looks_like_ingredient_line(line: str) -> bool:
    low = line.casefold()
    if any(k in low for k in INGREDIENT_HEADING_KEYWORDS):
        return False
    if re.search(
        r"(\d+(\.\d+)?\s*(g|kg|ml|l|tbsp|tsp|cup|cups|oz|lb|pcs|pc|片|隻|只|個|克|公斤|毫升|茶匙|汤匙|湯匙|大さじ|小さじ|컵|큰술|작은술|그램|개))",
        low,
    ):
        return True
    if any(x in low for x in ["適量", "适量", "to taste", "少許", "少许"]):
        return True
    if len(line) <= 120 and not line.startswith("http"):
        return True
    return False


def looks_like_step_line(line: str) -> bool:
    if re.match(r"^\s*(\d{1,2}|[一二三四五六七八九十])[\.、\):：]", line):
        return True
    if len(line) >= 18:
        return True
    return False


def extract_sections_from_lines(lines: List[str]) -> Tuple[List[str], List[str]]:
    ingredients: List[str] = []
    steps: List[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if is_heading_start_line(line, INGREDIENT_HEADING_KEYWORDS):
            j = i + 1
            block: List[str] = []
            inline_tail = heading_inline_tail(line, INGREDIENT_HEADING_KEYWORDS)
            if inline_tail:
                block.extend(split_ingredient_candidates(inline_tail))
            while j < len(lines):
                cur = lines[j]
                if is_heading_start_line(cur, METHOD_HEADING_KEYWORDS) or is_heading_start_line(cur, STOP_SECTION_KEYWORDS):
                    break
                if is_heading_start_line(cur, INGREDIENT_HEADING_KEYWORDS):
                    j += 1
                    continue
                if looks_like_ingredient_line(cur):
                    block.extend(split_ingredient_candidates(cur))
                j += 1
                if len(block) >= 260:
                    break
            ingredients.extend(block)
            i = j
            continue

        if is_heading_start_line(line, METHOD_HEADING_KEYWORDS):
            j = i + 1
            block = []
            inline_tail = heading_inline_tail(line, METHOD_HEADING_KEYWORDS)
            if inline_tail:
                block.extend(split_step_candidates(inline_tail))
            while j < len(lines):
                cur = lines[j]
                if is_heading_start_line(cur, INGREDIENT_HEADING_KEYWORDS) or is_heading_start_line(cur, STOP_SECTION_KEYWORDS):
                    break
                if is_heading_start_line(cur, METHOD_HEADING_KEYWORDS):
                    j += 1
                    continue
                if looks_like_step_line(cur):
                    block.extend(split_step_candidates(cur))
                j += 1
                if len(block) >= 300:
                    break
            steps.extend(block)
            i = j
            continue
        i += 1

    return unique_clean_lines(ingredients), unique_clean_lines(steps)


def extract_sections_by_regex(text_blob: str) -> Tuple[List[str], List[str]]:
    low_blob = text_blob
    ingredients: List[str] = []
    steps: List[str] = []

    ing_kw = heading_keywords_pattern(INGREDIENT_HEADING_KEYWORDS)
    method_kw = heading_keywords_pattern(METHOD_HEADING_KEYWORDS)
    stop_kw = heading_keywords_pattern(STOP_SECTION_KEYWORDS)

    ingredient_match = re.search(
        rf"(?:{ing_kw})(?:\s*[\(\[（【].{{0,40}}?[\)\]）】])?\s*[:：]\s*(.+?)(?:(?:{method_kw})(?:\s*[\(\[（【].{{0,40}}?[\)\]）】])?\s*[:：]|$)",
        low_blob,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if ingredient_match:
        block = ingredient_match.group(1)
        for part in split_ingredient_candidates(block):
            if looks_like_ingredient_line(part):
                ingredients.append(part)

    method_match = re.search(
        rf"(?:{method_kw})(?:\s*[\(\[（【].{{0,40}}?[\)\]）】])?\s*[:：]\s*(.+)",
        low_blob,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if method_match:
        block = method_match.group(1)
        # stop at common terminal sections if present
        stop = re.split(
            rf"(?:\n\s*(?:{stop_kw})(?:\s*[\(\[（【].{{0,40}}?[\)\]）】])?\s*[:：])",
            block,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        for part in split_step_candidates(stop):
            if looks_like_step_line(part):
                steps.append(part)

    return unique_clean_lines(ingredients), unique_clean_lines(steps)


def extract_recipe_sections_from_text_blob(text_blob: str) -> Tuple[List[str], List[str]]:
    if not text_blob.strip():
        return [], []
    lines = [normalize_space(x) for x in text_blob.splitlines() if normalize_space(x)]
    section_ingredients, section_steps = extract_sections_from_lines(lines)
    regex_ingredients, regex_steps = extract_sections_by_regex(text_blob)
    ingredients = unique_clean_lines(section_ingredients + regex_ingredients, max_items=260)
    steps = unique_clean_lines(section_steps + regex_steps, max_items=320)

    if not steps and lines:
        synthetic_steps = [
            x
            for x in lines
            if looks_like_step_line(x)
            and len(x) <= 280
            and not is_heading_start_line(x, INGREDIENT_HEADING_KEYWORDS)
            and not is_heading_start_line(x, STOP_SECTION_KEYWORDS)
        ]
        steps = unique_clean_lines(synthetic_steps, max_items=80)
    return ingredients, steps


def thumbnail_from_video_metadata(meta: Dict[str, Any]) -> str:
    thumb = str(meta.get("thumbnail", "")).strip()
    if thumb and re.search(r"\.(jpg|jpeg|png|gif)(?:$|\?)", thumb, flags=re.IGNORECASE):
        return thumb
    thumbs = meta.get("thumbnails")
    if isinstance(thumbs, list):
        preferred = ""
        preferred_h = -1
        best = ""
        best_h = -1
        for item in thumbs:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url", "")).strip()
            if not url:
                continue
            height = int(item.get("height", 0) or 0)
            is_supported = bool(re.search(r"\.(jpg|jpeg|png|gif)(?:$|\?)", url, flags=re.IGNORECASE))
            if is_supported and height > preferred_h:
                preferred_h = height
                preferred = url
            if height > best_h:
                best_h = height
                best = url
        if preferred:
            return preferred
        if best:
            return best
    return ""


def extract_video_text_sources(final_url: str, initial_description: str) -> Dict[str, str]:
    out: Dict[str, str] = {"description": normalize_multiline_text(initial_description), "transcript": "", "error": ""}
    meta, meta_err = fetch_video_metadata_with_ytdlp(final_url)
    if meta_err:
        if not has_meaningful_text(out.get("description", ""), min_chars=24):
            out["error"] = meta_err
        return out

    meta_description = normalize_multiline_text(str(meta.get("description", "")))
    if has_meaningful_text(meta_description, min_chars=20):
        out["description"] = meta_description

    preferred_lang = str(meta.get("language", "")).strip()
    subtitle_url = select_caption_track_url(meta.get("subtitles"), preferred_lang=preferred_lang)
    if not subtitle_url:
        subtitle_url = select_caption_track_url(meta.get("automatic_captions"), preferred_lang=preferred_lang)

    transcript_text = ""
    transcript_err = ""
    if subtitle_url:
        transcript_text, transcript_err = fetch_caption_text(subtitle_url)

    need_transcription = (not transcript_text) and (not has_meaningful_text(out.get("description", ""), min_chars=24))
    if need_transcription and is_fast_mode_enabled():
        transcript_err = "fast mode skipped audio transcription"
    elif need_transcription:
        audio_path, audio_err = download_video_audio_with_ytdlp(final_url)
        if audio_path:
            transcript_text, transcript_err = transcribe_audio_with_openai(audio_path)
            try:
                Path(audio_path).unlink(missing_ok=True)
                Path(audio_path).parent.rmdir()
            except Exception:
                pass
        else:
            transcript_err = audio_err

    if transcript_text:
        if len(transcript_text) > VIDEO_TEXT_MAX_CHARS:
            transcript_text = transcript_text[:VIDEO_TEXT_MAX_CHARS].rstrip() + "..."
        out["transcript"] = transcript_text
    elif transcript_err and not has_meaningful_text(out.get("description", ""), min_chars=24):
        out["error"] = transcript_err

    if not out.get("error"):
        title = decode_html_text(str(meta.get("title", "")))
        thumbnail = decode_html_text(thumbnail_from_video_metadata(meta))
        if title:
            out["title"] = title
        if thumbnail:
            out["thumbnail_url"] = thumbnail
    return out


def normalize_title_for_chat(text: str) -> str:
    title = decode_html_text(text)
    if " on Instagram" in title:
        title = title.split(" on Instagram", 1)[0].strip()
    for sep in (" | ", " - ", " · "):
        if sep in title:
            left = title.split(sep, 1)[0].strip()
            if len(left) >= 10:
                title = left
                break
    if len(title) > 120:
        title = title[:117].rstrip() + "..."
    return title


def script_profile(text: str) -> Dict[str, int]:
    stats = {
        "latin": 0,
        "han": 0,
        "hirakata": 0,
        "hangul": 0,
        "arabic": 0,
        "cyrillic": 0,
        "devanagari": 0,
        "thai": 0,
        "hebrew": 0,
        "greek": 0,
    }
    for ch in text:
        code = ord(ch)
        if ("a" <= ch.lower() <= "z") or (0x00C0 <= code <= 0x024F):
            stats["latin"] += 1
        elif 0x4E00 <= code <= 0x9FFF:
            stats["han"] += 1
        elif (0x3040 <= code <= 0x309F) or (0x30A0 <= code <= 0x30FF):
            stats["hirakata"] += 1
        elif 0xAC00 <= code <= 0xD7AF:
            stats["hangul"] += 1
        elif (0x0600 <= code <= 0x06FF) or (0x0750 <= code <= 0x077F) or (0x08A0 <= code <= 0x08FF):
            stats["arabic"] += 1
        elif 0x0400 <= code <= 0x052F:
            stats["cyrillic"] += 1
        elif 0x0900 <= code <= 0x097F:
            stats["devanagari"] += 1
        elif 0x0E00 <= code <= 0x0E7F:
            stats["thai"] += 1
        elif 0x0590 <= code <= 0x05FF:
            stats["hebrew"] += 1
        elif 0x0370 <= code <= 0x03FF:
            stats["greek"] += 1
    return stats


def dominant_script_group(text: str) -> str:
    p = script_profile(text)
    if p["hangul"] >= max(p["latin"], p["han"], p["hirakata"]) and p["hangul"] >= 2:
        return "hangul"
    if p["hirakata"] >= 2:
        return "japanese"
    if p["han"] >= p["latin"] and p["han"] >= 2:
        return "han"
    if p["latin"] >= 2:
        return "latin"
    return "unknown"


def title_segment_score(segment: str, target: str) -> float:
    p = script_profile(segment)
    if target == "hangul":
        return float(p["hangul"])
    if target == "japanese":
        return float(p["hirakata"] * 2 + p["han"] * 0.5)
    if target == "han":
        return float(p["han"])
    if target == "latin":
        return float(p["latin"])
    return 0.0


def cleanup_title_text(text: str) -> str:
    title = decode_html_text(text)
    title = re.sub(r"https?://\S+", "", title).strip()
    title = re.sub(r"(?:\s*#\S+){2,}$", "", title).strip()
    title = re.sub(r"\s{2,}", " ", title).strip()
    title = re.sub(r"[:：|/·\-\u2014]+$", "", title).strip()
    return title


def title_looks_generic(title: str) -> bool:
    low = title.casefold().strip()
    if not low:
        return True
    generic_prefixes = [
        "video by ",
        "reel by ",
        "post by ",
        "instagram",
        "youtube",
        "tiktok",
    ]
    if any(low.startswith(p) for p in generic_prefixes):
        return True
    if low.endswith(" on instagram"):
        return True
    return False


def derive_title_from_text(text: str) -> str:
    raw = decode_html_text(text)
    raw = re.sub(r"https?://\S+", "", raw)
    lines = [normalize_space(x) for x in raw.splitlines() if normalize_space(x)]
    if not lines:
        return ""
    for line in lines[:10]:
        candidate = re.sub(r"(?:\s*#\S+)+$", "", line).strip()
        candidate = candidate.strip(" -|·/:\t")
        candidate = re.split(r"[💖💕❤️✨⭐️🌟🟡🟠🟢🔴🔵🟣✅❌❣️✿]", candidate, maxsplit=1)[0].strip()
        if len(candidate) < 6:
            continue
        if candidate.casefold().startswith(("ingredients", "method", "directions", "steps", "做法", "材料")):
            continue
        segs = [cleanup_title_text(x) for x in re.split(r"[|/｜／]", candidate) if cleanup_title_text(x)]
        cjk_segs = []
        for seg in segs:
            p = script_profile(seg)
            if (p["han"] + p["hirakata"] + p["hangul"]) >= 2 and len(seg) <= 64:
                target = dominant_script_group(seg)
                filtered = filter_title_tokens_by_script(seg, target)
                cjk_segs.append(filtered or seg)
        if cjk_segs:
            return cjk_segs[0]
        if len(candidate) > 140:
            candidate = candidate[:140].rstrip() + "..."
        return cleanup_title_text(candidate)
    return ""


def filter_title_tokens_by_script(title: str, target: str) -> str:
    tokens = [t for t in title.split() if t.strip()]
    if not tokens:
        return ""

    def keep_token(tok: str) -> bool:
        p = script_profile(tok)
        if target == "hangul":
            return p["hangul"] > 0
        if target == "japanese":
            return (p["hirakata"] > 0) or (p["han"] > 0 and p["latin"] == 0)
        if target == "han":
            return p["han"] > 0
        if target == "latin":
            return p["latin"] > 0
        return False

    kept = [tok for tok in tokens if keep_token(tok)]
    if not kept:
        return ""
    out = " ".join(kept).strip()
    out = cleanup_title_text(out)
    return out


def refine_title_for_content_language(title: str, context_text: str) -> str:
    clean_title = cleanup_title_text(title)
    if not clean_title:
        return clean_title
    target = dominant_script_group(context_text)
    if target == "unknown":
        return clean_title

    raw_parts = re.split(r"\s(?:\||-|·|/|\u2014)\s|[|/]\s*|:\s+", clean_title)
    parts = [cleanup_title_text(p) for p in raw_parts if cleanup_title_text(p)]
    if not parts:
        return clean_title

    best_part = clean_title
    best_score = title_segment_score(clean_title, target)
    for part in parts:
        score = title_segment_score(part, target)
        if score > best_score:
            best_part = part
            best_score = score

    if best_score >= 2:
        filtered = filter_title_tokens_by_script(best_part, target)
        if filtered and len(filtered) >= 2:
            return filtered
        return best_part
    filtered = filter_title_tokens_by_script(clean_title, target)
    if filtered and len(filtered) >= 2:
        return filtered
    return clean_title


def strip_diagnostic_suffix(text: str) -> str:
    cleaned = decode_html_text(text)
    cleaned = normalize_space(cleaned)
    if not cleaned:
        return ""

    lower = cleaned.lower()
    markers = [
        "ai fallback:",
        "openai call failed",
        "httpsconnectionpool(",
        "readtimeout",
        "traceback",
        "error:",
    ]
    cut_idx = min([idx for idx in (lower.find(m) for m in markers) if idx >= 0], default=-1)
    if cut_idx >= 0:
        cleaned = cleaned[:cut_idx]

    cleaned = re.sub(r"\([^()]*$", "", cleaned).strip()
    cleaned = re.sub(r"[:：;,\-]+$", "", cleaned).strip()
    return normalize_space(cleaned)


def is_noise_or_error_line(text: str) -> bool:
    low = decode_html_text(text).casefold()
    if not low:
        return True

    noise_markers = [
        "window.wiz_global_data",
        "window.ytcfg",
        "__next_data__",
        "ytcfg.set(",
        "function(",
        "<!doctype html",
        "<html",
        "</script",
        "javascript:",
        "source map",
    ]
    error_markers = [
        "traceback",
        "exception",
        "error:",
        "openai call failed",
        "httpsconnectionpool",
        "readtimeout",
        "command still running",
        "no session found",
        "allocation failed",
        "fatal error",
    ]
    if any(marker in low for marker in noise_markers):
        return True
    if any(marker in low for marker in error_markers):
        return True
    if low.startswith(("window.", "var ", "const ", "let ")):
        return True

    # Drop script-like long blobs that are unlikely to be recipe content.
    punct = sum(1 for ch in low if ch in "{}[]<>;=|\\")
    if len(low) >= 120 and punct >= 8:
        return True
    return False


def sanitize_doc_text_block(text: str, max_chars: int) -> str:
    raw = unescape(str(text or "")).replace("\r\n", "\n").replace("\r", "\n")
    kept: List[str] = []
    for line in raw.split("\n"):
        clean = decode_html_text(line)
        if not clean:
            continue
        if is_noise_or_error_line(clean):
            continue
        kept.append(clean)
    if not kept:
        return ""
    cleaned = "\n".join(kept)
    cleaned = strip_diagnostic_suffix(cleaned)
    if len(cleaned) > max_chars:
        cleaned = cleaned[: max_chars - 3].rstrip() + "..."
    return cleaned


def sanitize_recipe_lines_for_doc(lines: List[str], max_items: int) -> List[str]:
    out: List[str] = []
    for line in unique_clean_lines(lines, max_items=max_items * 2):
        if is_noise_or_error_line(line):
            continue
        out.append(line)
        if len(out) >= max_items:
            break
    return out


def is_detailed_video_description(text: str) -> bool:
    clean = decode_html_text(text)
    if len(clean) >= 280:
        return True
    low = clean.casefold()
    has_ing = any(k in low for k in INGREDIENT_HEADING_KEYWORDS)
    has_step = any(k in low for k in METHOD_HEADING_KEYWORDS)
    if has_ing and has_step:
        return True
    line_count = len([x for x in clean.splitlines() if normalize_space(x)])
    return line_count >= 10


def short_chat_summary(source: Dict[str, Any], formats: Dict[str, str], source_error: str) -> str:
    url = str(source.get("url", ""))
    domain = urlparse(url).netloc.lower()
    title = normalize_title_for_chat(str(source.get("title", ""))) or "Untitled recipe"
    ai_summary = decode_html_text(str(formats.get("summary", "")))
    ai_summary = re.sub(r"https?://\S+", "", ai_summary).strip()
    ai_summary = strip_diagnostic_suffix(ai_summary)

    ingredients = [decode_html_text(str(x)) for x in source.get("ingredients", []) if str(x).strip()]
    top_ingredients = ", ".join(ingredients[:4])
    is_video = str(source.get("content_type", "")).strip().lower() == "video"
    is_text_recipe = str(source.get("content_type", "")).strip().lower() == "text_recipe_input"
    missing_fields = source.get("input_validation_missing", [])
    if not isinstance(missing_fields, list):
        missing_fields = []
    input_language = decode_html_text(str(source.get("input_language", ""))).strip()

    if is_text_recipe and missing_fields:
        missing_label = summarize_text_recipe_validation([str(x) for x in missing_fields])
        lang_part = f" ({input_language})" if input_language else ""
        summary = f"Recipe text received{lang_part}, but missing {missing_label}."
    elif is_text_recipe:
        lang_part = f" ({input_language})" if input_language else ""
        if top_ingredients:
            summary = f"Recipe text processed{lang_part}: {title}. Key ingredients: {top_ingredients}."
        else:
            summary = f"Recipe text processed{lang_part}: {title}."
    elif "instagram.com" in domain:
        if top_ingredients:
            summary = f"Instagram reel recipe: {title}. Key ingredients: {top_ingredients}."
        else:
            summary = f"Instagram reel recipe captured: {title}."
    elif "threads.com" in domain:
        if top_ingredients:
            summary = f"Threads post recipe: {title}. Key ingredients: {top_ingredients}."
        else:
            summary = f"Threads post recipe captured: {title}."
    elif is_video:
        if top_ingredients:
            summary = f"Video recipe captured: {title}. Key ingredients: {top_ingredients}."
        else:
            summary = f"Video recipe captured: {title}."
    else:
        summary = ai_summary or f"Recipe captured: {title}."
        if len(summary) < 40 and top_ingredients:
            summary = f"{summary} Key ingredients: {top_ingredients}."

    if source_error:
        summary = f"Source crawl failed; generated fallback content. {summary}"
    summary = normalize_space(summary)
    if len(summary) > 220:
        summary = summary[:217].rstrip() + "..."
    return summary


def decorate_summary_with_attempt_status(
    summary: str,
    initial_url: str,
    host_hint: str,
    has_processing_error: bool,
    auto_review_triggers: List[str],
) -> str:
    out = normalize_space(summary)
    if not out:
        out = "Processing completed."

    host = host_hint.strip().lower()
    if not host and initial_url:
        try:
            host = urlparse(initial_url).netloc.lower().strip()
        except Exception:
            host = ""

    if initial_url and has_processing_error:
        attempt_text = f"Tried processing URL source ({host}) first." if host else "Tried processing this URL first."
        if "tried processing" not in out.casefold():
            out = f"{attempt_text} {out}".strip()

    auto_review_hit = any(str(x).strip().endswith(":triggered") for x in auto_review_triggers)
    if has_processing_error and auto_review_hit and "self-improve" not in out.casefold():
        out = f"{out} Self-improve review triggered."

    out = normalize_space(out)
    if len(out) > 220:
        out = out[:217].rstrip() + "..."
    return out


def human_readable_doc_summary(source: Dict[str, Any], formats: Dict[str, str]) -> str:
    title = normalize_title_for_chat(str(source.get("title", ""))) or "Untitled recipe"
    description = decode_html_text(str(source.get("description", "")))
    description = normalize_space(description)
    description = re.sub(r"https?://\S+", "", description).strip()
    description = strip_diagnostic_suffix(description)
    video_description = decode_html_text(str(source.get("video_description", "")))
    video_description = normalize_space(video_description)
    video_description = re.sub(r"https?://\S+", "", video_description).strip()
    video_description = strip_diagnostic_suffix(video_description)
    transcript = decode_html_text(str(source.get("video_transcript", "")))
    transcript = normalize_space(transcript)
    transcript = strip_diagnostic_suffix(transcript)

    ai_summary = decode_html_text(str(formats.get("summary", "")))
    ai_summary = re.sub(r"https?://\S+", "", ai_summary).strip()
    ai_summary = strip_diagnostic_suffix(ai_summary)

    preferred = ai_summary if len(ai_summary) >= 18 else ""
    if not preferred and has_meaningful_text(video_description, min_chars=18):
        preferred = video_description
    if not preferred:
        preferred = description
    if not preferred and has_meaningful_text(transcript, min_chars=18):
        preferred = transcript[:320]
    if not preferred:
        ingredients = [decode_html_text(str(x)) for x in source.get("ingredients", []) if str(x).strip()]
        if ingredients:
            preferred = f"{title}. Key ingredients include {', '.join(ingredients[:5])}."
        else:
            preferred = f"{title}. Recipe extracted from the source page."

    if len(preferred) > 600:
        preferred = preferred[:597].rstrip() + "..."
    return preferred


def build_reply_message(summary: str, google_doc_status: str, google_doc_url: str, error_message: str) -> str:
    lines: List[str] = []
    lines.append(f"Summary: {normalize_space(summary)}")
    lines.append(f"Google Doc status: {normalize_space(google_doc_status)}")
    lines.append(f"Google Doc URL: {normalize_space(google_doc_url)}")
    clean_error = normalize_space(error_message)
    if clean_error:
        lines.append(f"Error: {clean_error}")
    return "\n".join(lines)


def first_non_empty(values: Iterable[Optional[str]]) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


def extract_first_url(text: str) -> str:
    if not text:
        return ""
    match = URL_PATTERN.search(text)
    if not match:
        return ""
    url = match.group(0).strip()
    url = url.rstrip(").,;!?\"'")
    return url


def extract_enquiry_terms(text: str, max_terms: int = 8) -> List[str]:
    clean = decode_html_text(text).casefold()
    if not clean:
        return []

    latin_terms = re.findall(r"[a-z0-9][a-z0-9\-]{1,30}", clean)
    cjk_terms = re.findall(r"[\u3400-\u9fff\u3040-\u30ff\uac00-\ud7af]{2,16}", clean)
    merged = latin_terms + cjk_terms
    out: List[str] = []
    seen: set[str] = set()
    for term in merged:
        t = term.strip()
        if not t:
            continue
        if len(t) < 2:
            continue
        if t in ENQUIRY_STOPWORDS:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= max_terms:
            break
    return out


def score_text_against_terms(text: str, terms: List[str], whole_query: str) -> float:
    hay = decode_html_text(text).casefold()
    if not hay:
        return 0.0
    score = 0.0
    for term in terms:
        if term in hay:
            count = hay.count(term)
            score += 1.0 + min(2.0, count * 0.2)
    whole = normalize_space(whole_query).casefold()
    if whole and len(whole) >= 3 and whole in hay:
        score += 2.5
    return score


def choose_snippet(text: str, terms: List[str], max_chars: int = 240) -> str:
    lines = [normalize_space(x) for x in str(text or "").splitlines() if normalize_space(x)]
    if not lines:
        return ""
    low_terms = [t.casefold() for t in terms if t]
    def is_machine_line(line: str) -> bool:
        trimmed = line.strip()
        return (trimmed.startswith("{") and "\"type\"" in trimmed) or (trimmed.startswith("[") and "{\"" in trimmed)

    for line in lines:
        low = line.casefold()
        if is_machine_line(line):
            continue
        if any(t in low for t in low_terms):
            return line[:max_chars]
    for line in lines:
        if not is_machine_line(line):
            return line[:max_chars]
    return lines[0][:max_chars]


def looks_like_structured_recipe_text(text: str) -> bool:
    lines = [normalize_space(x) for x in str(text or "").splitlines() if normalize_space(x)]
    if not lines:
        return False
    heading_ing = any(is_heading_line(line, INGREDIENT_HEADING_KEYWORDS) for line in lines[:120])
    heading_steps = any(is_heading_line(line, METHOD_HEADING_KEYWORDS) for line in lines[:120])
    ingredients, steps = extract_recipe_sections_from_text_blob("\n".join(lines))
    if heading_ing and heading_steps:
        return True
    if len(ingredients) >= 3 and len(steps) >= 2:
        return True
    return False


def looks_like_recipe_enquiry(text: str) -> bool:
    raw = unescape(str(text or "")).strip()
    if not raw:
        return False
    if extract_first_url(raw):
        return False
    if looks_like_structured_recipe_text(raw):
        return False
    clean = decode_html_text(raw)
    low = clean.casefold()
    has_keyword = any(k in low for k in ENQUIRY_KEYWORDS)
    if not has_keyword:
        has_action = bool(re.search(r"\b(find|search|lookup|look up|show|list)\b", low))
        has_recipe_context = bool(re.search(r"\b(recipe|recipes|saved|history|previous|old)\b", low))
        has_keyword = has_action and has_recipe_context
    has_question = ("?" in clean) or ("？" in clean)
    return has_keyword or has_question


def read_text_safely(path: Path, max_chars: int = 250000) -> str:
    try:
        data = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    if len(data) > max_chars:
        return data[:max_chars]
    return data


def is_accessible_dir(path: Path) -> bool:
    try:
        return path.exists() and path.is_dir()
    except OSError:
        return False


def is_accessible_file(path: Path) -> bool:
    try:
        return path.exists() and path.is_file()
    except OSError:
        return False


def extract_doc_url_from_text(text: str) -> str:
    m = re.search(r"https://docs\.google\.com/document/d/[A-Za-z0-9_-]+/edit", text)
    return m.group(0) if m else ""


def extract_source_url_from_text(text: str) -> str:
    for line in text.splitlines():
        clean = normalize_space(line)
        low = clean.casefold()
        if low.startswith("source url:") or low.startswith("original page url:"):
            parts = clean.split(":", 1)
            if len(parts) == 2:
                url = extract_first_url(parts[1])
                if url:
                    return url
    return ""


def normalize_recipe_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        p = urlparse(raw)
    except Exception:
        return raw
    scheme = (p.scheme or "https").lower()
    netloc = (p.netloc or "").lower()

    # Canonicalize YouTube variants to reduce duplicate recipe docs for the same video.
    if netloc in {"youtu.be", "www.youtu.be"}:
        video_id = p.path.strip("/").split("/")[0]
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"
    if netloc.endswith("youtube.com"):
        path = p.path or "/"
        q = dict(parse_qsl(p.query, keep_blank_values=False))
        video_id = q.get("v", "").strip()
        if not video_id and path.startswith("/shorts/"):
            video_id = path.split("/shorts/", 1)[1].split("/", 1)[0].strip()
        if not video_id and path.startswith("/embed/"):
            video_id = path.split("/embed/", 1)[1].split("/", 1)[0].strip()
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"
    if "instagram.com" in netloc:
        netloc = "www.instagram.com"
        path = (p.path or "/").strip("/")
        parts = [x for x in path.split("/") if x]
        if len(parts) >= 2 and parts[0] in {"reel", "p", "tv"}:
            return f"https://{netloc}/{parts[0]}/{parts[1]}/"
        if len(parts) >= 2 and parts[0] == "stories":
            return f"https://{netloc}/stories/{parts[1]}/"
    if "facebook.com" in netloc:
        netloc = "www.facebook.com"
        path = (p.path or "/").strip("/")
        parts = [x for x in path.split("/") if x]
        q = dict(parse_qsl(p.query, keep_blank_values=False))
        if parts and parts[0] in {"reel", "watch"}:
            if parts[0] == "reel" and len(parts) >= 2:
                return f"https://{netloc}/reel/{parts[1]}"
            if parts[0] == "watch" and q.get("v"):
                return f"https://{netloc}/watch?v={q.get('v','').strip()}"
        if len(parts) >= 3 and parts[-2] in {"videos", "reels"}:
            return f"https://{netloc}/{parts[-2]}/{parts[-1]}"
        if q.get("v"):
            return f"https://{netloc}/watch?v={q.get('v','').strip()}"
    if "threads.com" in netloc:
        netloc = "www.threads.com"
        path = (p.path or "/").strip("/")
        parts = [x for x in path.split("/") if x]
        if len(parts) >= 3 and parts[1] == "post":
            return f"https://{netloc}/{parts[0]}/post/{parts[2]}"
        if len(parts) >= 2 and parts[0] == "post":
            return f"https://{netloc}/post/{parts[1]}"

    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]
    path = p.path or "/"
    if path != "/":
        path = path.rstrip("/")
    kept_query: List[str] = []
    if p.query:
        for part in p.query.split("&"):
            if not part:
                continue
            key = part.split("=", 1)[0].strip().lower()
            if key.startswith("utm_"):
                continue
            if key in {"fbclid", "gclid", "igshid", "si", "ref"}:
                continue
            kept_query.append(part)
    query = "&".join(sorted(kept_query))
    rebuilt = f"{scheme}://{netloc}{path}"
    if query:
        rebuilt += f"?{query}"
    return rebuilt


def canonical_host_from_url(url: str) -> str:
    norm = normalize_recipe_url(url)
    if not norm:
        return ""
    try:
        host = urlparse(norm).netloc.lower()
    except Exception:
        return ""
    for pref in ("www.", "m.", "mbasic.", "mobile."):
        if host.startswith(pref):
            host = host[len(pref) :]
            break
    return host


def read_json_file_safely(path: Path) -> Any:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_json_file_safely(path: Path, data: Any) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def detect_and_record_new_url_host(url: str) -> Tuple[bool, str]:
    host = canonical_host_from_url(url)
    if not host:
        return False, ""
    hosts_path = Path(
        read_env_value(
            "CHIEF_FAFA_SEEN_HOSTS_FILE",
            "/home/felixlee/Desktop/chief-fafa/.pi/seen_url_hosts.json",
        )
    )
    data = read_json_file_safely(hosts_path)
    if not isinstance(data, dict):
        data = {"hosts": []}
    hosts = data.get("hosts", [])
    if not isinstance(hosts, list):
        hosts = []
    known = {str(x).strip().lower() for x in hosts if str(x).strip()}
    is_new = host not in known
    if is_new:
        known.add(host)
        data["hosts"] = sorted(known)
        write_json_file_safely(hosts_path, data)
    return is_new, host


def maybe_trigger_auto_review(
    reason: str,
    source_url: str,
    error_message: str = "",
    input_snippet: str = "",
) -> str:
    enabled_raw = read_env_value("CHIEF_FAFA_AUTO_REVIEW_ENABLED", "1").strip().lower()
    if enabled_raw in {"0", "false", "no", "off"}:
        return "disabled"

    script_path = Path(
        read_env_value(
            "CHIEF_FAFA_AUTO_REVIEW_SCRIPT",
            "/home/felixlee/Desktop/chief-fafa/scripts/chief_fafa_auto_review.py",
        )
    )
    if not script_path.exists():
        return "script_missing"

    cmd = [
        "/usr/bin/python3",
        str(script_path),
        "--reason",
        reason[:120],
    ]
    if source_url:
        cmd.extend(["--source-url", source_url[:500]])
    if error_message:
        cmd.extend(["--error", error_message[:1200]])
    if input_snippet:
        cmd.extend(["--input-snippet", input_snippet[:1200]])

    try:
        subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return "triggered"
    except Exception as exc:
        return f"trigger_failed:{exc.__class__.__name__}"


def extract_summary_from_report_text(text: str) -> str:
    lines = [x.rstrip() for x in str(text or "").splitlines()]
    for idx, line in enumerate(lines):
        if normalize_space(line).casefold() == "summary:":
            for j in range(idx + 1, min(len(lines), idx + 7)):
                cand = normalize_space(lines[j])
                if cand:
                    return cand[:240]
    return ""


def find_existing_doc_in_notes_by_url(source_url: str, notes_root: Path, limit_files: int = 450) -> Dict[str, Any]:
    norm_url = normalize_recipe_url(source_url)
    if not norm_url or not is_accessible_dir(notes_root):
        return {"found": False}

    files = sorted(notes_root.glob("*.md"), key=lambda p: p.stat().st_mtime if is_accessible_file(p) else 0, reverse=True)
    latest_seen: Dict[str, Any] = {"found": False}
    for path in files[:limit_files]:
        if "-enquiry-" in path.name:
            continue
        raw = read_text_safely(path, max_chars=200000)
        if not raw:
            continue
        report_url = extract_source_url_from_text(raw)
        if not report_url:
            continue
        if normalize_recipe_url(report_url) != norm_url:
            continue
        title = extract_title_from_report_text(raw, path.stem.replace("-", " "))
        summary = extract_summary_from_report_text(raw)
        doc_url = extract_doc_url_from_text(raw)
        candidate = {
            "found": bool(doc_url),
            "match_type": "notes_url_match",
            "title": title,
            "summary": summary,
            "source_url": report_url,
            "normalized_url": norm_url,
            "doc_url": doc_url,
            "note_path": str(path),
            "modified": int(path.stat().st_mtime),
        }
        if doc_url:
            return candidate
        if not latest_seen.get("note_path"):
            latest_seen = candidate
    return latest_seen


def google_drive_find_existing_doc_by_source_url(source_url: str, limit: int = 8) -> Tuple[Dict[str, Any], str]:
    token, err = resolve_docs_access_token()
    if not token:
        return {"found": False}, err or "missing Google Docs token"

    norm_url = normalize_recipe_url(source_url)
    if not norm_url:
        return {"found": False}, "source url missing"
    parsed = urlparse(norm_url)
    host = parsed.netloc.lower()
    tail = Path(parsed.path).name.strip().lower()
    terms = [host] if host else []
    if tail and tail not in terms:
        terms.append(tail)
    if not terms:
        return {"found": False}, "url terms missing"

    clauses = []
    for term in terms[:2]:
        esc = escape_drive_query_literal(term)
        clauses.append(f"fullText contains '{esc}'")
    query = "mimeType='application/vnd.google-apps.document' and trashed=false and (" + " and ".join(clauses) + ")"

    headers = {"Authorization": f"Bearer {token}"}
    quota_project = resolve_docs_quota_project()
    if quota_project:
        headers["X-Goog-User-Project"] = quota_project
    params = {
        "q": query,
        "spaces": "drive",
        "pageSize": max(1, min(25, int(limit))),
        "orderBy": "modifiedTime desc",
        "fields": "files(id,name,webViewLink,modifiedTime)",
    }
    try:
        resp = requests.get(
            f"{GOOGLE_DRIVE_API_BASE}",
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        data = resp.json() if resp.text else {}
    except Exception as exc:
        return {"found": False}, f"Google Docs URL search failed ({exc.__class__.__name__}: {exc})"
    if resp.status_code >= 400:
        detail = extract_google_api_error(data)
        return {"found": False}, f"Google Docs URL search HTTP {resp.status_code}: {detail or 'request failed'}"

    files = data.get("files", []) if isinstance(data, dict) else []
    if not isinstance(files, list):
        files = []
    for item in files[:limit]:
        if not isinstance(item, dict):
            continue
        file_id = str(item.get("id", "")).strip()
        if not file_id:
            continue
        title = decode_html_text(str(item.get("name", ""))).strip() or "Untitled document"
        doc_url = str(item.get("webViewLink", "")).strip()
        if not doc_url:
            doc_url = f"https://docs.google.com/document/d/{file_id}/edit"

        verified = False
        try:
            export_resp = requests.get(
                f"{GOOGLE_DRIVE_API_BASE}/{file_id}/export",
                headers=headers,
                params={"mimeType": "text/plain"},
                timeout=REQUEST_TIMEOUT,
            )
            if export_resp.status_code < 400:
                body = export_resp.text or ""
                if source_url in body or norm_url in body:
                    verified = True
        except Exception:
            verified = False

        if verified:
            return (
                {
                    "found": True,
                    "match_type": "google_docs_url_match",
                    "title": title,
                    "summary": f"Existing Google Doc matched source URL ({host}).",
                    "source_url": source_url,
                    "normalized_url": norm_url,
                    "doc_url": doc_url,
                    "note_path": "",
                    "modified": int(dt.datetime.now(dt.timezone.utc).timestamp()),
                },
                "",
            )
    return {"found": False}, ""


def find_existing_recipe_doc_for_url(source_url: str, notes_root: Path) -> Dict[str, Any]:
    note_hit = find_existing_doc_in_notes_by_url(source_url, notes_root=notes_root)
    if bool(note_hit.get("found")) and str(note_hit.get("doc_url", "")).strip():
        return note_hit

    drive_hit, drive_err = google_drive_find_existing_doc_by_source_url(source_url)
    if bool(drive_hit.get("found")) and str(drive_hit.get("doc_url", "")).strip():
        return drive_hit

    out = {"found": False}
    if note_hit.get("note_path"):
        out["note_hit_without_doc"] = note_hit
    if drive_err:
        out["check_error"] = drive_err
    return out


def extract_title_from_report_text(text: str, fallback: str) -> str:
    for line in text.splitlines():
        clean = normalize_space(line)
        low = clean.casefold()
        if low.startswith("title:") or low.startswith("recipe title:"):
            parts = clean.split(":", 1)
            if len(parts) == 2:
                title = decode_html_text(parts[1])
                if title:
                    return title[:180]
    if fallback:
        return fallback
    return "Untitled recipe"


def search_markdown_files_for_enquiry(query_text: str, root: Path, source_name: str, limit: int = 8) -> List[Dict[str, Any]]:
    if not is_accessible_dir(root):
        return []
    terms = extract_enquiry_terms(query_text)
    if not terms:
        return []
    files = sorted(root.glob("*.md"), key=lambda p: p.stat().st_mtime if is_accessible_file(p) else 0, reverse=True)
    out: List[Dict[str, Any]] = []
    for path in files[:240]:
        if "-enquiry-" in path.name:
            continue
        raw = read_text_safely(path)
        if not raw:
            continue
        score = score_text_against_terms(raw, terms, query_text)
        if score <= 0:
            continue
        title = extract_title_from_report_text(raw, path.stem.replace("-", " "))
        out.append(
            {
                "source": source_name,
                "score": round(score, 3),
                "title": title,
                "doc_url": extract_doc_url_from_text(raw),
                "source_url": extract_source_url_from_text(raw),
                "snippet": choose_snippet(raw, terms),
                "path": str(path),
                "modified": int(path.stat().st_mtime),
            }
        )
    out.sort(key=lambda x: (float(x.get("score", 0.0)), int(x.get("modified", 0))), reverse=True)
    return out[:limit]


def search_session_history_for_enquiry(query_text: str, sessions_dir: Path, limit: int = 8) -> List[Dict[str, Any]]:
    if not is_accessible_dir(sessions_dir):
        return []
    terms = extract_enquiry_terms(query_text)
    if not terms:
        return []
    files = sorted(sessions_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime if is_accessible_file(p) else 0, reverse=True)
    out: List[Dict[str, Any]] = []
    for path in files[:220]:
        raw = read_text_safely(path, max_chars=120000)
        if not raw:
            continue
        score = score_text_against_terms(raw, terms, query_text) * 0.55
        if score <= 0:
            continue
        title = ""
        m = re.search(r'"summary"\s*:\s*"([^"]+)"', raw)
        if m:
            title = decode_html_text(m.group(1))
        if not title:
            m = re.search(r'"title"\s*:\s*"([^"]+)"', raw)
        if m:
            title = decode_html_text(m.group(1))
        if not title:
            title = path.stem
        out.append(
            {
                "source": "conversation_history",
                "score": round(score, 3),
                "title": title[:180],
                "doc_url": extract_doc_url_from_text(raw),
                "source_url": extract_first_url(raw),
                "snippet": choose_snippet(raw, terms),
                "path": str(path),
                "modified": int(path.stat().st_mtime),
            }
        )
    out.sort(key=lambda x: (float(x.get("score", 0.0)), int(x.get("modified", 0))), reverse=True)
    return out[:limit]


def resolve_docs_quota_project() -> str:
    return read_env_value(
        "GOOGLE_DOCS_QUOTA_PROJECT",
        read_env_value("GOOGLE_KEEP_QUOTA_PROJECT", ""),
    )


def escape_drive_query_literal(text: str) -> str:
    return str(text or "").replace("\\", "\\\\").replace("'", "\\'")


def google_drive_search_docs(query_text: str, limit: int = 8) -> Tuple[List[Dict[str, Any]], str]:
    token, err = resolve_docs_access_token()
    if not token:
        return [], err or "missing Google Docs token"

    terms = extract_enquiry_terms(query_text)
    if not terms:
        clean = normalize_space(query_text)
        if clean:
            terms = [clean[:32].casefold()]
    if not terms:
        return [], "query terms missing"

    clauses = []
    for term in terms[:4]:
        esc = escape_drive_query_literal(term)
        clauses.append(f"name contains '{esc}' or fullText contains '{esc}'")
    query = "mimeType='application/vnd.google-apps.document' and trashed=false and (" + " or ".join(clauses) + ")"

    headers = {"Authorization": f"Bearer {token}"}
    quota_project = resolve_docs_quota_project()
    if quota_project:
        headers["X-Goog-User-Project"] = quota_project
    params = {
        "q": query,
        "spaces": "drive",
        "pageSize": max(1, min(20, int(limit))),
        "orderBy": "modifiedTime desc",
        "fields": "files(id,name,webViewLink,modifiedTime)",
    }
    try:
        resp = requests.get(
            f"{GOOGLE_DRIVE_API_BASE}",
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        data = resp.json() if resp.text else {}
    except Exception as exc:
        return [], f"Google Docs search failed ({exc.__class__.__name__}: {exc})"
    if resp.status_code >= 400:
        detail = extract_google_api_error(data)
        return [], f"Google Docs search HTTP {resp.status_code}: {detail or 'request failed'}"
    files = data.get("files", []) if isinstance(data, dict) else []
    out: List[Dict[str, Any]] = []
    for item in files if isinstance(files, list) else []:
        if not isinstance(item, dict):
            continue
        title = decode_html_text(str(item.get("name", ""))).strip() or "Untitled document"
        out.append(
            {
                "source": "google_docs",
                "score": score_text_against_terms(title, terms, query_text),
                "title": title[:180],
                "doc_url": str(item.get("webViewLink", "")).strip(),
                "source_url": "",
                "snippet": f"Google Doc: {title}",
                "path": "",
                "modified": int(dt.datetime.now(dt.timezone.utc).timestamp()),
            }
        )
    out.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    return out[:limit], ""


def run_recipe_enquiry(query_text: str, output_dir: Path) -> Dict[str, Any]:
    notes_root = output_dir
    memory_root = Path(read_env_value("CHIEF_FAFA_MEMORY_ROOT", "/home/felixlee/Desktop/chief-fafa"))
    memory_dir = memory_root / "memory"
    memory_file = memory_root / "MEMORY.md"
    sessions_dir = Path(
        read_env_value(
            "CHIEF_FAFA_OPENCLAW_SESSIONS_DIR",
            "/home/felixlee/.openclaw/agents/chief-fafa/sessions",
        )
    )

    memory_hits = search_markdown_files_for_enquiry(query_text, memory_dir, "memory_daily", limit=5)
    if is_accessible_file(memory_file):
        memory_single = search_markdown_files_for_enquiry(query_text, memory_file.parent, "memory", limit=5)
        # Keep only MEMORY.md entries from this call.
        memory_hits.extend([x for x in memory_single if Path(str(x.get("path", ""))).name == "MEMORY.md"])
    note_hits = search_markdown_files_for_enquiry(query_text, notes_root, "notes", limit=8)
    history_hits = search_session_history_for_enquiry(query_text, sessions_dir, limit=8)
    docs_hits, docs_err = google_drive_search_docs(query_text, limit=8)

    source_rank = {"memory": 4, "memory_daily": 4, "notes": 3, "conversation_history": 2, "google_docs": 1}

    def ranked_key(item: Dict[str, Any]) -> Tuple[int, float, int]:
        src = str(item.get("source", ""))
        return (
            int(source_rank.get(src, 0)),
            float(item.get("score", 0.0)),
            int(item.get("modified", 0)),
        )

    all_local = memory_hits + note_hits + history_hits
    all_local.sort(key=ranked_key, reverse=True)
    deduped: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    docs_hits.sort(key=ranked_key, reverse=True)
    for item in all_local + docs_hits:
        key = (str(item.get("doc_url", "")) + "|" + str(item.get("title", "")).casefold()).strip("|")
        if not key:
            key = str(item.get("path", ""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(item)
        if len(deduped) >= 12:
            break

    top_doc_url = ""
    for item in deduped:
        doc_url = str(item.get("doc_url", "")).strip()
        if doc_url:
            top_doc_url = doc_url
            break

    top_titles = [str(x.get("title", "")).strip() for x in deduped[:3] if str(x.get("title", "")).strip()]
    if deduped:
        summary = f"Found {len(deduped)} matching saved recipes for '{normalize_space(query_text)[:60]}'."
        if top_titles:
            summary += f" Top match: {top_titles[0]}."
    else:
        summary = f"No saved recipe matched '{normalize_space(query_text)[:60]}' in memory/history or Google Docs."

    err_parts: List[str] = []
    if docs_err:
        err_parts.append(docs_err)
    error_message = " | ".join(err_parts).strip()
    doc_status = "found" if bool(top_doc_url) else "not_found"
    if doc_status == "not_found" and docs_err and not deduped:
        doc_status = "failed"

    return {
        "ok": True,
        "mode": "enquiry",
        "query": normalize_space(query_text),
        "summary": summary[:220],
        "google_doc_status": doc_status,
        "google_doc_url": top_doc_url,
        "error_message": error_message,
        "results": deduped,
        "source_counts": {
            "memory": len(memory_hits),
            "notes": len(note_hits),
            "conversation_history": len(history_hits),
            "google_docs": len(docs_hits),
        },
    }


def infer_text_language_label(text: str) -> str:
    clean = decode_html_text(text)
    if not clean:
        return "unknown"
    p = script_profile(clean)
    if p["hangul"] >= 3:
        return "korean"
    if p["hirakata"] >= 3:
        return "japanese"
    if p["han"] >= 3:
        cantonese_hints = [
            "佢",
            "咗",
            "嘅",
            "喺",
            "冇",
            "啲",
            "咁",
            "嚟",
            "靚",
            "餸",
        ]
        if any(h in clean for h in cantonese_hints):
            return "cantonese"
        traditional_hints = ["體", "麼", "這", "還", "與", "為", "會", "裡", "讓", "點"]
        simplified_hints = ["体", "么", "这", "还", "与", "为", "会", "里", "让", "点"]
        trad_score = sum(clean.count(x) for x in traditional_hints)
        simp_score = sum(clean.count(x) for x in simplified_hints)
        if trad_score > simp_score:
            return "chinese-traditional"
        if simp_score > trad_score:
            return "chinese-simplified"
        return "chinese"
    if p["arabic"] >= 3:
        return "arabic"
    if p["cyrillic"] >= 3:
        return "cyrillic"
    if p["devanagari"] >= 3:
        return "devanagari"
    if p["thai"] >= 3:
        return "thai"
    if p["hebrew"] >= 3:
        return "hebrew"
    if p["greek"] >= 3:
        return "greek"
    active = sum(1 for v in p.values() if v >= 3)
    if active >= 2:
        return "mixed"
    if p["latin"] >= 6:
        return "latin-script"
    return "unknown"


def extract_text_recipe_title(text: str) -> str:
    lines = [normalize_space(x) for x in text.splitlines() if normalize_space(x)]
    if not lines:
        return ""

    title_patterns = [
        r"^(?:title|recipe title|dish|recipe name|name)\s*[:：]\s*(.+)$",
        r"^(?:食譜名稱|菜名|料理名|名稱|名称|標題|标题)\s*[:：]\s*(.+)$",
        r"^(?:レシピ名|タイトル)\s*[:：]\s*(.+)$",
        r"^(?:요리 이름|레시피 이름|제목)\s*[:：]\s*(.+)$",
    ]
    for line in lines[:20]:
        for pat in title_patterns:
            m = re.match(pat, line, flags=re.IGNORECASE)
            if m:
                return cleanup_title_text(m.group(1))

    for line in lines[:10]:
        low = line.casefold()
        if is_heading_line(line, INGREDIENT_HEADING_KEYWORDS):
            continue
        if is_heading_line(line, METHOD_HEADING_KEYWORDS):
            continue
        if low.startswith(("ingredients", "ingredient", "method", "steps", "directions", "instructions")):
            continue
        if len(line) < 4:
            continue
        if len(line) > 140:
            continue
        if re.match(r"^\d+[.)、:：-]", line):
            continue
        return cleanup_title_text(line)
    return ""


def summarize_text_recipe_validation(missing: List[str]) -> str:
    label_map = {
        "recipe_name": "recipe name",
        "ingredients": "ingredients",
        "method_steps": "method/steps",
    }
    labels = [label_map.get(x, x) for x in missing]
    return ", ".join(labels)


def extract_source_payload_from_text(raw_text: str) -> Dict[str, Any]:
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("recipe text is empty")

    language = infer_text_language_label(text)
    title = extract_text_recipe_title(text)
    ingredients, steps = extract_recipe_sections_from_text_blob(text)
    description = ""
    lines = [normalize_space(x) for x in text.splitlines() if normalize_space(x)]
    for line in lines:
        if line == title:
            continue
        if is_heading_line(line, INGREDIENT_HEADING_KEYWORDS):
            continue
        if is_heading_line(line, METHOD_HEADING_KEYWORDS):
            continue
        if line in ingredients:
            continue
        if line in steps:
            continue
        description = line
        break

    ai_parse_note = ""
    need_ai_parse = (not title) or (not ingredients) or (not steps)
    if need_ai_parse:
        ai_parsed, ai_err = extract_text_recipe_with_openai(text, language)
        if ai_parsed:
            title = first_non_empty([title, str(ai_parsed.get("title", "")).strip(), derive_title_from_text(text)])
            description = first_non_empty([description, str(ai_parsed.get("description", "")).strip()])
            ingredients = unique_clean_lines(list(ingredients) + list(ai_parsed.get("ingredients", [])), max_items=320)
            steps = unique_clean_lines(list(steps) + list(ai_parsed.get("steps", [])), max_items=360)
            ai_lang = str(ai_parsed.get("language", "")).strip()
            if ai_lang and language in {"unknown", "mixed", "latin-script"}:
                language = ai_lang
        elif ai_err:
            ai_parse_note = ai_err

    if not title:
        title = derive_title_from_text(text)

    missing: List[str] = []
    if not title:
        missing.append("recipe_name")
    if not ingredients:
        missing.append("ingredients")
    if not steps:
        missing.append("method_steps")

    payload: Dict[str, Any] = {
        "url": "",
        "content_type": "text_recipe_input",
        "title": title or "Untitled recipe",
        "description": description,
        "image_url": "",
        "author": "",
        "prep_time": "",
        "cook_time": "",
        "total_time": "",
        "yield": "",
        "category": "",
        "cuisine": "",
        "ingredients": ingredients,
        "instructions": steps,
        "text_excerpt": text[:2400],
        "video_description": "",
        "video_transcript": "",
        "video_note": "",
        "input_language": language,
        "input_validation_missing": missing,
        "input_parse_note": ai_parse_note,
    }
    return payload


def extract_meta_content(html: str, key: str) -> str:
    patterns = [
        rf'<meta[^>]+(?:property|name)\s*=\s*["\']{re.escape(key)}["\'][^>]+content\s*=\s*["\']([^"\']+)["\']',
        rf'<meta[^>]+content\s*=\s*["\']([^"\']+)["\'][^>]+(?:property|name)\s*=\s*["\']{re.escape(key)}["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            return normalize_space(match.group(1))
    return ""


def strip_tags(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text or "")
    cleaned = cleaned.replace("&nbsp;", " ")
    return normalize_space(cleaned)


def extract_title(html: str) -> str:
    title_meta = first_non_empty(
        [
            extract_meta_content(html, "og:title"),
            extract_meta_content(html, "twitter:title"),
        ]
    )
    if title_meta:
        return title_meta
    match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return strip_tags(match.group(1))
    return ""


def extract_description(html: str) -> str:
    return first_non_empty(
        [
            extract_meta_content(html, "og:description"),
            extract_meta_content(html, "twitter:description"),
            extract_meta_content(html, "description"),
        ]
    )


def extract_image_url(html: str, base_url: str) -> str:
    raw = first_non_empty(
        [
            extract_meta_content(html, "og:image"),
            extract_meta_content(html, "twitter:image"),
        ]
    )
    if not raw:
        return ""
    return urljoin(base_url, decode_html_text(raw))


def parse_json_ld_blocks(html: str) -> List[Any]:
    blocks: List[Any] = []
    for match in re.finditer(
        r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        raw = (match.group(1) or "").strip()
        if not raw:
            continue
        try:
            blocks.append(json.loads(raw))
        except Exception:
            continue
    return blocks


def iter_json_objects(value: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for key in ("@graph", "itemListElement", "mainEntity"):
            nested = value.get(key)
            if isinstance(nested, list):
                for item in nested:
                    yield from iter_json_objects(item)
            elif isinstance(nested, dict):
                yield from iter_json_objects(nested)
    elif isinstance(value, list):
        for item in value:
            yield from iter_json_objects(item)


def is_recipe_object(obj: Dict[str, Any]) -> bool:
    t = obj.get("@type")
    if isinstance(t, str):
        return t.lower() == "recipe"
    if isinstance(t, list):
        return any(isinstance(x, str) and x.lower() == "recipe" for x in t)
    return False


def pick_recipe_obj(blocks: List[Any]) -> Dict[str, Any]:
    for block in blocks:
        for obj in iter_json_objects(block):
            if is_recipe_object(obj):
                return obj
    return {}


def is_video_object(obj: Dict[str, Any]) -> bool:
    t = obj.get("@type")
    if isinstance(t, str):
        return t.lower() == "videoobject"
    if isinstance(t, list):
        return any(isinstance(x, str) and x.lower() == "videoobject" for x in t)
    return False


def pick_video_obj(blocks: List[Any]) -> Dict[str, Any]:
    for block in blocks:
        for obj in iter_json_objects(block):
            if is_video_object(obj):
                return obj
    return {}


def has_meaningful_text(text: str, min_chars: int = 24) -> bool:
    clean = decode_html_text(text)
    clean = re.sub(r"https?://\S+", "", clean).strip()
    return len(clean) >= min_chars


def is_video_source_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    query = parsed.query.lower()
    if any(hint in host for hint in VIDEO_URL_HINTS):
        return True
    if any(tag in path for tag in ["/watch", "/video", "/videos", "/reel", "/shorts", "/clip", "/tv/", "/post/"]):
        return True
    if any(tag in query for tag in ["v=", "video", "watch"]):
        return True
    return False


def is_video_source_page(url: str, html: str, json_ld_blocks: List[Any]) -> bool:
    if is_video_source_url(url):
        return True
    og_type = extract_meta_content(html, "og:type").lower()
    if "video" in og_type:
        return True
    twitter_card = extract_meta_content(html, "twitter:card").lower()
    if twitter_card in {"player", "video"}:
        return True
    if pick_video_obj(json_ld_blocks):
        return True
    return False


def extract_video_from_json_ld(video_obj: Dict[str, Any], base_url: str) -> Dict[str, str]:
    if not video_obj:
        return {}
    thumb = coerce_image_url(video_obj.get("thumbnailUrl"), base_url)
    if not thumb:
        thumb = coerce_image_url(video_obj.get("image"), base_url)
    return {
        "name": first_non_empty([str(video_obj.get("name", "")).strip()]),
        "description": first_non_empty([str(video_obj.get("description", "")).strip()]),
        "thumbnail_url": thumb,
    }


def decode_js_escaped_text(value: str) -> str:
    if not value:
        return ""
    text = str(value)

    def _u4(match: re.Match[str]) -> str:
        try:
            return chr(int(match.group(1), 16))
        except Exception:
            return ""

    # Decode common JavaScript escapes found in embedded JSON blobs.
    text = re.sub(r"\\u([0-9a-fA-F]{4})", _u4, text)
    text = text.replace("\\n", "\n").replace("\\r", "\n").replace("\\t", " ")
    text = text.replace('\\"', '"').replace("\\/", "/").replace("\\\\", "\\")
    return normalize_multiline_text(text)


def extract_youtube_fast_fields_from_html(html: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not html:
        return out

    title_match = re.search(
        r'"videoDetails":\{.*?"title":"((?:[^"\\]|\\.)+)"',
        html,
        flags=re.DOTALL,
    )
    if title_match:
        title = cleanup_title_text(decode_js_escaped_text(title_match.group(1)))
        if title:
            out["title"] = title

    desc_match = re.search(
        r'"shortDescription":"((?:[^"\\]|\\.)*)","isCrawlable"',
        html,
        flags=re.DOTALL,
    )
    if not desc_match:
        desc_match = re.search(
            r'"shortDescription":"((?:[^"\\]|\\.)*)","thumbnail"',
            html,
            flags=re.DOTALL,
        )
    if desc_match:
        desc = decode_js_escaped_text(desc_match.group(1))
        if has_meaningful_text(desc, min_chars=20):
            out["description"] = desc

    thumb_match = re.search(r'"thumbnailUrl":"(https:[^"]+)"', html)
    if thumb_match:
        out["thumbnail_url"] = decode_html_text(thumb_match.group(1).replace("\\/", "/"))

    return out


def find_ytdlp_binary() -> str:
    detected = shutil.which("yt-dlp")
    if detected:
        return detected
    local_bin = Path.home() / ".local/bin/yt-dlp"
    if local_bin.exists():
        return str(local_bin)
    return ""


def fetch_video_metadata_with_ytdlp(url: str) -> Tuple[Dict[str, Any], str]:
    ytdlp = find_ytdlp_binary()
    if not ytdlp:
        return {}, "yt-dlp not installed"
    cmd = [
        ytdlp,
        "--dump-single-json",
        "--skip-download",
        "--no-playlist",
        "--no-warnings",
        url,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=False)
    except Exception as exc:
        return {}, f"yt-dlp metadata failed ({exc.__class__.__name__}: {exc})"
    if proc.returncode != 0:
        err = normalize_space(proc.stderr or proc.stdout or "")
        return {}, f"yt-dlp metadata error: {err[:320] or 'unknown'}"
    out = (proc.stdout or "").strip()
    if not out:
        return {}, "yt-dlp metadata returned empty output"
    try:
        data = json.loads(out)
        return data if isinstance(data, dict) else {}, ""
    except Exception as exc:
        return {}, f"yt-dlp metadata parse failed ({exc.__class__.__name__}: {exc})"


def select_caption_track_url(tracks: Any, preferred_lang: str = "") -> str:
    if not isinstance(tracks, dict):
        return ""
    preferred_tokens = [preferred_lang.casefold()] if preferred_lang else []
    preferred_tokens.extend(["zh-hant", "zh-hans", "zh", "yue", "ja", "ko", "en"])
    scored: List[Tuple[int, str]] = []
    for lang, entries in tracks.items():
        if not isinstance(entries, list):
            continue
        lang_low = str(lang).casefold()
        lang_score = 0
        for idx, token in enumerate(preferred_tokens):
            if token and (lang_low == token or lang_low.startswith(token)):
                lang_score = max(lang_score, 100 - idx)
                break
        for ent in entries:
            if not isinstance(ent, dict):
                continue
            u = str(ent.get("url", "")).strip()
            if not u:
                continue
            ext = str(ent.get("ext", "")).lower()
            ext_score = 0
            if ext == "vtt":
                ext_score = 30
            elif ext in {"srv3", "srv2", "ttml", "srt"}:
                ext_score = 20
            scored.append((lang_score + ext_score, u))
    if not scored:
        return ""
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def parse_caption_text(raw: str) -> str:
    if not raw:
        return ""
    text = raw.replace("\r\n", "\n").replace("\r", "\n")
    lines: List[str] = []
    prev = ""
    for line in text.split("\n"):
        cur = line.strip()
        if not cur:
            continue
        if cur.upper().startswith("WEBVTT"):
            continue
        if "-->" in cur:
            continue
        if re.match(r"^\d+$", cur):
            continue
        cur = re.sub(r"<[^>]+>", " ", cur)
        cur = decode_html_text(cur)
        if not cur:
            continue
        if is_noise_or_error_line(cur):
            continue
        if cur == prev:
            continue
        prev = cur
        lines.append(cur)
        if sum(len(x) for x in lines) > TRANSCRIPT_MAX_CHARS:
            break
    return "\n".join(lines).strip()


def fetch_caption_text(caption_url: str) -> Tuple[str, str]:
    if not caption_url:
        return "", "caption URL missing"
    try:
        resp = requests.get(
            caption_url,
            headers={"User-Agent": USER_AGENT, "Accept": "text/vtt,text/plain,*/*"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    except Exception as exc:
        return "", f"caption download failed ({exc.__class__.__name__}: {exc})"
    parsed = parse_caption_text(resp.text or "")
    if not parsed:
        return "", "caption text empty"
    if len(parsed) > TRANSCRIPT_MAX_CHARS:
        parsed = parsed[:TRANSCRIPT_MAX_CHARS].rstrip() + "..."
    return parsed, ""


def pick_audio_downloaded_file(tmp_dir: str) -> str:
    root = Path(tmp_dir)
    best = ""
    best_size = -1
    for p in root.glob("audio.*"):
        if not p.is_file():
            continue
        try:
            size = p.stat().st_size
        except OSError:
            continue
        if size > best_size:
            best_size = size
            best = str(p)
    return best


def download_video_audio_with_ytdlp(url: str) -> Tuple[str, str]:
    ytdlp = find_ytdlp_binary()
    if not ytdlp:
        return "", "yt-dlp not installed"
    tmp_dir = tempfile.mkdtemp(prefix="chief_fafa_audio_")
    out_tmpl = str(Path(tmp_dir) / "audio.%(ext)s")
    cmd = [
        ytdlp,
        "--no-playlist",
        "--no-warnings",
        "-f",
        "bestaudio",
        "-o",
        out_tmpl,
        url,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=240, check=False)
    except Exception as exc:
        return "", f"yt-dlp audio download failed ({exc.__class__.__name__}: {exc})"
    if proc.returncode != 0:
        err = normalize_space(proc.stderr or proc.stdout or "")
        return "", f"yt-dlp audio error: {err[:320] or 'unknown'}"
    audio_path = pick_audio_downloaded_file(tmp_dir)
    if not audio_path:
        return "", "yt-dlp audio file not found after download"
    return audio_path, ""


def transcribe_audio_with_openai(audio_path: str) -> Tuple[str, str]:
    api_key = read_env_value("OPENAI_API_KEY", "")
    if not api_key:
        return "", "OPENAI_API_KEY missing for transcription"
    try:
        size = Path(audio_path).stat().st_size
    except OSError as exc:
        return "", f"audio file unreadable ({exc.__class__.__name__}: {exc})"
    if size <= 0:
        return "", "audio file is empty"
    if size > 24 * 1024 * 1024:
        return "", f"audio too large for transcription ({size} bytes > 25165824 bytes)"

    model = read_env_value("CHIEF_FAFA_TRANSCRIBE_MODEL", TRANSCRIPT_MODEL)
    headers = {"Authorization": f"Bearer {api_key}"}
    data = {
        "model": model,
        "response_format": "text",
    }
    try:
        with open(audio_path, "rb") as audio_file:
            files = {"file": (Path(audio_path).name, audio_file, "application/octet-stream")}
            resp = requests.post(
                OPENAI_TRANSCRIPTIONS_URL,
                headers=headers,
                data=data,
                files=files,
                timeout=240,
            )
    except Exception as exc:
        return "", f"transcription request failed ({exc.__class__.__name__}: {exc})"

    if resp.status_code >= 400:
        err = normalize_space(resp.text or "")
        return "", f"transcription HTTP {resp.status_code}: {err[:320] or 'request failed'}"

    if "application/json" in (resp.headers.get("Content-Type", "").lower()):
        try:
            payload = resp.json()
            text = str(payload.get("text", "")).strip() if isinstance(payload, dict) else ""
        except Exception:
            text = ""
    else:
        text = (resp.text or "").strip()

    text = parse_caption_text(text)
    if not text:
        return "", "transcription returned empty text"
    if len(text) > TRANSCRIPT_MAX_CHARS:
        text = text[:TRANSCRIPT_MAX_CHARS].rstrip() + "..."
    return text, ""

def coerce_image_url(value: Any, base_url: str) -> str:
    if isinstance(value, str):
        return urljoin(base_url, value)
    if isinstance(value, dict):
        candidate = first_non_empty([value.get("url"), value.get("contentUrl"), value.get("@id")])  # type: ignore[arg-type]
        return urljoin(base_url, candidate) if candidate else ""
    if isinstance(value, list):
        for item in value:
            picked = coerce_image_url(item, base_url)
            if picked:
                return picked
    return ""


def instruction_to_text(step: Any) -> str:
    if isinstance(step, str):
        return normalize_space(step)
    if isinstance(step, dict):
        text = step.get("text")
        if isinstance(text, str):
            return normalize_space(text)
        if isinstance(step.get("itemListElement"), list):
            nested = [instruction_to_text(x) for x in step.get("itemListElement", [])]
            return normalize_space(" ".join(x for x in nested if x))
    return ""


def extract_recipe_from_json_ld(recipe_obj: Dict[str, Any], base_url: str) -> Dict[str, Any]:
    if not recipe_obj:
        return {}
    ingredients = [normalize_space(str(x)) for x in recipe_obj.get("recipeIngredient", []) if str(x).strip()]
    instructions_raw = recipe_obj.get("recipeInstructions", [])
    instructions: List[str] = []
    if isinstance(instructions_raw, list):
        instructions = [instruction_to_text(x) for x in instructions_raw if instruction_to_text(x)]
    elif isinstance(instructions_raw, str):
        instructions = [normalize_space(instructions_raw)]

    author = recipe_obj.get("author")
    author_name = ""
    if isinstance(author, dict):
        author_name = normalize_space(str(author.get("name", "")))
    elif isinstance(author, list):
        for item in author:
            if isinstance(item, dict) and item.get("name"):
                author_name = normalize_space(str(item.get("name")))
                break
    elif isinstance(author, str):
        author_name = normalize_space(author)

    return {
        "name": first_non_empty([str(recipe_obj.get("name", "")).strip()]),
        "description": first_non_empty([str(recipe_obj.get("description", "")).strip()]),
        "image_url": coerce_image_url(recipe_obj.get("image"), base_url),
        "author": author_name,
        "prep_time": normalize_space(str(recipe_obj.get("prepTime", "")).strip()),
        "cook_time": normalize_space(str(recipe_obj.get("cookTime", "")).strip()),
        "total_time": normalize_space(str(recipe_obj.get("totalTime", "")).strip()),
        "yield": normalize_space(str(recipe_obj.get("recipeYield", "")).strip()),
        "category": normalize_space(str(recipe_obj.get("recipeCategory", "")).strip()),
        "cuisine": normalize_space(str(recipe_obj.get("recipeCuisine", "")).strip()),
        "ingredients": unique_clean_lines(ingredients, max_items=260),
        "instructions": unique_clean_lines(instructions, max_items=300),
    }


def extract_main_text(html: str) -> str:
    scrubbed = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    scrubbed = re.sub(r"<style[^>]*>.*?</style>", " ", scrubbed, flags=re.IGNORECASE | re.DOTALL)
    paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", scrubbed, flags=re.IGNORECASE | re.DOTALL)
    cleaned = [strip_tags(p) for p in paragraphs]
    cleaned = [c for c in cleaned if len(c) > 40]
    joined = "\n".join(cleaned[:35])
    return joined[:4500]


def fetch_page(url: str) -> Tuple[str, str]:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    resp.raise_for_status()
    return resp.url, resp.text


def read_openai_text(payload: Dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str) and payload["output_text"].strip():
        return payload["output_text"].strip()
    texts: List[str] = []
    for item in payload.get("output", []) if isinstance(payload.get("output"), list) else []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) if isinstance(item.get("content"), list) else []:
            if not isinstance(content, dict):
                continue
            if content.get("type") in {"output_text", "text"} and isinstance(content.get("text"), str):
                texts.append(content["text"])
    return "\n".join(texts).strip()


def parse_json_object_from_text(text: str) -> Dict[str, Any]:
    text = text.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def call_openai_responses_json(system_prompt: str, user_prompt: str) -> Tuple[Dict[str, Any], str]:
    api_key = read_env_value("OPENAI_API_KEY", "")
    if not api_key:
        return {}, "OPENAI_API_KEY missing"

    model = read_env_value("CHIEF_FAFA_MODEL", "gpt-5-nano")
    req_payload = {
        "model": model,
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_prompt}]},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    timeout_sec = int(read_env_value("CHIEF_FAFA_OPENAI_TIMEOUT_SEC", str(OPENAI_REQUEST_TIMEOUT)) or OPENAI_REQUEST_TIMEOUT)
    max_retries = int(read_env_value("CHIEF_FAFA_OPENAI_RETRIES", str(OPENAI_MAX_RETRIES)) or OPENAI_MAX_RETRIES)
    max_retries = max(0, min(max_retries, 5))
    last_error = ""
    data: Dict[str, Any] = {}

    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(OPENAI_RESPONSES_URL, headers=headers, json=req_payload, timeout=timeout_sec)
            if resp.status_code in {429, 500, 502, 503, 504}:
                detail = normalize_space(resp.text or "")
                last_error = f"OpenAI HTTP {resp.status_code}: {detail[:220] or 'transient server error'}"
                if attempt < max_retries:
                    time.sleep(OPENAI_RETRY_BACKOFF_SEC * (attempt + 1))
                    continue
            resp.raise_for_status()
            parsed_data = resp.json()
            data = parsed_data if isinstance(parsed_data, dict) else {}
            last_error = ""
            break
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
            last_error = f"OpenAI call failed ({exc.__class__.__name__}: {exc})"
            if attempt < max_retries:
                time.sleep(OPENAI_RETRY_BACKOFF_SEC * (attempt + 1))
                continue
        except Exception as exc:
            last_error = f"OpenAI call failed ({exc.__class__.__name__}: {exc})"
            break

    if not data:
        return {}, last_error or "OpenAI call failed (unknown error)"
    text = read_openai_text(data)
    parsed = parse_json_object_from_text(text)
    if not parsed:
        return {}, "OpenAI response was not parseable JSON"
    return parsed, ""


def list_from_any(value: Any, max_items: int) -> List[str]:
    items: List[str] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                items.append(item)
            elif isinstance(item, dict):
                text = str(item.get("text", "")).strip()
                if text:
                    items.append(text)
    elif isinstance(value, str):
        items.extend([x for x in value.splitlines() if x.strip()])
    return unique_clean_lines(items, max_items=max_items)


def extract_text_recipe_with_openai(raw_text: str, language_hint: str) -> Tuple[Dict[str, Any], str]:
    clean_text = raw_text.strip()
    if not clean_text:
        return {}, "recipe text is empty"

    system_prompt = (
        "You extract structured recipe data from free-form user text in ANY language. "
        "Do not translate. Keep original language wording. "
        "Return strict JSON object with keys: "
        "title (string), description (string), language (string), ingredients (array of strings), steps (array of strings)."
    )
    user_prompt = (
        "Parse this recipe text and return JSON only.\n"
        f"Language hint: {language_hint or 'unknown'}\n\n"
        "Recipe text:\n"
        + clean_text[:16000]
    )
    parsed, err = call_openai_responses_json(system_prompt, user_prompt)
    if err:
        return {}, err

    ingredients = list_from_any(parsed.get("ingredients", []), max_items=320)
    steps = list_from_any(parsed.get("steps", []), max_items=360)
    if not steps:
        steps = list_from_any(parsed.get("method", []), max_items=360)
    if not steps:
        steps = list_from_any(parsed.get("instructions", []), max_items=360)

    out = {
        "title": cleanup_title_text(str(parsed.get("title", "")).strip()),
        "description": decode_html_text(str(parsed.get("description", "")).strip()),
        "language": decode_html_text(str(parsed.get("language", "")).strip().lower()),
        "ingredients": ingredients,
        "steps": steps,
    }
    return out, ""


def compact_payload_for_openai(payload: Dict[str, Any]) -> Dict[str, Any]:
    def clip(value: Any, limit: int) -> str:
        text = decode_html_text(str(value or ""))
        if len(text) <= limit:
            return text
        return text[: limit - 3].rstrip() + "..."

    ingredients = [clip(x, 200) for x in payload.get("ingredients", []) if str(x).strip()][:40]
    instructions = [clip(x, 320) for x in payload.get("instructions", []) if str(x).strip()][:40]
    compact = {
        "url": clip(payload.get("url", ""), 320),
        "content_type": clip(payload.get("content_type", ""), 40),
        "title": clip(payload.get("title", ""), 220),
        "description": clip(payload.get("description", ""), 1200),
        "video_description": clip(payload.get("video_description", ""), 1400),
        "video_transcript_excerpt": clip(payload.get("video_transcript", ""), 1800),
        "ingredients": ingredients,
        "instructions": instructions,
        "author": clip(payload.get("author", ""), 160),
        "language_hint": dominant_script_group(
            "\n".join(
                [
                    str(payload.get("title", "")),
                    str(payload.get("description", "")),
                    str(payload.get("video_description", "")),
                ]
            )
        ),
    }
    return compact


def should_skip_openai_generation(payload: Dict[str, Any]) -> bool:
    is_video = str(payload.get("content_type", "")).strip().lower() == "video"
    if not is_video:
        return False
    has_steps = len(payload.get("instructions", []) or []) >= 4
    detailed_video_desc = is_detailed_video_description(str(payload.get("video_description", "")))
    return detailed_video_desc and has_steps


def build_formats_with_openai(payload: Dict[str, Any]) -> Tuple[Dict[str, str], str]:
    if is_fast_mode_enabled():
        return {}, "OpenAI skipped (fast mode)"
    if should_skip_openai_generation(payload):
        return {}, "OpenAI skipped (video source already has rich structured content)"
    system_prompt = (
        "You are Chief Fafa Bot. Produce practical social/media content from recipe source data. "
        "Return strict JSON with keys: summary, webpage_copy, facebook_post, instagram_post, youtube_video_script. "
        "Use concise, engaging tone. Keep claims grounded in the source only. "
        "Support multilingual inputs (including English, Chinese, Cantonese, Japanese, Korean and mixed text). "
        "Preserve original ingredient and step wording where possible."
    )
    source_json = compact_payload_for_openai(payload)
    user_prompt = (
        "Create content pack from this source JSON:\n"
        + json.dumps(source_json, ensure_ascii=True)
        + "\nConstraints: include ingredients + key steps when available; mention source URL once in webpage_copy."
    )
    parsed, err = call_openai_responses_json(system_prompt, user_prompt)
    if err:
        return {}, err

    out = {
        "summary": normalize_space(str(parsed.get("summary", ""))),
        "webpage_copy": normalize_space(str(parsed.get("webpage_copy", ""))),
        "facebook_post": normalize_space(str(parsed.get("facebook_post", ""))),
        "instagram_post": normalize_space(str(parsed.get("instagram_post", ""))),
        "youtube_video_script": normalize_space(str(parsed.get("youtube_video_script", ""))),
    }
    if not any(out.values()):
        return {}, "OpenAI JSON missing expected fields"
    return out, ""


def build_formats_fallback(source: Dict[str, Any]) -> Dict[str, str]:
    title = source.get("title", "Recipe")
    description = source.get("description", "")
    ingredients = source.get("ingredients", [])[:10]
    instructions = source.get("instructions", [])[:5]
    url = source.get("url", "")

    ingredient_block = "; ".join(ingredients) if ingredients else "See source ingredients."
    step_block = " ".join(instructions) if instructions else "Follow the source method."

    return {
        "summary": f"{title}: {description}"[:600],
        "webpage_copy": (
            f"{title}\n\n{description}\n\nIngredients (top): {ingredient_block}\n\n"
            f"Method (quick): {step_block}\n\nSource: {url}"
        ),
        "facebook_post": (
            f"Cook idea: {title}\n\n{description}\n\nTop ingredients: {ingredient_block}\n"
            f"Quick method: {step_block}\n\nSource: {url}"
        ),
        "instagram_post": (
            f"{title}\n\n{description}\n\nIngredients: {ingredient_block}\n"
            f"Method: {step_block}\n\n#recipe #homecooking #foodie"
        ),
        "youtube_video_script": (
            f"Today we are making {title}. {description}. "
            f"Ingredients you need: {ingredient_block}. "
            f"Steps: {step_block}. If this helped, like and subscribe."
        ),
    }


def resolve_google_client_secrets() -> Tuple[str, str]:
    client_id = read_env_value("GOOGLE_DOCS_CLIENT_ID", read_env_value("GOOGLE_KEEP_CLIENT_ID", ""))
    client_secret = read_env_value("GOOGLE_DOCS_CLIENT_SECRET", read_env_value("GOOGLE_KEEP_CLIENT_SECRET", ""))
    if client_id and client_secret:
        return client_id, client_secret

    secret_file = read_env_value(
        "GOOGLE_DOCS_CLIENT_SECRET_FILE",
        read_env_value("GOOGLE_KEEP_CLIENT_SECRET_FILE", ""),
    )
    if not secret_file:
        return client_id, client_secret
    try:
        raw = json.loads(Path(secret_file).read_text(encoding="utf-8"))
    except Exception:
        return client_id, client_secret

    block = raw.get("installed") if isinstance(raw.get("installed"), dict) else raw.get("web")
    if isinstance(block, dict):
        client_id = client_id or str(block.get("client_id", "")).strip()
        client_secret = client_secret or str(block.get("client_secret", "")).strip()
    return client_id, client_secret


def resolve_docs_access_token() -> Tuple[str, str]:
    refresh = read_env_value("GOOGLE_DOCS_REFRESH_TOKEN", read_env_value("GOOGLE_KEEP_REFRESH_TOKEN", ""))
    direct = read_env_value("GOOGLE_DOCS_ACCESS_TOKEN", read_env_value("GOOGLE_KEEP_ACCESS_TOKEN", ""))

    if refresh:
        client_id, client_secret = resolve_google_client_secrets()
        if not client_id or not client_secret:
            # Fall back to direct token if available.
            if direct:
                return direct, ""
            return "", "GOOGLE_DOCS_CLIENT_ID / GOOGLE_DOCS_CLIENT_SECRET missing"

        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh,
            "grant_type": "refresh_token",
        }
        try:
            resp = requests.post(GOOGLE_TOKEN_URL, data=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            token_data = resp.json()
            access_token = str(token_data.get("access_token", "")).strip()
            if access_token:
                return access_token, ""
            refresh_error = "token refresh response missing access_token"
        except Exception as exc:
            refresh_error = f"token refresh failed ({exc.__class__.__name__}: {exc})"

        # If refresh failed, try direct token as last resort.
        if direct:
            return direct, ""
        return "", refresh_error

    if direct:
        return direct, ""

    return "", "GOOGLE_DOCS_ACCESS_TOKEN / GOOGLE_DOCS_REFRESH_TOKEN not configured"


def extract_google_api_error(data: Any) -> str:
    if not isinstance(data, dict):
        return ""
    err_obj = data.get("error")
    if isinstance(err_obj, dict):
        return str(err_obj.get("message", "")).strip()
    if isinstance(err_obj, str):
        return err_obj.strip()
    return ""


def normalize_image_mime(mime: str) -> str:
    low = (mime or "").strip().lower()
    aliases = {
        "image/jpg": "image/jpeg",
        "image/pjpeg": "image/jpeg",
        "image/x-png": "image/png",
    }
    return aliases.get(low, low)


def guess_image_mime_and_name(image_url: str, content_type: str) -> Tuple[str, str]:
    parsed_content_type = normalize_image_mime((content_type or "").split(";")[0].strip().lower())
    if parsed_content_type.startswith("image/"):
        mime = parsed_content_type
    else:
        guessed, _ = mimetypes.guess_type(image_url)
        mime = normalize_image_mime(guessed if guessed and guessed.startswith("image/") else "image/jpeg")

    ext = mimetypes.guess_extension(mime) or ".jpg"
    filename = f"chief-fafa-image{ext}"
    return mime, filename


def extract_youtube_video_id_from_image_url(image_url: str) -> str:
    parsed = urlparse(image_url)
    host = parsed.netloc.lower()
    path = parsed.path
    if "ytimg.com" not in host and "youtube.com" not in host:
        return ""
    match = re.search(r"/vi(?:_webp)?/([^/]+)/", path)
    if match:
        return match.group(1).strip()
    return ""


def youtube_thumbnail_jpg_candidates(image_url: str) -> List[str]:
    vid = extract_youtube_video_id_from_image_url(image_url)
    if not vid:
        return []
    base = f"https://i.ytimg.com/vi/{vid}"
    return [
        f"{base}/maxresdefault.jpg",
        f"{base}/sddefault.jpg",
        f"{base}/hqdefault.jpg",
        f"{base}/mqdefault.jpg",
        f"{base}/default.jpg",
    ]


def candidate_image_urls_for_embed(image_url: str) -> List[str]:
    raw = [image_url] + youtube_thumbnail_jpg_candidates(image_url)
    out: List[str] = []
    seen: set[str] = set()
    for u in raw:
        clean = decode_html_text(str(u))
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def convert_image_bytes_to_jpeg(image_bytes: bytes) -> Tuple[bytes, str]:
    try:
        from PIL import Image  # type: ignore
    except Exception:
        return b"", "Pillow not installed for image conversion"
    try:
        with Image.open(io.BytesIO(image_bytes)) as im:
            rgb = im.convert("RGB")
            buf = io.BytesIO()
            rgb.save(buf, format="JPEG", quality=90, optimize=True)
            return buf.getvalue(), ""
    except Exception as exc:
        return b"", f"image conversion failed ({exc.__class__.__name__}: {exc})"


def download_image_for_embed(image_url: str) -> Tuple[bytes, str, str, str]:
    if not image_url:
        return b"", "", "", "missing image URL"
    last_err = "image download failed"
    for candidate in candidate_image_urls_for_embed(image_url):
        try:
            resp = requests.get(
                candidate,
                headers={"User-Agent": USER_AGENT, "Accept": "image/*,*/*;q=0.8"},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
        except Exception as exc:
            last_err = f"image download failed ({exc.__class__.__name__}: {exc})"
            continue

        image_bytes = resp.content or b""
        if not image_bytes:
            last_err = "image download returned empty content"
            continue
        if len(image_bytes) > MAX_EMBED_IMAGE_BYTES:
            last_err = f"image too large ({len(image_bytes)} bytes > {MAX_EMBED_IMAGE_BYTES} bytes)"
            continue

        mime, filename = guess_image_mime_and_name(candidate, resp.headers.get("Content-Type", ""))
        if mime in DOCS_SUPPORTED_IMAGE_MIME:
            return image_bytes, mime, filename, ""

        # Convert unsupported formats (webp/avif/...) to JPEG when possible.
        converted, conv_err = convert_image_bytes_to_jpeg(image_bytes)
        if converted:
            if len(converted) > MAX_EMBED_IMAGE_BYTES:
                last_err = f"converted image too large ({len(converted)} bytes > {MAX_EMBED_IMAGE_BYTES} bytes)"
                continue
            return converted, "image/jpeg", "chief-fafa-image.jpg", ""

        last_err = f"unsupported image format for Docs: {mime} ({conv_err})"
    return b"", "", "", last_err


def upload_image_to_drive_for_embed(
    access_token: str,
    quota_project: str,
    image_bytes: bytes,
    mime_type: str,
    filename: str,
) -> Tuple[str, str]:
    boundary = f"==============={int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)}=="
    metadata = json.dumps({"name": filename, "mimeType": mime_type}, ensure_ascii=True).encode("utf-8")
    body = b"".join(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            b"Content-Type: application/json; charset=UTF-8\r\n\r\n",
            metadata,
            b"\r\n",
            f"--{boundary}\r\n".encode("utf-8"),
            f"Content-Type: {mime_type}\r\n\r\n".encode("utf-8"),
            image_bytes,
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": f"multipart/related; boundary={boundary}",
    }
    if quota_project:
        headers["X-Goog-User-Project"] = quota_project

    try:
        resp = requests.post(GOOGLE_DRIVE_UPLOAD_URL, headers=headers, data=body, timeout=REQUEST_TIMEOUT)
        data = resp.json() if resp.text else {}
    except Exception as exc:
        return "", f"Drive upload failed ({exc.__class__.__name__}: {exc})"

    if resp.status_code >= 400:
        detail = extract_google_api_error(data)
        return "", f"Drive upload HTTP {resp.status_code}: {detail or 'request failed'}"

    file_id = str(data.get("id", "")).strip() if isinstance(data, dict) else ""
    if not file_id:
        return "", "Drive upload response missing file id"

    # Make the uploaded image publicly readable so Docs can fetch it.
    perm_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    if quota_project:
        perm_headers["X-Goog-User-Project"] = quota_project
    perm_payload = {"type": "anyone", "role": "reader", "allowFileDiscovery": False}
    try:
        perm_resp = requests.post(
            f"{GOOGLE_DRIVE_API_BASE}/{file_id}/permissions",
            headers=perm_headers,
            json=perm_payload,
            timeout=REQUEST_TIMEOUT,
        )
        perm_data = perm_resp.json() if perm_resp.text else {}
    except Exception as exc:
        return "", f"Drive permission failed ({exc.__class__.__name__}: {exc})"

    if perm_resp.status_code >= 400:
        detail = extract_google_api_error(perm_data)
        return "", f"Drive permission HTTP {perm_resp.status_code}: {detail or 'request failed'}"

    return f"https://drive.google.com/uc?id={file_id}", ""


def drive_image_uri_candidates(primary_uri: str) -> List[str]:
    if not primary_uri:
        return []
    out: List[str] = []
    seen: set[str] = set()

    def add(uri: str) -> None:
        clean = uri.strip()
        if clean and clean not in seen:
            seen.add(clean)
            out.append(clean)

    add(primary_uri)
    parsed = urlparse(primary_uri)
    file_id = ""
    if parsed.query:
        for part in parsed.query.split("&"):
            if part.startswith("id="):
                file_id = part.split("=", 1)[1].strip()
                break
    if file_id:
        add(f"https://drive.google.com/uc?export=view&id={file_id}")
        add(f"https://drive.google.com/thumbnail?id={file_id}&sz=w1600")
    return out


def insert_image_into_doc(
    document_id: str,
    access_token: str,
    quota_project: str,
    image_uri: str,
    insert_index: Optional[int] = None,
) -> str:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    if quota_project:
        headers["X-Goog-User-Project"] = quota_project

    if insert_index is None:
        end_index = 1
        try:
            get_resp = requests.get(
                f"{DOCS_API_CREATE_URL}/{document_id}",
                headers=headers,
                params={"fields": "body/content/endIndex"},
                timeout=REQUEST_TIMEOUT,
            )
            get_data = get_resp.json() if get_resp.text else {}
            if get_resp.status_code < 400:
                content = (((get_data or {}).get("body") or {}).get("content") or []) if isinstance(get_data, dict) else []
                if isinstance(content, list) and content:
                    last = content[-1]
                    if isinstance(last, dict) and isinstance(last.get("endIndex"), int):
                        end_index = int(last["endIndex"])
        except Exception:
            pass
        insert_index = max(1, end_index - 1)
    else:
        insert_index = max(1, int(insert_index))
    batch_payload = {
        "requests": [
            {
                "insertInlineImage": {
                    "location": {"index": insert_index},
                    "uri": image_uri,
                }
            }
        ]
    }

    try:
        batch_resp = requests.post(
            f"{DOCS_API_CREATE_URL}/{document_id}:batchUpdate",
            headers=headers,
            json=batch_payload,
            timeout=REQUEST_TIMEOUT,
        )
        batch_data = batch_resp.json() if batch_resp.text else {}
    except Exception as exc:
        return f"Docs image insert failed ({exc.__class__.__name__}: {exc})"

    if batch_resp.status_code >= 400:
        detail = extract_google_api_error(batch_data)
        return f"Docs image insert HTTP {batch_resp.status_code}: {detail or 'request failed'}"
    return ""


def find_text_range_in_doc(
    document_id: str,
    access_token: str,
    quota_project: str,
    marker: str,
) -> Tuple[int, int]:
    headers = {"Authorization": f"Bearer {access_token}"}
    if quota_project:
        headers["X-Goog-User-Project"] = quota_project
    params = {
        "fields": "body/content(paragraph/elements(startIndex,endIndex,textRun/content))",
    }
    try:
        resp = requests.get(
            f"{DOCS_API_CREATE_URL}/{document_id}",
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        data = resp.json() if resp.text else {}
    except Exception:
        return -1, -1
    if resp.status_code >= 400 or not isinstance(data, dict):
        return -1, -1

    content = (((data.get("body") or {}).get("content")) or [])
    if not isinstance(content, list):
        return -1, -1
    for block in content:
        if not isinstance(block, dict):
            continue
        para = block.get("paragraph")
        if not isinstance(para, dict):
            continue
        elements = para.get("elements")
        if not isinstance(elements, list):
            continue
        for el in elements:
            if not isinstance(el, dict):
                continue
            text_run = el.get("textRun")
            if not isinstance(text_run, dict):
                continue
            txt = str(text_run.get("content", ""))
            if not txt or marker not in txt:
                continue
            start_index = el.get("startIndex")
            if not isinstance(start_index, int):
                continue
            offset = txt.find(marker)
            m_start = start_index + offset
            m_end = m_start + len(marker)
            return m_start, m_end
    return -1, -1


def delete_text_range_in_doc(
    document_id: str,
    access_token: str,
    quota_project: str,
    start_index: int,
    end_index: int,
) -> str:
    if start_index < 1 or end_index <= start_index:
        return "invalid delete range"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    if quota_project:
        headers["X-Goog-User-Project"] = quota_project
    payload = {
        "requests": [
            {
                "deleteContentRange": {
                    "range": {
                        "startIndex": int(start_index),
                        "endIndex": int(end_index),
                    }
                }
            }
        ]
    }
    try:
        resp = requests.post(
            f"{DOCS_API_CREATE_URL}/{document_id}:batchUpdate",
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        data = resp.json() if resp.text else {}
    except Exception as exc:
        return f"Docs delete marker failed ({exc.__class__.__name__}: {exc})"
    if resp.status_code >= 400:
        detail = extract_google_api_error(data)
        return f"Docs delete marker HTTP {resp.status_code}: {detail or 'request failed'}"
    return ""


def insert_text_into_doc(
    document_id: str,
    access_token: str,
    quota_project: str,
    index: int,
    text: str,
) -> str:
    if not text:
        return ""
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    if quota_project:
        headers["X-Goog-User-Project"] = quota_project
    payload = {
        "requests": [
            {
                "insertText": {
                    "location": {"index": max(1, int(index))},
                    "text": text,
                }
            }
        ]
    }
    try:
        resp = requests.post(
            f"{DOCS_API_CREATE_URL}/{document_id}:batchUpdate",
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        data = resp.json() if resp.text else {}
    except Exception as exc:
        return f"Docs insert text failed ({exc.__class__.__name__}: {exc})"
    if resp.status_code >= 400:
        detail = extract_google_api_error(data)
        return f"Docs insert text HTTP {resp.status_code}: {detail or 'request failed'}"
    return ""


def create_google_doc_note(title: str, body: str, image_url: str = "") -> Dict[str, Any]:
    fast_mode = is_fast_mode_enabled()
    token, err = resolve_docs_access_token()
    if not token:
        return {"ok": False, "message": err or "missing Google Docs token"}

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    quota_project = read_env_value(
        "GOOGLE_DOCS_QUOTA_PROJECT",
        read_env_value("GOOGLE_KEEP_QUOTA_PROJECT", ""),
    )
    if quota_project:
        headers["X-Goog-User-Project"] = quota_project

    create_payload = {"title": title[:200]}

    try:
        resp = requests.post(DOCS_API_CREATE_URL, headers=headers, json=create_payload, timeout=REQUEST_TIMEOUT)
        data = resp.json() if resp.text else {}
    except Exception as exc:
        return {"ok": False, "message": f"Docs create failed ({exc.__class__.__name__}: {exc})"}

    if resp.status_code >= 400:
        detail = extract_google_api_error(data)
        return {"ok": False, "message": f"Docs API HTTP {resp.status_code}: {detail or 'request failed'}"}

    document_id = str(data.get("documentId", "")).strip() if isinstance(data, dict) else ""
    if not document_id:
        return {"ok": False, "message": "Docs API create response missing documentId"}

    doc_url = f"https://docs.google.com/document/d/{document_id}/edit"
    text_to_insert = body[:50000]
    if text_to_insert:
        batch_payload = {
            "requests": [
                {
                    "insertText": {
                        "location": {"index": 1},
                        "text": text_to_insert,
                    }
                }
            ]
        }
        try:
            batch_resp = requests.post(
                f"{DOCS_API_CREATE_URL}/{document_id}:batchUpdate",
                headers=headers,
                json=batch_payload,
                timeout=REQUEST_TIMEOUT,
            )
            batch_data = batch_resp.json() if batch_resp.text else {}
        except Exception as exc:
            return {
                "ok": False,
                "message": f"Docs batchUpdate failed ({exc.__class__.__name__}: {exc})",
                "document_id": document_id,
                "url": doc_url,
            }
        if batch_resp.status_code >= 400:
            detail = extract_google_api_error(batch_data)
            return {
                "ok": False,
                "message": f"Docs batchUpdate HTTP {batch_resp.status_code}: {detail or 'request failed'}",
                "document_id": document_id,
                "url": doc_url,
            }

    image_embed_note = ""
    marker_start, marker_end = find_text_range_in_doc(
        document_id=document_id,
        access_token=token,
        quota_project=quota_project,
        marker=DOC_IMAGE_MARKER,
    )

    if image_url and fast_mode:
        # Fast mode: avoid Drive upload roundtrips and try direct-source embed first.
        target_index: Optional[int]
        if marker_start > 0 and marker_end > marker_start:
            delete_err = delete_text_range_in_doc(
                document_id=document_id,
                access_token=token,
                quota_project=quota_project,
                start_index=marker_start,
                end_index=marker_end,
            )
            if delete_err:
                return {
                    "ok": False,
                    "message": f"created, but image marker cleanup failed: {delete_err}",
                    "document_id": document_id,
                    "url": doc_url,
                }
            target_index = marker_start
        else:
            target_index = None

        insert_err = ""
        for uri_candidate in candidate_image_urls_for_embed(image_url):
            insert_err = insert_image_into_doc(
                document_id=document_id,
                access_token=token,
                quota_project=quota_project,
                image_uri=uri_candidate,
                insert_index=target_index,
            )
            if not insert_err:
                break

        if insert_err:
            if marker_start > 0 and marker_end > marker_start:
                insert_text_into_doc(
                    document_id=document_id,
                    access_token=token,
                    quota_project=quota_project,
                    index=marker_start,
                    text="(image unavailable)\n",
                )
            image_embed_note = f" (fast mode: image unavailable - {insert_err})"
        else:
            image_embed_note = " (image embedded, fast mode)"
    elif image_url:
        image_bytes, mime_type, filename, download_err = download_image_for_embed(image_url)
        if download_err:
            if marker_start > 0 and marker_end > marker_start:
                delete_text_range_in_doc(
                    document_id=document_id,
                    access_token=token,
                    quota_project=quota_project,
                    start_index=marker_start,
                    end_index=marker_end,
                )
                insert_text_into_doc(
                    document_id=document_id,
                    access_token=token,
                    quota_project=quota_project,
                    index=marker_start,
                    text="(image unavailable)\n",
                )
            return {
                "ok": False,
                "message": f"created, but image embed failed: {download_err}",
                "document_id": document_id,
                "url": doc_url,
            }

        drive_uri, drive_err = upload_image_to_drive_for_embed(
            access_token=token,
            quota_project=quota_project,
            image_bytes=image_bytes,
            mime_type=mime_type,
            filename=filename,
        )
        # Fallback: if Drive upload is unavailable (scope/quota), try embedding from source URI.
        image_uri = drive_uri if not drive_err else image_url
        if marker_start > 0 and marker_end > marker_start:
            delete_err = delete_text_range_in_doc(
                document_id=document_id,
                access_token=token,
                quota_project=quota_project,
                start_index=marker_start,
                end_index=marker_end,
            )
            if delete_err:
                return {
                    "ok": False,
                    "message": f"created, but image marker cleanup failed: {delete_err}",
                    "document_id": document_id,
                    "url": doc_url,
                }
            target_index = marker_start
        else:
            target_index = None
        uri_candidates = [image_uri]
        if drive_uri and not drive_err:
            uri_candidates = drive_image_uri_candidates(drive_uri)
        insert_err = ""
        for idx, uri_candidate in enumerate(uri_candidates):
            insert_err = insert_image_into_doc(
                document_id=document_id,
                access_token=token,
                quota_project=quota_project,
                image_uri=uri_candidate,
                insert_index=target_index,
            )
            if not insert_err:
                break
            is_retrieval_issue = "problem retrieving the image" in insert_err.casefold()
            if is_retrieval_issue and idx < (len(uri_candidates) - 1):
                time.sleep(1.2)
                continue
            if is_retrieval_issue and idx == (len(uri_candidates) - 1):
                # One final retry after a short delay for Drive permission/index propagation.
                time.sleep(1.6)
                insert_err = insert_image_into_doc(
                    document_id=document_id,
                    access_token=token,
                    quota_project=quota_project,
                    image_uri=uri_candidate,
                    insert_index=target_index,
                )
                if not insert_err:
                    break
        if insert_err:
            base = f"{insert_err}"
            if drive_err:
                base = f"{base}; Drive fallback reason: {drive_err}"
            return {
                "ok": False,
                "message": f"created, but image embed failed: {base}",
                "document_id": document_id,
                "url": doc_url,
            }
        if drive_err:
            image_embed_note = f" (image embedded via source URL; Drive fallback: {drive_err})"
        else:
            image_embed_note = " (image embedded)"
    else:
        if marker_start > 0 and marker_end > marker_start:
            delete_text_range_in_doc(
                document_id=document_id,
                access_token=token,
                quota_project=quota_project,
                start_index=marker_start,
                end_index=marker_end,
            )
            insert_text_into_doc(
                document_id=document_id,
                access_token=token,
                quota_project=quota_project,
                index=marker_start,
                text="(image unavailable)\n",
            )

    return {"ok": True, "document_id": document_id, "url": doc_url, "message": f"created{image_embed_note}"}


def slugify(value: str) -> str:
    raw = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower())
    raw = raw.strip("-")
    return raw[:80] or "recipe"


def build_google_doc_recipe_text(source: Dict[str, Any], summary_text: str) -> str:
    title = decode_html_text(str(source.get("title", ""))) or "Untitled recipe"
    source_url = str(source.get("url", "")).strip()
    is_video = str(source.get("content_type", "")).strip().lower() == "video"
    is_text_recipe = str(source.get("content_type", "")).strip().lower() == "text_recipe_input"
    input_language = decode_html_text(str(source.get("input_language", ""))).strip()
    ingredients = sanitize_recipe_lines_for_doc([str(x) for x in source.get("ingredients", [])], max_items=320)
    steps = sanitize_recipe_lines_for_doc([str(x) for x in source.get("instructions", [])], max_items=360)
    video_description = sanitize_doc_text_block(str(source.get("video_description", "")), max_chars=2400)
    video_transcript = sanitize_doc_text_block(str(source.get("video_transcript", "")), max_chars=7000)

    if not summary_text:
        summary_text = decode_html_text(str(source.get("description", "")))
    summary_text = decode_html_text(summary_text)
    if is_video and is_detailed_video_description(video_description):
        summary_text = f"Video recipe captured: {title}."
    summary_text = normalize_space(summary_text.replace("\n", " "))
    summary_text = strip_diagnostic_suffix(summary_text)
    if len(summary_text) > 1400:
        summary_text = summary_text[:1397].rstrip() + "..."

    lines: List[str] = []
    lines.append("Chief Fafa Recipe Note")
    lines.append("")
    lines.append(f"Recipe Title: {title}")
    lines.append("")
    lines.append("Original Page URL:")
    if source_url:
        lines.append(source_url)
    elif is_text_recipe:
        lines.append("(not provided; recipe submitted as text)")
    else:
        lines.append("(missing)")
    lines.append("")
    if is_text_recipe:
        lines.append("Source Type:")
        lines.append("Direct text input")
        lines.append("")
        if input_language:
            lines.append("Detected Language:")
            lines.append(input_language)
            lines.append("")
    if is_video:
        lines.append("Source Type:")
        lines.append("Video link")
        lines.append("")
    lines.append("Recipe Summary:")
    lines.append(summary_text or "(summary unavailable)")
    lines.append("")
    lines.append("Food Image:")
    lines.append(DOC_IMAGE_MARKER)
    lines.append("")
    if is_video and video_description:
        lines.append("Video Description (from original source):")
        lines.append(video_description)
        lines.append("")
    if is_video and (not ingredients or not steps) and video_transcript:
        lines.append("Transcript Excerpt (auto):")
        lines.append(video_transcript)
        lines.append("")
    lines.append("Ingredients (from original source):")
    if ingredients:
        for item in ingredients:
            lines.append(f"- {item}")
    else:
        lines.append("- Not clearly detected from source.")
    lines.append("")
    lines.append("Method / Steps (from original source):")
    if steps:
        for idx, step in enumerate(steps, start=1):
            lines.append(f"{idx}. {step}")
    else:
        lines.append("1. Not clearly detected from source.")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def format_markdown_report(source: Dict[str, Any], summary_text: str, note_result: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"Chief Fafa Recipe Run - {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"Content type: {source.get('content_type', '')}")
    if source.get("input_language"):
        lines.append(f"Detected language: {source.get('input_language')}")
    lines.append(f"Title: {source.get('title', '')}")
    lines.append(f"Source URL: {source.get('url', '')}")
    lines.append(f"Ingredients detected: {len(source.get('ingredients', []) or [])}")
    lines.append(f"Steps detected: {len(source.get('instructions', []) or [])}")
    lines.append("")
    lines.append("Summary:")
    lines.append(summary_text or "")
    lines.append("")
    if note_result.get("ok"):
        lines.append(f"Google Doc: OK ({note_result.get('url') or note_result.get('document_id') or 'created'})")
    else:
        lines.append(f"Google Doc: FAILED ({note_result.get('message', 'unknown error')})")
    return "\n".join(lines).strip() + "\n"


def format_enquiry_markdown_report(lookup: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"Chief Fafa Recipe Enquiry - {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"Query: {lookup.get('query', '')}")
    lines.append(f"Summary: {lookup.get('summary', '')}")
    lines.append(f"Google Doc status: {lookup.get('google_doc_status', 'not_found')}")
    lines.append(f"Google Doc URL: {lookup.get('google_doc_url', '')}")
    if lookup.get("error_message"):
        lines.append(f"Error: {lookup.get('error_message')}")
    lines.append("")
    lines.append("Matches:")
    results = lookup.get("results", [])
    if isinstance(results, list) and results:
        for item in results[:8]:
            title = decode_html_text(str(item.get("title", ""))).strip() or "Untitled"
            source = str(item.get("source", "")).strip() or "unknown"
            doc_url = str(item.get("doc_url", "")).strip()
            source_url = str(item.get("source_url", "")).strip()
            snippet = decode_html_text(str(item.get("snippet", ""))).strip()
            lines.append(f"- [{source}] {title}")
            if doc_url:
                lines.append(f"  Doc: {doc_url}")
            if source_url:
                lines.append(f"  Source: {source_url}")
            if snippet:
                lines.append(f"  Note: {snippet}")
    else:
        lines.append("- No matches")
    return "\n".join(lines).strip() + "\n"


def format_duplicate_markdown_report(hit: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"Chief Fafa Recipe Duplicate Check - {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")
    lines.append(f"Match type: {hit.get('match_type', '')}")
    lines.append(f"Title: {hit.get('title', '')}")
    lines.append(f"Source URL: {hit.get('source_url', '')}")
    lines.append(f"Google Doc URL: {hit.get('doc_url', '')}")
    if hit.get("note_path"):
        lines.append(f"Note path: {hit.get('note_path', '')}")
    if hit.get("summary"):
        lines.append("")
        lines.append("Summary:")
        lines.append(str(hit.get("summary", "")))
    return "\n".join(lines).strip() + "\n"


def extract_source_payload(url: str) -> Dict[str, Any]:
    final_url, html = fetch_page(url)
    domain = urlparse(final_url).netloc.lower()
    is_youtube_source = ("youtube.com" in domain) or ("youtu.be" in domain)
    title = extract_title(html)
    description = extract_description(html)
    image_url = extract_image_url(html, final_url)

    json_ld_blocks = parse_json_ld_blocks(html)
    recipe_obj = pick_recipe_obj(json_ld_blocks)
    recipe_fields = extract_recipe_from_json_ld(recipe_obj, final_url)
    video_obj = pick_video_obj(json_ld_blocks)
    video_fields = extract_video_from_json_ld(video_obj, final_url)
    is_video_source = is_video_source_page(final_url, html, json_ld_blocks)
    text_excerpt = extract_main_text(html)
    text_lines = html_to_text_lines(html)
    section_ingredients, section_steps = extract_sections_from_lines(text_lines)
    regex_ingredients, regex_steps = extract_sections_by_regex("\n".join(text_lines))

    all_ingredients = unique_clean_lines(
        list(recipe_fields.get("ingredients", [])) + section_ingredients + regex_ingredients,
        max_items=260,
    )
    all_steps = unique_clean_lines(
        list(recipe_fields.get("instructions", [])) + section_steps + regex_steps,
        max_items=300,
    )

    video_description = first_non_empty([video_fields.get("description"), description])
    video_transcript = ""
    video_note = ""
    if is_video_source:
        fast_mode = is_fast_mode_enabled()
        if fast_mode and is_youtube_source:
            youtube_fast = extract_youtube_fast_fields_from_html(html)
            video_description = first_non_empty([youtube_fast.get("description", ""), video_description])
            title = first_non_empty([youtube_fast.get("title", ""), video_fields.get("name"), title, urlparse(final_url).netloc])
            image_url = first_non_empty(
                [
                    youtube_fast.get("thumbnail_url", ""),
                    video_fields.get("thumbnail_url"),
                    recipe_fields.get("image_url"),
                    image_url,
                ]
            )

        need_enrichment = not (fast_mode and is_youtube_source and has_meaningful_text(video_description, min_chars=40))
        if need_enrichment:
            enriched = extract_video_text_sources(final_url, video_description)
            video_description = first_non_empty([enriched.get("description", ""), video_description])
            video_transcript = normalize_multiline_text(str(enriched.get("transcript", "")))
            video_note = decode_html_text(str(enriched.get("error", "")))
            title = first_non_empty([enriched.get("title", ""), video_fields.get("name"), title, urlparse(final_url).netloc])
            image_url = first_non_empty(
                [
                    enriched.get("thumbnail_url", ""),
                    video_fields.get("thumbnail_url"),
                    recipe_fields.get("image_url"),
                    image_url,
                ]
            )
        # Prefer extraction from description; only use transcript as fallback when missing.
        desc_ingredients, desc_steps = extract_recipe_sections_from_text_blob(video_description)
        all_ingredients = unique_clean_lines(list(all_ingredients) + desc_ingredients, max_items=260)
        all_steps = unique_clean_lines(list(all_steps) + desc_steps, max_items=320)
        if (not all_ingredients or not all_steps) and video_transcript:
            transcript_ingredients, transcript_steps = extract_recipe_sections_from_text_blob(video_transcript)
            if not all_ingredients:
                all_ingredients = unique_clean_lines(list(all_ingredients) + transcript_ingredients, max_items=260)
            if not all_steps:
                all_steps = unique_clean_lines(list(all_steps) + transcript_steps, max_items=320)
        video_blob = "\n\n".join([video_description, video_transcript]).strip()
        if not text_excerpt and video_blob:
            text_excerpt = video_blob[:2400]
    else:
        image_url = first_non_empty([recipe_fields.get("image_url"), image_url])

    if is_video_source:
        description = first_non_empty([video_description, recipe_fields.get("description"), description, text_excerpt[:280]])
    else:
        description = first_non_empty([recipe_fields.get("description"), description, text_excerpt[:280]])
    title = first_non_empty([recipe_fields.get("name"), title, urlparse(final_url).netloc])

    title = decode_html_text(title)
    description = decode_html_text(description)
    image_url = decode_html_text(image_url)
    video_description = normalize_multiline_text(video_description) if is_video_source else ""
    video_transcript = normalize_multiline_text(video_transcript) if is_video_source else ""

    context_text = "\n".join([video_description, video_transcript[:800], description, text_excerpt[:500]])
    title = refine_title_for_content_language(title, context_text)
    if title_looks_generic(title):
        fallback_title = derive_title_from_text("\n".join([video_description, video_transcript, description, text_excerpt]))
        if fallback_title:
            title = refine_title_for_content_language(fallback_title, context_text) or fallback_title

    if "instagram.com" in domain and " on Instagram" in title:
        title = title.split(" on Instagram", 1)[0].strip()
    if len(title) > 180:
        title = title[:177].rstrip() + "..."
    if len(description) > 800:
        description = description[:797].rstrip() + "..."

    payload: Dict[str, Any] = {
        "url": final_url,
        "content_type": "video" if is_video_source else "recipe_page",
        "title": title,
        "description": description,
        "image_url": image_url,
        "author": recipe_fields.get("author", ""),
        "prep_time": recipe_fields.get("prep_time", ""),
        "cook_time": recipe_fields.get("cook_time", ""),
        "total_time": recipe_fields.get("total_time", ""),
        "yield": recipe_fields.get("yield", ""),
        "category": recipe_fields.get("category", ""),
        "cuisine": recipe_fields.get("cuisine", ""),
        "ingredients": all_ingredients,
        "instructions": all_steps,
        "text_excerpt": text_excerpt[:2400],
        "video_description": video_description if is_video_source else "",
        "video_transcript": video_transcript if is_video_source else "",
        "video_note": video_note if is_video_source else "",
    }
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Chief Fafa recipe content pack from URL or text input.")
    parser.add_argument("source", nargs="?", default="", help="Recipe URL or full recipe text")
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read source input from stdin (recommended for multiline Telegram messages)",
    )
    parser.add_argument("--no-doc", action="store_true", help="Skip Google Docs note creation")
    parser.add_argument("--no-keep", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--json", action="store_true", help="Print JSON output instead of markdown")
    parser.add_argument(
        "--json-brief",
        action="store_true",
        help="When used with --json, output a compact payload for chat delivery",
    )
    parser.add_argument(
        "--output-dir",
        default=read_env_value("CHIEF_FAFA_OUTPUT_DIR", "/home/felixlee/Desktop/chief-fafa/notes"),
        help="Directory to persist generated markdown reports",
    )
    args = parser.parse_args()

    raw_source = str(args.source or "").strip()
    if args.stdin:
        stdin_payload = sys.stdin.read()
        if stdin_payload and stdin_payload.strip():
            raw_source = stdin_payload.strip()

    output_dir = Path(args.output_dir)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        output_dir = Path(tempfile.gettempdir()) / "chief-fafa-notes"
        output_dir.mkdir(parents=True, exist_ok=True)

    initial_input_url = extract_first_url(raw_source)
    auto_review_triggers: List[str] = []
    new_host_detected = False
    new_host_name = ""
    if initial_input_url:
        is_new_host, host = detect_and_record_new_url_host(initial_input_url)
        new_host_detected = bool(is_new_host)
        new_host_name = str(host or "").strip().lower()
        if is_new_host:
            trigger_state = maybe_trigger_auto_review(
                reason="new_url_host",
                source_url=initial_input_url,
                error_message="",
                input_snippet=raw_source,
            )
            auto_review_triggers.append(f"new_url_host:{host}:{trigger_state}")

    if raw_source and looks_like_recipe_enquiry(raw_source):
        lookup = run_recipe_enquiry(raw_source, output_dir=output_dir)
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
        slug = slugify(f"enquiry-{lookup.get('query', '')}")
        report_path = output_dir / f"{stamp}-{slug}.md"
        report_markdown = format_enquiry_markdown_report(lookup)
        report_path.write_text(report_markdown, encoding="utf-8")

        lookup_result = {
            "ok": bool(lookup.get("ok", True)),
            "mode": "enquiry",
            "query": lookup.get("query", ""),
            "report_path": str(report_path),
            "lookup": lookup,
        }
        if args.json:
            if args.json_brief:
                summary = str(lookup.get("summary", "")).strip()
                google_doc_status = str(lookup.get("google_doc_status", "not_found")).strip()
                google_doc_url = str(lookup.get("google_doc_url", "")).strip()
                error_message = str(lookup.get("error_message", "")).strip()
                brief = {
                    "ok": bool(lookup_result.get("ok")),
                    "summary": summary,
                    "google_doc_status": google_doc_status,
                    "google_doc_url": google_doc_url,
                    "error_message": error_message,
                    "reply_message": build_reply_message(
                        summary=summary,
                        google_doc_status=google_doc_status,
                        google_doc_url=google_doc_url,
                        error_message=error_message,
                    ),
                    "lookup": lookup,
                    "auto_review_triggers": auto_review_triggers,
                    "report_path": str(report_path),
                }
                print(json.dumps(brief, ensure_ascii=True, indent=2))
            else:
                print(json.dumps(lookup_result, ensure_ascii=True, indent=2))
        else:
            print(report_markdown)
            print(f"Report saved: {report_path}")
        return

    if initial_input_url and not args.no_doc and not args.no_keep:
        initial_dup = find_existing_recipe_doc_for_url(initial_input_url, notes_root=output_dir)
        if bool(initial_dup.get("found")) and str(initial_dup.get("doc_url", "")).strip():
            stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
            slug = slugify(f"duplicate-{initial_dup.get('title', 'recipe')}")
            report_path = output_dir / f"{stamp}-{slug}.md"
            report_markdown = format_duplicate_markdown_report(initial_dup)
            report_path.write_text(report_markdown, encoding="utf-8")

            duplicate_summary = (
                f"Recipe already exists for this URL. Reusing existing Google Doc: "
                f"{decode_html_text(str(initial_dup.get('title', 'Recipe')))}."
            )
            brief = {
                "ok": True,
                "summary": duplicate_summary[:220],
                "google_doc_status": "exists",
                "google_doc_url": str(initial_dup.get("doc_url", "")).strip(),
                "error_message": "",
                "reply_message": build_reply_message(
                    summary=duplicate_summary[:220],
                    google_doc_status="exists",
                    google_doc_url=str(initial_dup.get("doc_url", "")).strip(),
                    error_message="",
                ),
                "doc": {
                    "ok": True,
                    "url": str(initial_dup.get("doc_url", "")).strip(),
                    "message": "already exists; skipped duplicate creation",
                },
                "duplicate": initial_dup,
                "auto_review_triggers": auto_review_triggers,
                "report_path": str(report_path),
            }
            if args.json:
                print(json.dumps(brief, ensure_ascii=True, indent=2))
            else:
                print(report_markdown)
                print(f"Report saved: {report_path}")
            return

    if not raw_source:
        source = {
            "url": "",
            "content_type": "text_recipe_input",
            "title": "Input missing",
            "description": "",
            "image_url": "",
            "ingredients": [],
            "instructions": [],
            "text_excerpt": "",
            "input_language": "unknown",
            "input_validation_missing": ["recipe_name", "ingredients", "method_steps"],
        }
        source_error = "No URL or recipe text provided"
    else:
        source_error = ""
        try:
            input_url = extract_first_url(raw_source)
            if input_url:
                source = extract_source_payload(input_url)
            else:
                source = extract_source_payload_from_text(raw_source)
        except Exception as exc:
            source = {
                "url": extract_first_url(raw_source),
                "content_type": "text_recipe_input",
                "title": "Source fetch unavailable",
                "description": "",
                "image_url": "",
                "ingredients": [],
                "instructions": [],
                "text_excerpt": raw_source[:2400],
                "input_language": infer_text_language_label(raw_source),
                "input_validation_missing": ["recipe_name", "ingredients", "method_steps"],
            }
            source_error = f"{exc.__class__.__name__}: {exc}"
            source["source_error"] = source_error

    validation_missing = source.get("input_validation_missing", [])
    if not isinstance(validation_missing, list):
        validation_missing = []
    validation_error = ""
    if str(source.get("content_type", "")).strip().lower() == "text_recipe_input" and validation_missing:
        validation_error = f"text recipe missing required fields: {summarize_text_recipe_validation([str(x) for x in validation_missing])}"

    ai_err = ""
    if validation_error:
        formats = build_formats_fallback(source)
        formats["summary"] = (
            f"Recipe text validation failed. Missing: {summarize_text_recipe_validation([str(x) for x in validation_missing])}."
        )
    else:
        formats, ai_err = build_formats_with_openai(source)
        if not formats:
            formats = build_formats_fallback(source)
    if source_error:
        summary_prefix = "Source crawl failed; generated fallback content only."
        formats["summary"] = f"{summary_prefix} {formats.get('summary', '')}".strip()
    video_note = decode_html_text(str(source.get("video_note", ""))).strip()
    text_parse_note = decode_html_text(str(source.get("input_parse_note", ""))).strip()

    summary_for_doc = human_readable_doc_summary(source, formats)

    duplicate_hit_post_fetch: Dict[str, Any] = {}
    duplicate_check_error = ""
    if str(source.get("url", "")).strip() and not validation_error and not args.no_doc and not args.no_keep:
        post_dup = find_existing_recipe_doc_for_url(str(source.get("url", "")), notes_root=output_dir)
        if bool(post_dup.get("found")) and str(post_dup.get("doc_url", "")).strip():
            duplicate_hit_post_fetch = post_dup
        duplicate_check_error = str(post_dup.get("check_error", "")).strip()

    note_result: Dict[str, Any] = {"ok": False, "message": "skipped"}
    if validation_error:
        note_result = {"ok": False, "message": validation_error}
    elif duplicate_hit_post_fetch:
        note_result = {
            "ok": True,
            "url": str(duplicate_hit_post_fetch.get("doc_url", "")).strip(),
            "message": "already exists; skipped duplicate creation",
        }
    elif not args.no_doc and not args.no_keep:
        note_title = f"Chief Fafa - {source.get('title', 'Recipe')}"[:120]
        note_body = build_google_doc_recipe_text(source, summary_for_doc)
        note_result = create_google_doc_note(note_title, note_body, str(source.get("image_url", "")))

    report_markdown = format_markdown_report(source, summary_for_doc, note_result)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    slug = slugify(str(source.get("title", "recipe")))
    report_path = output_dir / f"{stamp}-{slug}.md"
    report_path.write_text(report_markdown, encoding="utf-8")

    result = {
        "ok": True,
        "url": source.get("url", ""),
        "title": source.get("title", ""),
        "image_url": source.get("image_url", ""),
        "doc": note_result,
        "note": note_result,
        "auto_review_triggers": auto_review_triggers,
        "report_path": str(report_path),
        "formats": formats,
    }

    if args.json:
        if args.json_brief:
            if duplicate_hit_post_fetch:
                summary = (
                    f"Recipe already exists for this URL. Reusing existing Google Doc: "
                    f"{decode_html_text(str(duplicate_hit_post_fetch.get('title', source.get('title', 'Recipe'))))}."
                )
            else:
                summary = short_chat_summary(source, formats, source_error)
            doc_status = "ok" if bool(note_result.get("ok")) else "failed"
            if duplicate_hit_post_fetch:
                doc_status = "exists"
            doc_url = str(note_result.get("url", "")).strip()
            error_messages: List[str] = []
            if source_error:
                error_messages.append(source_error)
            if video_note:
                error_messages.append(video_note)
            if text_parse_note and validation_error:
                error_messages.append(text_parse_note)
            if validation_error:
                error_messages.append(validation_error)
            if doc_status != "ok":
                error_messages.append(str(note_result.get("message", "unknown error")))
            include_ai_err = bool(ai_err) and not ai_err.startswith("OpenAI skipped")
            if include_ai_err and not error_messages:
                error_messages.append(ai_err)
            if duplicate_check_error and not duplicate_hit_post_fetch:
                error_messages.append(duplicate_check_error)
            deduped_errors: List[str] = []
            seen_errors: set[str] = set()
            for item in error_messages:
                msg = normalize_space(str(item))
                if not msg:
                    continue
                if msg in seen_errors:
                    continue
                seen_errors.add(msg)
                deduped_errors.append(msg)
            error_message = " | ".join(deduped_errors).strip()

            source_url_for_review = str(source.get("url", "")).strip() or initial_input_url
            if source_url_for_review and source_error:
                trigger_state = maybe_trigger_auto_review(
                    reason="pipeline_error",
                    source_url=source_url_for_review,
                    error_message=source_error,
                    input_snippet=raw_source,
                )
                auto_review_triggers.append(f"pipeline_error:{trigger_state}")
            elif source_url_for_review and error_message:
                low = error_message.casefold()
                if "not configured" not in low:
                    likely_processing_issue = any(
                        token in low
                        for token in [
                            "failed",
                            "timeout",
                            "http ",
                            "exception",
                            "traceback",
                            "unsupported",
                            "unavailable",
                            "not a valid",
                            "missing",
                            "error",
                        ]
                    )
                    if doc_status == "failed" or likely_processing_issue:
                        trigger_state = maybe_trigger_auto_review(
                            reason="pipeline_error",
                            source_url=source_url_for_review,
                            error_message=error_message,
                            input_snippet=raw_source,
                        )
                        auto_review_triggers.append(f"pipeline_error:{trigger_state}")

            has_processing_error = bool(source_error or error_message)
            host_hint = new_host_name
            if not host_hint and initial_input_url and has_processing_error and new_host_detected:
                host_hint = canonical_host_from_url(initial_input_url)
            summary = decorate_summary_with_attempt_status(
                summary=summary,
                initial_url=initial_input_url,
                host_hint=host_hint,
                has_processing_error=has_processing_error,
                auto_review_triggers=auto_review_triggers,
            )

            brief = {
                "ok": bool(result.get("ok")),
                "summary": summary,
                "google_doc_status": doc_status,
                "google_doc_url": doc_url,
                "error_message": error_message,
                "reply_message": build_reply_message(
                    summary=summary,
                    google_doc_status=doc_status,
                    google_doc_url=doc_url,
                    error_message=error_message,
                ),
                "doc": note_result,
                "duplicate": duplicate_hit_post_fetch if duplicate_hit_post_fetch else {},
                "auto_review_triggers": auto_review_triggers,
                "report_path": str(report_path),
            }
            print(json.dumps(brief, ensure_ascii=True, indent=2))
        else:
            print(json.dumps(result, ensure_ascii=True, indent=2))
    else:
        print(report_markdown)
        print(f"Report saved: {report_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Chief Fafa pipeline failed: {exc.__class__.__name__}: {exc}")
