"""Small, testable HLS parsing and delayed live-playlist builder."""
from dataclasses import dataclass
from math import ceil
from urllib.parse import urljoin


@dataclass(frozen=True)
class Segment:
    sequence: int
    uri: str
    duration: float
    extinf: str
    tags: tuple[str, ...]


@dataclass(frozen=True)
class ParsedPlaylist:
    header: tuple[str, ...]
    media_sequence: int
    target_duration: float
    segments: tuple[Segment, ...]
    variant_url: str | None = None


def parse_playlist(text: str, base_url: str) -> ParsedPlaylist:
    """Parse a media playlist; for a master playlist return its first variant URL."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for index, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF:"):
            variant = next((item for item in lines[index + 1:] if not item.startswith("#")), None)
            if not variant:
                raise ValueError("Master playlist has no media variant")
            return ParsedPlaylist((), 0, 0.0, (), urljoin(base_url, variant))

    header: list[str] = []
    segments: list[Segment] = []
    media_sequence = 0
    target_duration = 0.0
    pending_extinf: str | None = None
    pending_tags: list[str] = []
    for line in lines:
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                media_sequence = int(line.split(":", 1)[1])
            except ValueError as exc:
                raise ValueError("Invalid EXT-X-MEDIA-SEQUENCE") from exc
            continue
        if line.startswith("#EXT-X-TARGETDURATION:"):
            try:
                target_duration = float(line.split(":", 1)[1])
            except ValueError as exc:
                raise ValueError("Invalid EXT-X-TARGETDURATION") from exc
            header.append(line)
            continue
        if line.startswith("#EXTINF:"):
            pending_extinf = line
            continue
        if line.startswith("#"):
            if pending_extinf is not None or segments:
                pending_tags.append(line)
            else:
                header.append(line)
            continue
        if pending_extinf is None:
            continue
        try:
            duration = float(pending_extinf.split(":", 1)[1].split(",", 1)[0])
        except ValueError as exc:
            raise ValueError("Invalid EXTINF duration") from exc
        if duration <= 0:
            raise ValueError("Segment duration must be positive")
        segments.append(Segment(
            sequence=media_sequence + len(segments),
            uri=urljoin(base_url, line),
            duration=duration,
            extinf=pending_extinf,
            tags=tuple(pending_tags),
        ))
        pending_extinf = None
        pending_tags = []

    if not segments:
        raise ValueError("Media playlist contains no EXTINF segments")
    return ParsedPlaylist(tuple(header), media_sequence, target_duration, tuple(segments))


def delayed_playlist(
    header: list[str],
    playlist_entries: list[dict],
    cached_segments: dict[int, dict],
    buffer_seconds: float,
    cache_seconds: float,
    now: float,
    stream_id: int,
) -> str | None:
    """Build a valid HLS window delayed behind live edge using only complete cached segments."""
    if not playlist_entries or not cached_segments:
        return None

    delay = 0.0
    cutoff = None
    for entry in reversed(playlist_entries):
        delay += float(entry["duration"])
        if delay >= buffer_seconds:
            cutoff = int(entry["sequence"]) - 1
            break
    if cutoff is None:
        return None

    available = [
        (int(seq), item) for seq, item in cached_segments.items()
        if int(seq) <= cutoff
        and now - float(item.get("cached_at", 0)) <= cache_seconds
    ]
    available.sort(key=lambda pair: pair[0])
    if len(available) < 2:
        return None

    # HLS media sequence must be contiguous. Never advertise a missing segment.
    last_gap = -1
    for index in range(1, len(available)):
        if available[index][0] != available[index - 1][0] + 1:
            last_gap = index - 1
    visible = available[last_gap + 1:]
    if len(visible) < 2:
        return None

    max_duration = max(float(item["duration"]) for _, item in visible)
    target = max(1, ceil(max_duration))
    output = ["#EXTM3U"]
    for line in header:
        if line.startswith(("#EXTM3U", "#EXT-X-MEDIA-SEQUENCE:", "#EXT-X-TARGETDURATION:", "#EXT-X-ENDLIST")):
            continue
        output.append(line)
    output.extend((f"#EXT-X-TARGETDURATION:{target}", f"#EXT-X-MEDIA-SEQUENCE:{visible[0][0]}"))
    for sequence, item in visible:
        output.extend(item.get("tags", ()))
        output.append(item["extinf"])
        output.append(f"/live/{stream_id}/segment/{sequence}.ts")
    return "\n".join(output) + "\n"
