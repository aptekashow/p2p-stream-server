import unittest

from app.hls_relay import parse_playlist, delayed_playlist


class HlsRelayTests(unittest.TestCase):
    def test_media_sequence_not_filename_controls_sequence(self):
        text = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:120
#EXTINF:4.0,
random-a.ts
#EXTINF:4.0,
segment_9999.ts
"""
        parsed = parse_playlist(text, "http://127.0.0.1:6878/live/index.m3u8")
        self.assertEqual([x.sequence for x in parsed.segments], [120, 121])
        self.assertEqual(parsed.segments[0].uri, "http://127.0.0.1:6878/live/random-a.ts")

    def test_master_playlist_resolves_variant(self):
        text = """#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=1000000
media/index.m3u8
"""
        parsed = parse_playlist(text, "http://127.0.0.1:6878/master.m3u8")
        self.assertEqual(parsed.variant_url, "http://127.0.0.1:6878/media/index.m3u8")

    def test_delayed_playlist_excludes_live_edge_buffer(self):
        entries = [
            {"sequence": n, "duration": 4.0}
            for n in range(100, 108)
        ]
        cached = {
            n: {
                "cached_at": 1000.0,
                "duration": 4.0,
                "extinf": "#EXTINF:4.0,",
                "tags": [],
            }
            for n in range(100, 108)
        }
        result = delayed_playlist(
            ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:4"],
            entries,
            cached,
            8,
            60,
            1001.0,
            7,
        )
        self.assertIsNotNone(result)
        self.assertIn("#EXT-X-MEDIA-SEQUENCE:100", result)
        self.assertIn("/live/7/segment/105.ts", result)
        self.assertNotIn("/live/7/segment/106.ts", result)
        self.assertNotIn("/live/7/segment/107.ts", result)

    def test_missing_segment_breaks_window_and_old_cache_expires(self):
        entries = [{"sequence": n, "duration": 4.0} for n in range(1, 9)]
        cached = {
            n: {
                "cached_at": 1000.0 if n != 4 else 900.0,
                "duration": 4.0,
                "extinf": "#EXTINF:4.0,",
                "tags": [],
            }
            for n in range(1, 9)
        }
        result = delayed_playlist([], entries, cached, 8, 60, 1001.0, 1)
        self.assertIsNotNone(result)
        self.assertIn("#EXT-X-MEDIA-SEQUENCE:5", result)
        self.assertNotIn("/live/1/segment/3.ts", result)
        self.assertIn("/live/1/segment/5.ts", result)


if __name__ == "__main__":
    unittest.main()
