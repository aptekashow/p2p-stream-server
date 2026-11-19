# P2P Stream Server v0.1.0

Начальный каркас для Ubuntu Server 24.04: поиск AceStream через встроенный Ace Search, каталог результатов и интеграция с локальным Ace Stream Engine.

## Запуск
1. Установите Ace Stream Engine для Linux из официального дистрибутива и запустите в console mode.
2. Проверьте Engine: `curl 'http://127.0.0.1:6878/webui/api/service?method=get_version'`
3. `cp .env.example .env`
4. `docker compose up -d --build`
5. Откройте `http://SERVER_IP:9999`.

Engine API не публикуйте в Интернет. Приложение не содержит искусственного лимита каналов, но реальное число потоков ограничено ресурсами сервера и P2P-доступностью. Используйте только источники, на распространение которых у вас есть право.

## Уже есть
- FastAPI backend
- проверка Engine
- поиск через Ace Search `/search`
- SQLite-каталог и импорт результатов
- Docker Compose
- начальная веб-страница

## Следующий этап
Реализовать relay `/live/{id}/index.m3u8`, переписывание URI HLS-сегментов, остановку playback session, а также отдельный HLS relay с `-c copy` и проверяемой задержкой около 20 секунд. Сейчас `/playlist.m3u` — заготовка; полноценный HLS relay ещё не реализован.

Документация:
- https://docs.acestream.media/developers/search/
- https://docs.acestream.media/developers/start-playback/
- https://docs.acestream.media/developers/api-reference/
- https://docs.acestream.media/developers/running-engine-desktop/
