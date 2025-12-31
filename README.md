# Scraping Manga-News avec Scrapy

Projet de base pour scraper le site Manga-News avec Scrapy. La structure est volontairement minimale pour servir de point de depart.

## Prerequis

- Python 3.10+

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Lancer le spider

```bash
scrapy crawl manga_news -O data.json
```

## Structure

- `scrapy.cfg` : configuration du projet Scrapy
- `manga_news_scraper/settings.py` : reglages du projet
- `manga_news_scraper/spiders/manga_news.py` : spider principal (SitemapSpider)

## Notes

- Respecter les robots.txt et les conditions d'utilisation du site.
- Le spider actuel est un point de depart : adaptez `sitemap_rules` et les selecteurs CSS selon les pages cibles.
