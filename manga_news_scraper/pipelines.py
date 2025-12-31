import os
import re
import json
import datetime as dt
import psycopg2
from psycopg2.extras import execute_values
# manga_news_scraper/pipelines.py
from itemadapter import ItemAdapter
from manga_news_scraper.utils.enrich_jsonl import enrich_item


class EnrichPipeline:
    def process_item(self, item, spider):
        data = ItemAdapter(item).asdict()
        data = enrich_item(data)
        return data


class ValidationError(Exception):
    pass

def normalize_spaces(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

def parse_origin(origin: str | None):
    """
    Ex: "Japon - 2018" -> ("Japon", 2018)
    """
    if not origin:
        return (None, None)
    origin = origin.replace(":", "").strip()
    m = re.search(r"^(?P<country>.+?)\s*-\s*(?P<year>\d{4})$", origin)
    if not m:
        return (normalize_spaces(origin), None)
    return (normalize_spaces(m.group("country")), int(m.group("year")))

class MangaNewsPostgresPipeline:
    """
    - normalise
    - valide
    - upsert en PostgreSQL
    - batch insert (performance)
    """

    def __init__(self, dsn: str, batch_size: int = 200):
        self.dsn = dsn
        self.batch_size = batch_size
        self.buffer = []
        self.conn = None
        self.cur = None

    @classmethod
    def from_crawler(cls, crawler):
        dsn = crawler.settings.get("POSTGRES_DSN") or os.getenv("APIMANGA_DSN") or os.getenv("POSTGRES_DSN")
        if not dsn:
            raise RuntimeError("POSTGRES_DSN manquant (settings.py ou variable d'environnement).")
        batch_size = crawler.settings.getint("PG_BATCH_SIZE", 200)
        return cls(dsn=dsn, batch_size=batch_size)

    def open_spider(self, spider):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = False
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        self.flush()
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def process_item(self, item, spider):
        # --- Normalisation ---
        url = normalize_spaces(item.get("url"))
        title_page = normalize_spaces(item.get("title_page"))
        titre_vo = normalize_spaces(item.get("titre_vo"))
        titre_traduit = normalize_spaces(item.get("titre_traduit"))

        resume = normalize_spaces(item.get("resume"))
        points_forts = normalize_spaces(item.get("points_forts"))
        rag_text = normalize_spaces(item.get("rag_text"))

        genres = item.get("genres") or []
        genres = [normalize_spaces(g) for g in genres if normalize_spaces(g)]

        origin_raw = normalize_spaces(item.get("origine"))
        origin_country, origin_year = parse_origin(origin_raw)

        # --- Validations (règles de base) ---
        if not url:
            raise ValidationError("url manquante")

        if not (titre_traduit or titre_vo or title_page):
            raise ValidationError("aucun titre disponible (titre_traduit/titre_vo/title_page)")

        # Pour le RAG, on veut au moins une source de texte
        if not (resume or points_forts):
            # tu peux choisir warning plutôt que drop
            raise ValidationError("resume ET points_forts manquants (doc RAG vide)")

        if origin_year is not None and not (1900 <= origin_year <= 2100):
            raise ValidationError(f"origin_year incohérent: {origin_year}")

        row = (
            url,
            title_page,
            titre_vo,
            titre_traduit,
            normalize_spaces(item.get("dessin")),
            normalize_spaces(item.get("scenario")),
            normalize_spaces(item.get("traducteur")),
            normalize_spaces(item.get("editeur_vf")),
            normalize_spaces(item.get("collection")),
            normalize_spaces(item.get("type")),
            json.dumps(genres, ensure_ascii=False),
            origin_country,
            origin_year,
            resume,
            points_forts,
            rag_text,
            dt.datetime.utcnow(),
            "manganews",
        )

        self.buffer.append(row)
        if len(self.buffer) >= self.batch_size:
            self.flush()

        return item

    def flush(self):
        if not self.buffer:
            return

        sql = """
        INSERT INTO manga.mn_series (
            url, title_page, titre_vo, titre_traduit,
            dessin, scenario, traducteur,
            editeur_vf, collection, type,
            genres_json,
            origin_country, origin_year,
            resume, points_forts, rag_text,
            scraped_at, source
        ) VALUES %s
        ON CONFLICT (url) DO UPDATE SET
            title_page = EXCLUDED.title_page,
            titre_vo = EXCLUDED.titre_vo,
            titre_traduit = EXCLUDED.titre_traduit,
            dessin = EXCLUDED.dessin,
            scenario = EXCLUDED.scenario,
            traducteur = EXCLUDED.traducteur,
            editeur_vf = EXCLUDED.editeur_vf,
            collection = EXCLUDED.collection,
            type = EXCLUDED.type,
            genres_json = EXCLUDED.genres_json,
            origin_country = EXCLUDED.origin_country,
            origin_year = EXCLUDED.origin_year,
            resume = EXCLUDED.resume,
            points_forts = EXCLUDED.points_forts,
            rag_text = EXCLUDED.rag_text,
            scraped_at = EXCLUDED.scraped_at,
            source = EXCLUDED.source
        ;
        """

        execute_values(self.cur, sql, self.buffer, page_size=self.batch_size)
        self.conn.commit()
        self.buffer.clear()

